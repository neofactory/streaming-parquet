package pqc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	// S3's multipart rules: every part but the last is at least MinPart,
	// and an assembly has at most MaxParts parts.
	MinPart  = 5 << 20
	MaxParts = 10_000
)

// S3Sink is the real thing, the same contract as the Python S3Sink: put,
// ranged get, head for size and exists, delete, paginated list, and
// assembly as a multipart upload built from server-side part copies. The
// client is the caller's (its region, credentials and endpoint); names
// are rooted at bucket+prefix.
type S3Sink struct {
	Bucket string
	Prefix string // "" or "dir/"; rooted at NewS3Sink
	client *s3.Client
	ctx    context.Context
}

// NewS3Sink roots names at bucket/prefix. The prefix may be "" or any
// path; a trailing slash is supplied.
func NewS3Sink(client *s3.Client, bucket, prefix string) *S3Sink {
	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return &S3Sink{Bucket: bucket, Prefix: prefix, client: client, ctx: context.Background()}
}

func (s *S3Sink) PartMin() uint64 { return MinPart }

// Sub roots the sink deeper under the same bucket and client.
func (s *S3Sink) Sub(prefix string) Sink {
	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return &S3Sink{Bucket: s.Bucket, Prefix: s.Prefix + prefix, client: s.client, ctx: s.ctx}
}

func (s *S3Sink) URI() string { return "s3://" + s.Bucket + "/" + s.Prefix }

func (s *S3Sink) key(name string) string { return s.Prefix + name }

func (s *S3Sink) Put(name string, data []byte) error {
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket), Key: aws.String(s.key(name)), Body: bytes.NewReader(data)})
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", name, err)
	}
	return nil
}

func (s *S3Sink) Get(name string, offset, length int64) ([]byte, error) {
	in := &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(s.key(name))}
	if offset > 0 || length >= 0 {
		end := ""
		if length >= 0 {
			end = fmt.Sprint(offset + length - 1)
		}
		in.Range = aws.String(fmt.Sprintf("bytes=%d-%s", offset, end))
	}
	out, err := s.client.GetObject(s.ctx, in)
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", name, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", name, err)
	}
	return b, nil
}

func (s *S3Sink) Size(name string) (int64, error) {
	out, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(s.key(name))})
	if err != nil {
		return 0, fmt.Errorf("s3 head %s: %w", name, err)
	}
	return aws.ToInt64(out.ContentLength), nil
}

// Exists is a head; any failure reads as absent, as in the Python sink.
func (s *S3Sink) Exists(name string) bool {
	_, err := s.Size(name)
	return err == nil
}

// Delete is idempotent: S3 answers the same for an absent key.
func (s *S3Sink) Delete(name string) error {
	_, err := s.client.DeleteObject(s.ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(s.key(name))})
	if err != nil {
		return fmt.Errorf("s3 delete %s: %w", name, err)
	}
	return nil
}

func (s *S3Sink) List(prefix string) ([]string, error) {
	var out []string
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket), Prefix: aws.String(s.key(prefix))})
	for p.HasMorePages() {
		page, err := p.NextPage(s.ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list %s: %w", prefix, err)
		}
		for _, o := range page.Contents {
			out = append(out, strings.TrimPrefix(aws.ToString(o.Key), s.Prefix))
		}
	}
	sort.Strings(out)
	return out, nil
}

// Assemble builds dest from the sources as one multipart upload of
// server-side part copies: a whole object or a byte range each. Every
// part but the last must reach MinPart; the upload is aborted on any
// failure so no part lingers. The final object appears atomically at
// completion.
func (s *S3Sink) Assemble(dest string, sources []Source) (err error) {
	if len(sources) > MaxParts {
		return fmt.Errorf("assemble %s: %d parts exceeds %d", dest, len(sources), MaxParts)
	}
	key := s.key(dest)
	mpu, err := s.client.CreateMultipartUpload(s.ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("assemble %s: create multipart upload: %w", dest, err)
	}
	defer func() {
		if err != nil {
			_, _ = s.client.AbortMultipartUpload(s.ctx, &s3.AbortMultipartUploadInput{
				Bucket: aws.String(s.Bucket), Key: aws.String(key), UploadId: mpu.UploadId})
		}
	}()
	var parts []types.CompletedPart
	for i, src := range sources {
		n := int32(i + 1)
		in := &s3.UploadPartCopyInput{
			Bucket: aws.String(s.Bucket), Key: aws.String(key), UploadId: mpu.UploadId, PartNumber: aws.Int32(n),
			CopySource: aws.String(url.PathEscape(s.Bucket + "/" + s.key(src.Name))),
		}
		var srcLen int64
		if src.Offset < 0 {
			if srcLen, err = s.Size(src.Name); err != nil {
				return fmt.Errorf("assemble %s: part %d: %w", dest, n, err)
			}
		} else {
			srcLen = src.Length
			in.CopySourceRange = aws.String(fmt.Sprintf("bytes=%d-%d", src.Offset, src.Offset+src.Length-1))
		}
		if i < len(sources)-1 && srcLen < MinPart {
			return fmt.Errorf("assemble %s: part %d is %d bytes; parts before the last must be >= %d", dest, n, srcLen, MinPart)
		}
		out, err := s.client.UploadPartCopy(s.ctx, in)
		if err != nil {
			return fmt.Errorf("assemble %s: part %d copy: %w", dest, n, err)
		}
		parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(n), ETag: out.CopyPartResult.ETag})
	}
	_, err = s.client.CompleteMultipartUpload(s.ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s.Bucket), Key: aws.String(key), UploadId: mpu.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	if err != nil {
		return fmt.Errorf("assemble %s: complete: %w", dest, err)
	}
	return nil
}

// IsNotFound reports whether a sink error is S3's absent key or object.
func IsNotFound(err error) bool {
	var nk *types.NoSuchKey
	var nf *types.NotFound
	return errors.As(err, &nk) || errors.As(err, &nf)
}
