package streaming

// tickets_s3.go — the s3:// discovery root: tickets as objects at
// <prefix>/tickets/<name>.json, the same JSON the file root and the
// Python module write. A put is one PutObject (atomic replace, so a
// poller never reads a half ticket); a missing object is "no publisher".
// Credentials are not the root's business: the standard AWS chain
// applies.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// parseS3Root splits s3://bucket[/prefix]; ok is false for any other root.
func parseS3Root(root string) (bucket, prefix string, ok bool) {
	if !strings.HasPrefix(root, "s3://") {
		return "", "", false
	}
	bucket, prefix, _ = strings.Cut(strings.TrimPrefix(root, "s3://"), "/")
	if bucket == "" {
		return "", "", false
	}
	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return bucket, prefix, true
}

type s3Store struct {
	client *s3.Client
	bucket string
	prefix string // "" or "dir/"
}

func newS3Store(client *s3.Client, bucket, prefix string) *s3Store {
	return &s3Store{client: client, bucket: bucket, prefix: prefix}
}

// newDefaultS3Store builds the store on the standard AWS configuration
// chain (environment, profile, instance or task role).
func newDefaultS3Store(bucket, prefix string) (*s3Store, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("ticket root s3://%s: %w", bucket, err)
	}
	return newS3Store(s3.NewFromConfig(cfg), bucket, prefix), nil
}

func (s *s3Store) key(name string) string { return s.prefix + "tickets/" + name + ".json" }

func (s *s3Store) put(name string, t Ticket) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(name)), Body: bytes.NewReader(b)})
	return err
}

func (s *s3Store) get(name string) (*Ticket, error) {
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(name))})
	var missing *types.NoSuchKey
	if errors.As(err, &missing) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	var t Ticket
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("ticket %s: %w", name, err)
	}
	return &t, nil
}

func (s *s3Store) delete(name string) error {
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(name))})
	return err
}

func (s *s3Store) list() ([]string, error) {
	var names []string
	base := s.prefix + "tickets/"
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(base)})
	for p.HasMorePages() {
		page, err := p.NextPage(context.Background())
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			k := aws.ToString(o.Key)
			if !strings.HasSuffix(k, ".json") {
				continue
			}
			names = append(names, strings.TrimSuffix(strings.TrimPrefix(k, base), ".json"))
		}
	}
	sort.Strings(names)
	return names, nil
}
