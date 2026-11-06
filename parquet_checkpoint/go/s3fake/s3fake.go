// Package s3fake is an in-process S3 for the Go tests: the Go side of
// the Python module's FakeS3. One HTTP server speaks the subset of the
// S3 REST API the sinks and the ticket stores use, path-style, with S3
// SEMANTICS: atomic whole-object replace, immutable objects, no append,
// ranged reads, prefix listing with continuation, and assembly through
// a real multipart-upload state machine that enforces the >=5MB-per-
// part-except-last rule and the 10,000-part ceiling at completion, the
// way S3 does. Client builds an SDK client pointed at it.
package s3fake

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	MinPart  = 5 << 20 // S3's multipart part minimum, every part but the last
	MaxParts = 10_000
)

// Server is one fake S3 holding any number of buckets.
type Server struct {
	srv     *httptest.Server
	mu      sync.Mutex
	objects map[string][]byte           // "bucket/key" -> bytes
	uploads map[string]map[int32][]byte // uploadId -> part number -> bytes
	upKeys  map[string]string           // uploadId -> "bucket/key"
	nextUp  int
	partMin int
}

// New starts a fake with the real part minimum.
func New() *Server {
	f := &Server{objects: map[string][]byte{}, uploads: map[string]map[int32][]byte{}, upKeys: map[string]string{}, partMin: MinPart}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

// SetPartMin lowers the part minimum for a test that assembles small
// pieces; the rule itself stays.
func (f *Server) SetPartMin(n int) { f.mu.Lock(); f.partMin = n; f.mu.Unlock() }

func (f *Server) Close() { f.srv.Close() }

// URL is the endpoint an SDK client points at.
func (f *Server) URL() string { return f.srv.URL }

// Client is an SDK client for this fake: path-style, anonymous, no
// request or response checksums beyond what an operation requires.
func (f *Server) Client() *s3.Client {
	return s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, RetryMaxAttempts: 1},
		func(o *s3.Options) {
			o.BaseEndpoint = aws.String(f.srv.URL)
			o.UsePathStyle = true
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		})
}

// Object answers a stored object's bytes, for assertions.
func (f *Server) Object(bucket, key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[bucket+"/"+key]
	return b, ok
}

// OpenUploads counts multipart uploads neither completed nor aborted.
func (f *Server) OpenUploads() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.uploads) }

// --- the wire --------------------------------------------------------------

type xmlError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func (f *Server) fail(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(xmlError{Code: code, Message: msg})
}

func etag(b []byte) string {
	sum := md5.Sum(b)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func (f *Server) handle(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	q := r.URL.Query()
	switch {
	case key == "" && r.Method == http.MethodGet:
		f.list(w, bucket, q)
	case r.Method == http.MethodPost && q.Has("uploads"):
		f.initiate(w, bucket, key)
	case r.Method == http.MethodPut && q.Has("uploadId"):
		f.copyPart(w, r, bucket, key, q)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		f.complete(w, r, bucket, key, q.Get("uploadId"))
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		f.abort(w, q.Get("uploadId"))
	case r.Method == http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			f.fail(w, http.StatusBadRequest, "IncompleteBody", err.Error())
			return
		}
		f.mu.Lock()
		f.objects[bucket+"/"+key] = body // atomic replace
		f.mu.Unlock()
		w.Header().Set("ETag", etag(body))
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet, r.Method == http.MethodHead:
		f.get(w, r, bucket, key)
	case r.Method == http.MethodDelete:
		f.mu.Lock()
		delete(f.objects, bucket+"/"+key)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		f.fail(w, http.StatusBadRequest, "InvalidRequest", r.Method+" "+r.URL.String())
	}
}

func (f *Server) get(w http.ResponseWriter, r *http.Request, bucket, key string) {
	f.mu.Lock()
	b, ok := f.objects[bucket+"/"+key]
	f.mu.Unlock()
	if !ok {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		f.fail(w, http.StatusNotFound, "NoSuchKey", key)
		return
	}
	status := http.StatusOK
	if rng := r.Header.Get("Range"); rng != "" {
		spec := strings.TrimPrefix(rng, "bytes=")
		first, last, _ := strings.Cut(spec, "-")
		start, err := strconv.Atoi(first)
		if err != nil || start >= len(b) {
			f.fail(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", rng)
			return
		}
		end := len(b) - 1
		if last != "" {
			if n, err := strconv.Atoi(last); err == nil && n < end {
				end = n
			}
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(b)))
		b = b[start : end+1]
		status = http.StatusPartialContent
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.Header().Set("ETag", etag(b))
	w.Header().Set("Last-Modified", "Sat, 18 Oct 2026 00:00:00 GMT")
	w.WriteHeader(status)
	if r.Method == http.MethodGet {
		_, _ = w.Write(b)
	}
}

type listResult struct {
	XMLName     xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name        string   `xml:"Name"`
	Prefix      string   `xml:"Prefix"`
	KeyCount    int      `xml:"KeyCount"`
	MaxKeys     int      `xml:"MaxKeys"`
	IsTruncated bool     `xml:"IsTruncated"`
	NextToken   string   `xml:"NextContinuationToken,omitempty"`
	Contents    []struct {
		Key          string `xml:"Key"`
		Size         int    `xml:"Size"`
		ETag         string `xml:"ETag"`
		LastModified string `xml:"LastModified"`
	} `xml:"Contents"`
}

func (f *Server) list(w http.ResponseWriter, bucket string, q url.Values) {
	prefix := q.Get("prefix")
	max := 1000
	if n, err := strconv.Atoi(q.Get("max-keys")); err == nil && n > 0 {
		max = n
	}
	after := q.Get("continuation-token")
	f.mu.Lock()
	var keys []string
	for k := range f.objects {
		b, key, _ := strings.Cut(k, "/")
		if b == bucket && strings.HasPrefix(key, prefix) && key > after {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	res := listResult{Name: bucket, Prefix: prefix, MaxKeys: max}
	for _, k := range keys {
		if len(res.Contents) == max {
			res.IsTruncated = true
			res.NextToken = res.Contents[len(res.Contents)-1].Key
			break
		}
		b := f.objects[bucket+"/"+k]
		res.Contents = append(res.Contents, struct {
			Key          string `xml:"Key"`
			Size         int    `xml:"Size"`
			ETag         string `xml:"ETag"`
			LastModified string `xml:"LastModified"`
		}{k, len(b), etag(b), "2026-10-18T00:00:00.000Z"})
	}
	f.mu.Unlock()
	res.KeyCount = len(res.Contents)
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(res)
}

func (f *Server) initiate(w http.ResponseWriter, bucket, key string) {
	f.mu.Lock()
	f.nextUp++
	id := fmt.Sprintf("upload-%d", f.nextUp)
	f.uploads[id] = map[int32][]byte{}
	f.upKeys[id] = bucket + "/" + key
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, bucket, key, id)
}

func (f *Server) copyPart(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
	id := q.Get("uploadId")
	n, err := strconv.Atoi(q.Get("partNumber"))
	if err != nil || n < 1 || n > MaxParts {
		f.fail(w, http.StatusBadRequest, "InvalidArgument", "partNumber")
		return
	}
	src, err := url.PathUnescape(strings.TrimPrefix(r.Header.Get("x-amz-copy-source"), "/"))
	if err != nil || src == "" {
		f.fail(w, http.StatusBadRequest, "InvalidArgument", "x-amz-copy-source")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	parts, ok := f.uploads[id]
	if !ok || f.upKeys[id] != bucket+"/"+key {
		f.fail(w, http.StatusNotFound, "NoSuchUpload", id)
		return
	}
	b, ok := f.objects[src]
	if !ok {
		f.fail(w, http.StatusNotFound, "NoSuchKey", src)
		return
	}
	if rng := r.Header.Get("x-amz-copy-source-range"); rng != "" {
		first, last, _ := strings.Cut(strings.TrimPrefix(rng, "bytes="), "-")
		start, err1 := strconv.Atoi(first)
		end, err2 := strconv.Atoi(last)
		if err1 != nil || err2 != nil || start > end || end >= len(b) {
			f.fail(w, http.StatusBadRequest, "InvalidArgument", "x-amz-copy-source-range "+rng)
			return
		}
		b = b[start : end+1]
	}
	parts[int32(n)] = bytes.Clone(b)
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ETag>%s</ETag><LastModified>2026-10-18T00:00:00.000Z</LastModified></CopyPartResult>`, etag(b))
}

type completeRequest struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int32  `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

func (f *Server) complete(w http.ResponseWriter, r *http.Request, bucket, key, id string) {
	var req completeRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		f.fail(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	parts, ok := f.uploads[id]
	if !ok || f.upKeys[id] != bucket+"/"+key {
		f.fail(w, http.StatusNotFound, "NoSuchUpload", id)
		return
	}
	if len(req.Parts) > MaxParts {
		f.fail(w, http.StatusBadRequest, "InvalidRequest", fmt.Sprintf("%d parts exceeds %d", len(req.Parts), MaxParts))
		return
	}
	var out []byte
	for i, p := range req.Parts {
		b, ok := parts[p.PartNumber]
		if !ok {
			f.fail(w, http.StatusBadRequest, "InvalidPart", fmt.Sprintf("part %d", p.PartNumber))
			return
		}
		if i < len(req.Parts)-1 && len(b) < f.partMin {
			f.fail(w, http.StatusBadRequest, "EntityTooSmall", fmt.Sprintf("part %d is %d bytes; parts before the last must be >= %d", p.PartNumber, len(b), f.partMin))
			return
		}
		out = append(out, b...)
	}
	f.objects[bucket+"/"+key] = out // atomic materialize
	delete(f.uploads, id)
	delete(f.upKeys, id)
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>%s</Bucket><Key>%s</Key><ETag>%s</ETag></CompleteMultipartUploadResult>`, bucket, key, etag(out))
}

func (f *Server) abort(w http.ResponseWriter, id string) {
	f.mu.Lock()
	delete(f.uploads, id)
	delete(f.upKeys, id)
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// Ensure the context type is referenced for callers building clients
// with their own config; the fake itself needs none.
var _ = context.Background
