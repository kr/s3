package s3util

import (
	"bytes"
	"encoding/xml"
	"github.com/kr/s3"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// defined by amazon
const (
	MinPartSize = 5 * 1024 * 1024
	MaxPartSize = 1<<31 - 1 // for 32-bit use; amz max is 5GiB
	MaxObjSize  = 5 * 1024 * 1024 * 1024 * 1024
	MaxNPart    = 10000
)

const (
	concurrency = 5
	nTry        = 2
)

type part struct {
	r   io.ReadSeeker
	len int64

	// read by xml encoder
	PartNumber int
	ETag       string
}

type Uploader struct {
	s3       s3.Service
	keys     s3.Keys
	url      string
	client   *http.Client
	UploadId string // written by xml decoder

	bufsz  int64
	buf    []byte
	off    int
	ch     chan *part
	part   int
	closed bool
	err    error
	wg     sync.WaitGroup

	xml struct {
		XMLName string `xml:"CompleteMultipartUpload"`
		Part    []*part
	}
}

// Marshalable subset of uploader for persisting and resuming later.
type UploaderState struct {
	Buffer   []byte // if buf < MinPartSize, we need to persist it
	Url      string
	UploadId string
	Part     int
	Parts    []*part
}

// Create creates an S3 object at url and sends multipart upload requests as
// data is written.
//
// See http://docs.amazonwebservices.com/AmazonS3/latest/dev/mpuoverview.html.
//
// If h is not nil, each of its entries is added to the HTTP request header.
// If c is nil, Create uses DefaultConfig.
func Create(url string, h http.Header, c *Config) (*Uploader, error) {
	if c == nil {
		c = DefaultConfig
	}
	u := newUploader(url, c)
	r, err := http.NewRequest("POST", url+"?uploads", nil)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	for k := range h {
		for _, v := range h[k] {
			r.Header.Add(k, v)
		}
	}
	u.s3.Sign(r, u.keys)
	resp, err := u.client.Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, newRespError(resp)
	}
	err = xml.NewDecoder(resp.Body).Decode(u)
	if err != nil {
		return nil, err
	}
	return u, nil
}

// Initializes an Uploader but does not initiate the S3 multipart upload.
func newUploader(url string, c *Config) *Uploader {
	u := new(Uploader)
	u.s3 = *c.Service
	u.url = url
	u.keys = *c.Keys
	u.client = c.Client
	if u.client == nil {
		u.client = http.DefaultClient
	}
	u.bufsz = MinPartSize
	u.ch = make(chan *part)
	for i := 0; i < concurrency; i++ {
		go u.worker()
	}
	return u
}

func (u *Uploader) Write(p []byte) (n int, err error) {
	if u.closed {
		return 0, syscall.EINVAL
	}
	if u.err != nil {
		return 0, u.err
	}
	for n < len(p) {
		if cap(u.buf) == 0 {
			u.buf = make([]byte, int(u.bufsz))
			// Increase part size (1.001x).
			// This lets us reach the max object size (5TiB) while
			// still doing minimal buffering for small objects.
			u.bufsz = min(u.bufsz+u.bufsz/1000, MaxPartSize)
		}
		r := copy(u.buf[u.off:], p[n:])
		u.off += r
		n += r
		if u.off == len(u.buf) {
			u.flush()
		}
	}
	return n, nil
}

func (u *Uploader) flush() {
	u.wg.Add(1)
	u.part++
	p := &part{bytes.NewReader(u.buf[:u.off]), int64(u.off), u.part, ""}
	u.xml.Part = append(u.xml.Part, p)
	u.ch <- p
	u.buf, u.off = nil, 0
}

func (u *Uploader) worker() {
	for p := range u.ch {
		u.retryUploadPart(p)
	}
}

// Calls putPart up to nTry times to recover from transient errors.
func (u *Uploader) retryUploadPart(p *part) {
	defer u.wg.Done()
	defer func() { p.r = nil }() // free the large buffer
	var err error
	for i := 0; i < nTry; i++ {
		p.r.Seek(0, 0)
		err = u.putPart(p)
		if err == nil {
			return
		}
	}
	u.err = err
}

// Uploads part p, reading its contents from p.r.
// Stores the ETag in p.ETag.
func (u *Uploader) putPart(p *part) error {
	v := url.Values{}
	v.Set("partNumber", strconv.Itoa(p.PartNumber))
	v.Set("uploadId", u.UploadId)
	req, err := http.NewRequest("PUT", u.url+"?"+v.Encode(), p.r)
	if err != nil {
		return err
	}
	req.ContentLength = p.len
	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	u.s3.Sign(req, u.keys)
	resp, err := u.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return newRespError(resp)
	}
	s := resp.Header.Get("etag") // includes quote chars for some reason
	p.ETag = s[1 : len(s)-1]
	return nil
}

// Flushes uploads, closes the uploader, and returns the current state for
// Resume()ing later. Note that Resume() requires the *Config as it is not
// included in *UploaderState.
func (u *Uploader) Pause() (*UploaderState, error) {
	// We can't flush parts less than the min size, so we have to persist them.
	var buf []byte
	if u.off > 0 && u.off < MinPartSize {
		buf = make([]byte, u.off)
		copy(buf, u.buf)
		u.buf, u.off = nil, 0
	}

	if err := u.shutdown(); err != nil {
		return nil, err
	}

	return &UploaderState{
		Part:     u.part,
		Parts:    u.xml.Part,
		Url:      u.url,
		UploadId: u.UploadId,
		Buffer:   buf,
	}, nil
}

// Resume returns an Uploader at the given state with a new config.
//
// Config must be included because it contains senstive keys callers may want
// to persist separately and an optional http.Client which cannot be persisted.
func Resume(state *UploaderState, c *Config) *Uploader {
	u := newUploader(state.Url, c)
	u.UploadId = state.UploadId
	u.part = state.Part
	u.xml.Part = state.Parts

	// Load buffer and set size (offset)
	u.buf = make([]byte, int(u.bufsz))
	copy(u.buf, state.Buffer)
	u.off = len(state.Buffer)
	return u
}

// Flushes buffer and closes connection but does not commit S3 upload.
func (u *Uploader) shutdown() error {
	if u.closed {
		return syscall.EINVAL
	}
	if cap(u.buf) > 0 {
		u.flush()
	}
	u.wg.Wait()
	close(u.ch)
	u.closed = true
	if u.err != nil {
		u.abort()
		return u.err
	}
	return nil
}

// Close flushes and commits the multipart upload. Calling more than once will
// return syscall.EINVAL.
func (u *Uploader) Close() error {
	if err := u.shutdown(); err != nil {
		return err
	}

	// Commit the upload
	body, err := xml.Marshal(u.xml)
	if err != nil {
		return err
	}
	b := bytes.NewBuffer(body)
	v := url.Values{}
	v.Set("uploadId", u.UploadId)
	req, err := http.NewRequest("POST", u.url+"?"+v.Encode(), b)
	if err != nil {
		return err
	}
	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	u.s3.Sign(req, u.keys)
	resp, err := u.client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return newRespError(resp)
	}
	resp.Body.Close()
	return nil
}

func (u *Uploader) abort() {
	// TODO(kr): devise a reasonable way to report an error here in addition
	// to the error that caused the abort.
	v := url.Values{}
	v.Set("uploadId", u.UploadId)
	s := u.url + "?" + v.Encode()
	req, err := http.NewRequest("DELETE", s, nil)
	if err != nil {
		return
	}
	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	u.s3.Sign(req, u.keys)
	resp, err := u.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return
	}
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
