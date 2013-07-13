package s3util

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestUploaderCloseRespBody(t *testing.T) {
	want := make(chan int, 100)
	got := make(closeCounter, 100)
	c := *DefaultConfig
	c.Client = &http.Client{
		Transport: RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			want <- 1
			var s string
			switch q := req.URL.Query(); {
			case req.Method == "PUT":
			case req.Method == "POST" && q["uploads"] != nil:
				s = `<UploadId>foo</UploadId>`
			case req.Method == "POST" && q["uploadId"] != nil:
			default:
				t.Fatal("unexpected request", req)
			}
			resp := &http.Response{
				StatusCode: 200,
				Body:       readClose{strings.NewReader(s), got},
				Header: http.Header{
					"Etag": {`"foo"`},
				},
			}
			return resp, nil
		}),
	}
	u, err := newUploader("https://s3.amazonaws.com/foo/bar", nil, &c)
	if err != nil {
		t.Fatal("unexpected err", err)
	}
	const size = minPartSize + minPartSize/3
	n, err := io.Copy(u, io.LimitReader(devZero, size))
	if err != nil {
		t.Fatal("unexpected err", err)
	}
	if n != size {
		t.Fatal("wrote %d bytes want %d", n, size)
	}
	err = u.Close()
	if err != nil {
		t.Fatal("unexpected err", err)
	}
	if len(want) != len(got) {
		t.Errorf("closes = %d want %d", len(got), len(want))
	}
}

type RoundTripperFunc func(*http.Request) (*http.Response, error)

func (f RoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type closeCounter chan int

func (c closeCounter) Close() error {
	c <- 1
	return nil
}

type readClose struct {
	io.Reader
	io.Closer
}

var devZero io.Reader = repeatReader(0)

type repeatReader byte

func (r repeatReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r)
	}
	return len(p), nil
}
