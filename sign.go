// Package s3 signs HTTP requests as prescribed in
// http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/RESTAuthentication.html.
package s3

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"io"
	"net/http"
	"sort"
	"strings"
)

var signParams = map[string]bool{
	"acl":                          true,
	"delete":                       true,
	"lifecycle":                    true,
	"location":                     true,
	"logging":                      true,
	"notification":                 true,
	"partNumber":                   true,
	"policy":                       true,
	"requestPayment":               true,
	"torrent":                      true,
	"uploadId":                     true,
	"uploads":                      true,
	"versionId":                    true,
	"versioning":                   true,
	"versions":                     true,
	"website":                      true,
	"response-content-type":        true,
	"response-content-language":    true,
	"response-expires":             true,
	"response-cache-control":       true,
	"response-content-disposition": true,
	"response-content-encoding":    true,
}

// Keys holds a set of Amazon Security Credentials.
type Keys struct {
	AccessKey string
	SecretKey string
}

// The default Service used by Sign.
var DefaultService = &Service{"amazonaws.com"}

// Sign signs an HTTP request with the given S3 keys.
//
// This function is shorthand for DefaultService.Sign(r, k).
func Sign(r *http.Request, k Keys) {
	DefaultService.Sign(r, k)
}

// Service represents an S3-compatible service.
type Service struct {
	Domain string // used to derive a bucket name from an http.Request
}

// Sign signs an HTTP request with the given S3 keys for use on service s.
func (s *Service) Sign(r *http.Request, k Keys) {
	h := hmac.New(sha1.New, []byte(k.SecretKey))
	s.writeSigData(h, r)
	sig := make([]byte, base64.StdEncoding.EncodedLen(h.Size()))
	base64.StdEncoding.Encode(sig, h.Sum(nil))
	r.Header.Set("Authorization", "AWS "+k.AccessKey+":"+string(sig))
}

func (s *Service) writeSigData(w io.Writer, r *http.Request) {
	w.Write([]byte(r.Method))
	w.Write([]byte{'\n'})
	w.Write([]byte(r.Header.Get("content-md5")))
	w.Write([]byte{'\n'})
	w.Write([]byte(r.Header.Get("content-type")))
	w.Write([]byte{'\n'})
	if _, ok := r.Header["X-Amz-Date"]; !ok {
		w.Write([]byte(r.Header.Get("date")))
	}
	w.Write([]byte{'\n'})
	writeAmzHeaders(w, r)
	s.writeResource(w, r)
}

func (s *Service) writeResource(w io.Writer, r *http.Request) {
	s.writeVhostBucket(w, r.Host)
	path := r.URL.RequestURI()
	if r.URL.RawQuery != "" {
		path = path[:len(path)-len(r.URL.RawQuery)-1]
	}
	w.Write([]byte(path))
	s.writeSubResource(w, r)
}

func (s *Service) writeVhostBucket(w io.Writer, host string) {
	if i := strings.Index(host, ":"); i != -1 {
		host = host[:i]
	}
	if !strings.HasSuffix(host, "."+s.Domain) {
		w.Write([]byte{'/'})
		w.Write([]byte(strings.ToLower(host)))
	} else if a := strings.Split(host, "."); len(a) > 3 {
		w.Write([]byte{'/'})
		w.Write([]byte(a[0]))
		for _, s := range a[1 : len(a)-3] { // omit .s3.amazonaws.com
			w.Write([]byte{'.'})
			w.Write([]byte(s))
		}
	}
}

func (s *Service) writeSubResource(w io.Writer, r *http.Request) {
	var a []string
	for k, vs := range r.URL.Query() {
		if signParams[k] {
			for _, v := range vs {
				if v == "" {
					a = append(a, k)
				} else {
					a = append(a, k+"="+v)
				}
			}
		}
	}
	sort.Strings(a)
	var p byte = '?'
	for _, s := range a {
		w.Write([]byte{p})
		w.Write([]byte(s))
		p = '&'
	}
}

func writeAmzHeaders(w io.Writer, r *http.Request) {
	var a []string
	for k, v := range r.Header {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, "x-amz-") {
			a = append(a, k+":"+strings.Join(v, ","))
		}
	}
	sort.Strings(a)
	for _, h := range a {
		w.Write([]byte(h))
		w.Write([]byte{'\n'})
	}
}
