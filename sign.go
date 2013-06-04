// Package s3 signs HTTP requests for Amazon S3.
//
// See
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
	"response-cache-control":       true,
	"response-content-disposition": true,
	"response-content-encoding":    true,
	"response-content-language":    true,
	"response-content-type":        true,
	"response-expires":             true,
	"torrent":                      true,
	"uploadId":                     true,
	"uploads":                      true,
	"versionId":                    true,
	"versioning":                   true,
	"versions":                     true,
	"website":                      true,
}

// Keys holds a set of Amazon Security Credentials.
type Keys struct {
	AccessKey string
	SecretKey string
	// Used for temporary security credentials. Leave blank to use
	// standard AWS Account or IAM credentials.
	// See http://docs.aws.amazon.com/AmazonS3/latest/dev/MakingRequests.html#TypesofSecurityCredentials
	SecurityToken string
}

// IdentityBucket returns subdomain as-is.
func IdentityBucket(subdomain string) string {
	return subdomain
}

// AmazonBucket returns up to last section of subdomain.
// Intended to be used with Amazon S3 service: "johnsmith.s3-eu-west-1" => "johnsmith".
func AmazonBucket(subdomain string) string {
	s := strings.Split(subdomain, ".")
	return strings.Join(s[:len(s)-1], ".")
}

// The default Service used by Sign.
var DefaultService = &Service{Domain: "amazonaws.com"}

// Sign signs an HTTP request with the given S3 keys.
//
// This function is shorthand for DefaultService.Sign(r, k).
func Sign(r *http.Request, k Keys) {
	DefaultService.Sign(r, k)
}

// Service represents an S3-compatible service.
type Service struct {
	Domain string                        // service root domain, used to extract subdomain from an http.Request and pass it to Bucket
	Bucket func(subdomain string) string // function used to derive a bucket name from subdomain; if nil, AmazonBucket is used
}

// Sign signs an HTTP request with the given S3 keys for use on service s.
func (s *Service) Sign(r *http.Request, k Keys) {
	if k.SecurityToken != "" {
		r.Header.Set("X-Amz-Security-Token", k.SecurityToken)
	}
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
	s.writeVhostBucket(w, strings.ToLower(r.Host))
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

	if host == s.Domain {
		// no vhost - do nothing
	} else if strings.HasSuffix(host, "."+s.Domain) {
		// vhost - bucket may be in prefix
		b := s.Bucket
		if b == nil {
			b = AmazonBucket
		}
		bucket := b(host[:len(host)-len(s.Domain)-1])

		if bucket != "" {
			w.Write([]byte{'/'})
			w.Write([]byte(bucket))
		}
	} else {
		// cname - bucket is host
		w.Write([]byte{'/'})
		w.Write([]byte(host))
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
