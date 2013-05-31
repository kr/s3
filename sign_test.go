package s3

import (
	"bytes"
	"net/http"
	"testing"
)

var exKeys = Keys{
	AccessKey: "AKIAIOSFODNN7EXAMPLE",
	SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
}

// Temporary Credentials
var tokenExKeys = Keys{
	AccessKey:     "AKIAIOSFODNN7EXAMPLE",
	SecretKey:     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	SecurityToken: "dummy",
}

var signTest = []struct {
	method string
	url    string
	more   http.Header
	expBuf string
	expSig string
}{
	{
		"GET",
		"http://johnsmith.s3.amazonaws.com/photos/puppy.jpg",
		http.Header{
			"Date": {"Tue, 27 Mar 2007 19:36:42 +0000"},
		},
		"GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg",
		"AWS AKIAIOSFODNN7EXAMPLE:bWq2s1WEIj+Ydj0vQ697zp+IXMU=",
	},
	{
		"PUT",
		"http://johnsmith.s3.amazonaws.com/photos/puppy.jpg",
		http.Header{
			"Content-Type":   {"image/jpeg"},
			"Content-Length": {"94328"},
			"Date":           {"Tue, 27 Mar 2007 21:15:45 +0000"},
		},
		"PUT\n\nimage/jpeg\nTue, 27 Mar 2007 21:15:45 +0000\n/johnsmith/photos/puppy.jpg",
		"AWS AKIAIOSFODNN7EXAMPLE:MyyxeRY7whkBe+bq8fHCL/2kKUg=",
	},
	{
		"GET",
		"http://johnsmith.s3.amazonaws.com/?prefix=photos&max-keys=50&marker=puppy",
		http.Header{
			"User-Agent": {"Mozilla/5.0"},
			"Date":       {"Tue, 27 Mar 2007 19:42:41 +0000"},
		},
		"GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/",
		"AWS AKIAIOSFODNN7EXAMPLE:htDYFYduRNen8P9ZfE/s9SuKy0U=",
	},
	{
		"GET",
		"http://johnsmith.s3.amazonaws.com/?acl",
		http.Header{
			"Date": {"Tue, 27 Mar 2007 19:44:46 +0000"},
		},
		"GET\n\n\nTue, 27 Mar 2007 19:44:46 +0000\n/johnsmith/?acl",
		"AWS AKIAIOSFODNN7EXAMPLE:c2WLPFtWHVgbEmeEG93a4cG37dM=",
	},
	{
		"DELETE",
		"http://s3.amazonaws.com/johnsmith/photos/puppy.jpg",
		http.Header{
			"User-Agent": {"dotnet"},
			"Date":       {"Tue, 27 Mar 2007 21:20:27 +0000"},
			"x-amz-date": {"Tue, 27 Mar 2007 21:20:26 +0000"},
		},
		"DELETE\n\n\n\nx-amz-date:Tue, 27 Mar 2007 21:20:26 +0000\n/johnsmith/photos/puppy.jpg",

		// The expected signature in Amazon's documentation,
		// "AWS AKIAIOSFODNN7EXAMPLE:9b2sXq0KfxsxHtdZkzx/9Ngqyh8=",
		// appears to be incorrect.
		"AWS AKIAIOSFODNN7EXAMPLE:R4dJ53KECjStyBO5iTBJZ4XVOaI=",
	},
	{
		"PUT",
		"http://static.johnsmith.net:8080/db-backup.dat.gz",
		http.Header{
			"User-Agent":                   {"curl/7.15.5"},
			"Date":                         {"Tue, 27 Mar 2007 21:06:08 +0000"},
			"x-amz-acl":                    {"public-read"},
			"content-type":                 {"application/x-download"},
			"Content-MD5":                  {"4gJE4saaMU4BqNR0kLY+lw=="},
			"X-Amz-Meta-ReviewedBy":        {"joe@johnsmith.net", "jane@johnsmith.net"},
			"X-Amz-Meta-FileChecksum":      {"0x02661779"},
			"X-Amz-Meta-ChecksumAlgorithm": {"crc32"},
			"Content-Disposition":          {"attachment; filename=database.dat"},
			"Content-Encoding":             {"gzip"},
			"Content-Length":               {"5913339"},
		},
		"PUT\n4gJE4saaMU4BqNR0kLY+lw==\napplication/x-download\nTue, 27 Mar 2007 21:06:08 +0000\nx-amz-acl:public-read\nx-amz-meta-checksumalgorithm:crc32\nx-amz-meta-filechecksum:0x02661779\nx-amz-meta-reviewedby:joe@johnsmith.net,jane@johnsmith.net\n/static.johnsmith.net/db-backup.dat.gz",
		"AWS AKIAIOSFODNN7EXAMPLE:ilyl83RwaSoYIEdixDQcA4OnAnc=",
	},
	{
		"GET",
		"http://s3.amazonaws.com/",
		http.Header{
			"Date": {"Wed, 28 Mar 2007 01:29:59 +0000"},
		},
		"GET\n\n\nWed, 28 Mar 2007 01:29:59 +0000\n/",
		"AWS AKIAIOSFODNN7EXAMPLE:qGdzdERIC03wnaRNKh6OqZehG9s=",
	},
	{
		"GET",
		// I've altered this example from the one documented by Amazon
		// since package http never produces lower-case %-encodings.
		"http://s3.amazonaws.com/dictionary/fran%C3%A7ais/pr%C3%A9f%C3%A8re",
		http.Header{
			"Date": {"Wed, 28 Mar 2007 01:49:49 +0000"},
		},
		"GET\n\n\nWed, 28 Mar 2007 01:49:49 +0000\n/dictionary/fran%C3%A7ais/pr%C3%A9f%C3%A8re",
		"AWS AKIAIOSFODNN7EXAMPLE:81VEw/Bc3GDt/k65Xrrk3AdfI4c=",
	},
	{
		"POST",
		// ?delete is required in CanonicalizedResource for:
		// http://docs.amazonwebservices.com/AmazonS3/latest/API/multiobjectdeleteapi.html
		"http://bucketname.S3.amazonaws.com/?delete",
		http.Header{
			"x-amz-date":     {"Wed, 30 Nov 2011 03:39:05 GMT"},
			"Content-MD5":    {"p5/WA/oEr30qrEEl21PAqw=="},
			"Content-Length": {"125"},
		},
		"POST\np5/WA/oEr30qrEEl21PAqw==\n\n\nx-amz-date:Wed, 30 Nov 2011 03:39:05 GMT\n/bucketname/?delete",
		// Doesn't match the example in the Amazon docs
		"AWS AKIAIOSFODNN7EXAMPLE:DXGmXMY+1QnRGC7vicUqu1gTmK4=",
	},
}

func TestSign(t *testing.T) {
	for _, ts := range signTest {
		r, err := http.NewRequest(ts.method, ts.url, nil)
		if err != nil {
			panic(err)
		}

		for k, vs := range ts.more {
			for _, v := range vs {
				r.Header.Add(k, v)
			}
		}
		var buf bytes.Buffer
		DefaultService.writeSigData(&buf, r)
		if buf.String() != ts.expBuf {
			t.Errorf("in %s:", r.Method)
			t.Logf("url %s", r.URL.String())
			t.Logf("exp %q", ts.expBuf)
			t.Logf("got %q", buf.String())
		}

		DefaultService.Sign(r, exKeys)
		if got := r.Header.Get("Authorization"); got != ts.expSig {
			t.Errorf("in %s:", r.Method)
			t.Logf("url %s", r.URL.String())
			t.Logf("exp %q", ts.expSig)
			t.Logf("got %q", got)
		}

		// Reset Auth header and test signing with temporary credentials
		r.Header.Del("Authorization")
		DefaultService.Sign(r, tokenExKeys)
		if got := r.Header.Get("X-Amz-Security-Token"); got != tokenExKeys.SecurityToken {
			t.Errorf("in %s:", r.Method)
			t.Logf("url %s", r.URL.String())
			t.Logf("exp %q", tokenExKeys.SecurityToken)
			t.Logf("got %q", got)
		}
	}
}

var bucketTest = []struct {
	url string
	svc *Service
	w   string
}{
	{
		"http://johnsmith.s3.amazonaws.com/photos/puppy.jpg",
		DefaultService,
		"/johnsmith",
	},
	{
		"http://johnsmith.s3-ap-northeast-1.amazonaws.com/photos/puppy.jpg",
		DefaultService,
		"/johnsmith",
	},
	{
		"http://johnsmith.s3.amazonaws.com/?prefix=photos&max-keys=50&marker=puppy",
		DefaultService,
		"/johnsmith",
	},
	{
		"http://johnsmith.s3.amazonaws.com/?acl",
		DefaultService,
		"/johnsmith",
	},
	{
		"http://s3.amazonaws.com/johnsmith/photos/puppy.jpg",
		DefaultService,
		"",
	},
	{
		"http://static.johnsmith.net:8080/db-backup.dat.gz",
		DefaultService,
		"/static.johnsmith.net",
	},
	{
		"http://s3.amazonaws.com/",
		DefaultService,
		"",
	},
	{
		"http://s3.amazonaws.com/dictionary/fran%C3%A7ais/pr%C3%A9f%C3%A8re",
		DefaultService,
		"",
	},
	{
		"http://bucketname.S3.amazonaws.com/?delete",
		DefaultService,
		"/bucketname",
	},
}

func TestVhostBucket(t *testing.T) {
	for i, ts := range bucketTest {
		r, err := http.NewRequest("GET", ts.url, nil)
		if err != nil {
			panic(err)
		}
		var g bytes.Buffer
		ts.svc.writeVhostBucket(&g, r.Host)
		if g.String() != ts.w {
			t.Errorf("test %d: want %q, got %q", i, ts.w, g.String())
		}
	}
}
