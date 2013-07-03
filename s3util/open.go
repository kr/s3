package s3util

import (
	"../../s3"
	"io"
	"net/http"
	"time"
)

// Open requests the S3 object at url. An HTTP status other than 200 is
// considered an error.
//
// If signer is nil, Open uses DefaultConfig.
func Open(url string, signer s3.Signer) (io.ReadCloser, error) {
	if signer == nil {
		signer = DefaultConfig
	}
	// TODO(kr): maybe parallel range fetching
	r, _ := http.NewRequest("GET", url, nil)
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	signer.Sign(r)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, newRespError(resp)
	}
	return resp.Body, nil
}
