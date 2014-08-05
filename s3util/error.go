package s3util

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

type respError struct {
	r *http.Response
	b bytes.Buffer
}

func newRespError(r *http.Response) *respError {
	e := new(respError)
	e.r = r
	io.Copy(&e.b, r.Body)
	r.Body.Close()
	return e
}

func (e *respError) Error() string {
	return fmt.Sprintf(
		"unwanted http status %d: %q",
		e.r.StatusCode,
		e.b.String(),
	)
}

func (e *respError) StatusCode() int {
	return e.r.StatusCode
}

func GetRespCode(e error) int {
	err, ok := e.(*respError)
	if !ok {
		return 0
	}

	return err.StatusCode()
}
