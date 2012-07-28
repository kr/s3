package s3_test

import (
	"fmt"
	"github.com/kr/s3"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func ExampleSign() {
	keys := s3.Keys{
		os.Getenv("S3_ACCESS_KEY"),
		os.Getenv("S3_SECRET_KEY"),
	}
	data := strings.NewReader("hello, world")
	r, _ := http.NewRequest("PUT", "https://example.s3.amazonaws.com/foo", data)
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	r.Header.Set("Content-Length", strconv.Itoa(data.Len()))
	r.Header.Set("X-Amz-Acl", "public-read")
	s3.Sign(r, keys)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.StatusCode)
}
