package s3

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func ExampleSign() {
	keys := Keys{
		os.Getenv("S3_ACCESS_KEY"),
		os.Getenv("S3_SECRET_KEY"),
	}
	data := strings.NewReader("hello, world")
	r, _ := http.NewRequest("PUT", "https://example.s3.amazonaws.com/foo", data)
	r.ContentLength = int64(data.Len())
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	r.Header.Set("X-Amz-Acl", "public-read")
	Sign(r, keys)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(resp.StatusCode)
}
