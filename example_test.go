package s3_test

import (
	"fmt"
	"github.com/kr/s3"
	"github.com/kr/s3/s3util"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func ExampleSign() {
	keys := s3.Keys{
		AccessKey: os.Getenv("S3_ACCESS_KEY"),
		SecretKey: os.Getenv("S3_SECRET_KEY"),
	}
	data := strings.NewReader("hello, world")
	r, _ := http.NewRequest("PUT", "https://example.s3.amazonaws.com/foo", data)
	r.ContentLength = int64(data.Len())
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	r.Header.Set("X-Amz-Acl", "public-read")
	s3.Sign(r, keys)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(resp.StatusCode)
}

func ExampleList() {
	s3util.DefaultConfig.AccessKey = os.Getenv("S3_ACCESS_KEY")
	s3util.DefaultConfig.SecretKey = os.Getenv("S3_SECRET_KEY")
	f, err := s3util.NewFile("https://examle.s3.amazonaws.com/foo", nil)
	if err != nil {
		panic(err)
	}
	var infos []os.FileInfo
	for {
		infos, err = f.List(0)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		for i, info := range infos {
			c := info.Sys().(*s3util.Stat)
			var etag string
			if c != nil {
				etag = c.ETag
			}
			fmt.Printf("%d: %v, %s\n", i, info, etag)
		}
	}
}
