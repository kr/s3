package s3util_test

import (
	"fmt"
	"github.com/kr/s3/s3util"
	"io"
	"os"
)

func ExampleCreate() {
	s3util.DefaultConfig.AccessKey = "...access key..."
	s3util.DefaultConfig.SecretKey = "...secret key..."
	r, _ := os.Open("/dev/stdin")
	w, _ := s3util.Create("https://mybucket.s3.amazonaws.com/log.txt", nil, nil)
	io.Copy(w, r)
	w.Close()
}

func ExampleOpen() {
	s3util.DefaultConfig.AccessKey = "...access key..."
	s3util.DefaultConfig.SecretKey = "...secret key..."
	r, _ := s3util.Open("https://mybucket.s3.amazonaws.com/log.txt", nil)
	w, _ := os.Create("out.txt")
	io.Copy(w, r)
	w.Close()
}

func ExampleListObjects() {
	s3util.DefaultConfig.AccessKey = "...access key..."
	s3util.DefaultConfig.SecretKey = "...secret key..."
	url := "https://mybucket.s3.amazonaws.com"
	result, err := s3util.ListObjects(url, nil)
	fmt.Printf("err: %v\n", err)
	if err != nil {
		return
	}
	fmt.Printf("Name: %v\n", result.Name)
	fmt.Printf("Prefix: %v\n", result.Prefix)
	fmt.Printf("Marker: %v\n", result.Marker)
	fmt.Printf("MaxKeys: %v\n", result.MaxKeys)
	fmt.Printf("IsTruncated: %v\n", result.IsTruncated)
	for _, content := range result.Contents {
		fmt.Printf("\n")
		fmt.Printf("Type: %v\n", content.Type)
		fmt.Printf("Key: %v\n", content.Key)
		fmt.Printf("ETag: %v\n", content.ETag)
		fmt.Printf("Size: %v\n", content.Size)
		fmt.Printf("StorageClass: %v\n", content.StorageClass)
		fmt.Printf("OwnerID: %v\n", content.Owner.ID)
		fmt.Printf("OwnerDisplayName: %v\n", content.Owner.DisplayName)
	}
}
