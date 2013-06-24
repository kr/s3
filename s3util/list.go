package s3util

import (
	"encoding/xml"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type Owner struct {
	ID          string
	DisplayName string
}

const (
	FolderSuffix1 = "/"
	FolderSuffix2 = "_$folder$"
)

type Content struct {
	Key          string // the original key at S3 servers
	LastModified string
	ETag         string // ETag value (doublequotes are trimmed)
	Size         string // Note type is string. See http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
	StorageClass string
	Owner        Owner
}

// Returns key with folder suffix trimmed
func (c *Content) Path() string {
	if strings.HasSuffix(c.Key, FolderSuffix1) {
		return strings.TrimSuffix(c.Key, FolderSuffix1)
	} else if strings.HasSuffix(c.Key, FolderSuffix2) {
		return strings.TrimSuffix(c.Key, FolderSuffix2)
	} else {
		return c.Key
	}
}

func (c *Content) IsDir() bool {
	return c.Size == "0" &&
		(strings.HasSuffix(c.Key, FolderSuffix1) ||
			strings.HasSuffix(c.Key, FolderSuffix2))
}

type ListObjectsResult struct {
	Name        string
	Prefix      string
	Marker      string
	MaxKeys     string
	IsTruncated bool
	Contents    []Content
}

func openObjectsList(url string, c *Config) (io.ReadCloser, error) {
	if c == nil {
		c = DefaultConfig
	}
	r, _ := http.NewRequest("GET", url, nil)
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	c.Sign(r, *c.Keys)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, newRespError(resp)
	}
	return resp.Body, nil

}

func decodeListObjectsResult(reader io.ReadCloser) (*ListObjectsResult, error) {
	decoder := xml.NewDecoder(reader)
	result := ListObjectsResult{}
	err := decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	for _, content := range result.Contents {
		content.ETag = strings.Trim(content.ETag, `"`)
	}
	return &result, nil
}

func ListObjects(url string, c *Config) (*ListObjectsResult, error) {
	reader, err := openObjectsList(url, c)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return decodeListObjectsResult(reader)
}
