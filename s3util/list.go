package s3util

import (
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"time"
)

type Owner struct {
	ID          string
	DisplayName string
}

type ContentsType string

const (
	ContentsFile   = "file"
	ContentsFolder = "folder"
)
const folderSuffix = "_$folder$"

type Contents struct {
	Type         ContentsType
	Key          string
	LastModified string
	ETag         string
	Size         string
	StorageClass string
	Owner        Owner
}

type ListObjectsResult struct {
	Name        string
	Prefix      string
	Marker      string
	MaxKeys     string
	IsTruncated bool
	Contents    []Contents
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
	defer reader.Close()
	decoder := xml.NewDecoder(reader)
	result := ListObjectsResult{}
	err := decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(result.Contents); i++ {
		contents := &result.Contents[i]
		if strings.HasSuffix(contents.Key, folderSuffix) {
			contents.Key = strings.TrimSuffix(contents.Key, folderSuffix)
			contents.Type = ContentsFolder
		} else {
			contents.Type = ContentsFile
		}
		contents.ETag = strings.Trim(contents.ETag, `"`)
	}
	return &result, nil
}

func ListObjects(url string, c *Config) (*ListObjectsResult, error) {
	reader, err := openObjectsList(url, c)
	if err != nil {
		return nil, err
	}

	result, err := decodeListObjectsResult(reader)
	return result, err
}
