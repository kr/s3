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

type ContentsType string

const (
	ContentsFile   = "file"
	ContentsFolder = "folder"
)

const folderSuffix1 = "/"
const folderSuffix2 = "_$folder$"

type Contents struct {
	Type         ContentsType
	Key          string
	LastModified string
	ETag         string
	Size         string
	StorageClass string
	Owner        Owner
}

type contentsSorter struct {
	c []Contents
}

func (s *contentsSorter) Len() int           { return len(s.c) }
func (s *contentsSorter) Swap(i, j int)      { s.c[i], s.c[j] = s.c[j], s.c[i] }
func (s *contentsSorter) Less(i, j int) bool { return s.c[i].Key < s.c[j].Key }

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

func isFolder(contents *Contents) bool {
	return contents.Size == "0" &&
		(strings.HasSuffix(contents.Key, folderSuffix1) ||
			strings.HasSuffix(contents.Key, folderSuffix2))
}

func decodeListObjectsResult(reader io.ReadCloser) (*ListObjectsResult, error) {
	defer reader.Close()
	decoder := xml.NewDecoder(reader)
	result := ListObjectsResult{}
	err := decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	sorted := true
	for i := 0; i < len(result.Contents); i++ {
		contents := &result.Contents[i]
		if isFolder(contents) {
			if strings.HasSuffix(contents.Key, folderSuffix1) {
				contents.Key = strings.TrimSuffix(contents.Key, folderSuffix1)
			} else if strings.HasSuffix(contents.Key, folderSuffix2) {
				contents.Key = strings.TrimSuffix(contents.Key, folderSuffix2)
				sorted = false
			}
			contents.Type = ContentsFolder
		} else {
			contents.Type = ContentsFile
		}
		contents.ETag = strings.Trim(contents.ETag, `"`)
	}
	if !sorted {
		sort.Sort(&contentsSorter{result.Contents})
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
