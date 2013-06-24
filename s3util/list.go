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

const (
	FolderSuffix1 = "/"
	FolderSuffix2 = "_$folder$"
)

type Content struct {
	Type         ContentsType
	Key          string  // the original key at S3 servers
	Path         string  // key with folder suffix trimmed
	LastModified string
	ETag         string
	Size         string
	StorageClass string
	Owner        Owner
}

type contentsSorter struct {
	c []Content
}

func (s *contentsSorter) Len() int           { return len(s.c) }
func (s *contentsSorter) Swap(i, j int)      { s.c[i], s.c[j] = s.c[j], s.c[i] }
func (s *contentsSorter) Less(i, j int) bool { return s.c[i].Path < s.c[j].Path }

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

func isFolder(contents *Content) bool {
	return contents.Size == "0" &&
		(strings.HasSuffix(contents.Key, FolderSuffix1) ||
			strings.HasSuffix(contents.Key, FolderSuffix2))
}

func decodeListObjectsResult(reader io.ReadCloser) (*ListObjectsResult, error) {
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
			if strings.HasSuffix(contents.Key, FolderSuffix1) {
				contents.Path = strings.TrimSuffix(contents.Key, FolderSuffix1)
			} else if strings.HasSuffix(contents.Key, FolderSuffix2) {
				contents.Path = strings.TrimSuffix(contents.Key, FolderSuffix2)
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
	defer reader.Close()

	return decodeListObjectsResult(reader)
}
