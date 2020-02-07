package index

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
)

type IndexEntry struct {
	URL          string `json:"url"`
	Mime         string `json:"mime"`
	MimeDetected string `json:"mime-detected"`
	Status       string `json:"status"`
	Digest       string `json:"digest"`
	Length       string `json:"length"`
	Offset       string `json:"offset"`
	Filename     string `json:"filename"`
	Redirect     string `json:"redirect"`
	Languages    string `json:"languages"`
	Charset      string `json:"charset"`
	URLParsed    *url.URL
	LengthI      int
}

func (e IndexEntry) Host() string {
	return strings.ToLower(e.URLParsed.Hostname())
}

type IndexEntries interface {
	Next(ctx context.Context) bool
	Err() error
	Entry() IndexEntry
}

type Index interface {
	Name() string
	EntriesForPrefix(ctx context.Context, key string) IndexEntries
}

type fetcher interface {
	Fetch(ctx context.Context, key string, start, end int) (io.ReadCloser, error)
	Length(ctx context.Context, key string) (int64, error)
}

type index struct {
	Fetcher       fetcher
	ClusterReader clusterReader
}

type entries struct {
	fetcher fetcher
	refs    []segmentRef
	idx     int
	reader  io.ReadCloser
	scanner scanner
	entry   IndexEntry
	err     error
}

func NewIndex(ctx context.Context, f fetcher) (Index, error) {
	cr, err := newClusterReader(ctx, f)
	if err != nil {
		return nil, err
	}
	return index{f, cr}, nil
}

type scanner interface {
	Scan() bool
	Text() string
	Err() error
}

func (i index) EntriesForPrefix(ctx context.Context, key string) IndexEntries {
	refs, err := segmentRefsForPrefix(ctx, i.ClusterReader, key)
	if err != nil {
		return &entries{i.Fetcher, nil, 0, nil, nil, IndexEntry{}, err}
	}
	return &entries{i.Fetcher, refs, 0, nil, nil, IndexEntry{}, nil}
}

func (i index) Name() string {
	return i.Fetcher.(S3Fetcher).Prefix
}

func (e *entries) Err() error {
	return e.err
}

func (e *entries) Next(ctx context.Context) bool {
	for true {
		if e.err != nil {
			return false
		}
		if e.scanner == nil {
			if e.idx == len(e.refs) {
				return false
			} else {
				reader, err := fetchSegment(ctx, e.refs[e.idx], e.fetcher)
				if err != nil {
					e.err = err
					return false
				}
				e.idx++
				e.reader = reader
				scanner := bufio.NewScanner(reader)
				scanner.Buffer(nil, 10*1024*1024)
				e.scanner = scanner
			}
		}
		if e.scanner.Scan() {
			entry, err := parseEntry(e.scanner.Text())
			if err != nil {
				e.err = err
				e.scanner = nil
				e.reader.Close()
				return false
			}
			e.entry = entry
			return true
		} else {
			e.err = e.scanner.Err()
			e.scanner = nil
			e.reader.Close()
		}
	}
	return false
}

func (e *entries) Entry() IndexEntry {
	return e.entry
}

func fetchSegment(ctx context.Context, r segmentRef, f fetcher) (io.ReadCloser, error) {
	var res io.ReadCloser
	err := Retry(ctx, func() error {
		zr, err := f.Fetch(ctx, r.File, r.Offset, r.Offset+r.Length-1)
		if err != nil {
			return err
		}
		res, err = gzip.NewReader(zr)
		if err != nil {
			zr.Close()
			return err
		}
		return nil
	})
	return res, err
}

func parseEntry(line string) (IndexEntry, error) {
	splits := strings.SplitN(line, " ", 3)
	var res IndexEntry
	if len(splits) != 3 {
		return res, fmt.Errorf("error parsing line; expected 3 key, timestamp, entry format.")
	}
	err := json.Unmarshal([]byte(splits[2]), &res)
	if err != nil {
		return res, err
	}
	res.URLParsed, err = url.Parse(res.URL)
	if err != nil {
		return res, err
	}
	res.LengthI, err = strconv.Atoi(res.Length)
	return res, err
}
