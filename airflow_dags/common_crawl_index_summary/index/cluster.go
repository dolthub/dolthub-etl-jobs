package index

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Chunk size of 1 MB.
const chunkSize = 1 * 1024 * 1024

type clusterReader struct {
	Fetcher  fetcher
	Length   int64
	Mu       *sync.Mutex
	Chunks   [][]byte
}

func newClusterReader(ctx context.Context, f fetcher) (clusterReader, error) {
	len, err := f.Length(ctx, "cluster.idx")
	if err != nil {
		return clusterReader{}, err
	}
	return clusterReader{f, len, &sync.Mutex{}, make([][]byte, len/chunkSize)}, nil
}

func (c clusterReader) ByteAt(ctx context.Context, offset int) (byte, error) {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	cidx := offset / chunkSize
	if c.Chunks[cidx] == nil {
		start := (offset / chunkSize) * chunkSize
		end := start + chunkSize - 1
		val := make([]byte, chunkSize)
		err := Retry(ctx, func() error {
			body, err := c.Fetcher.Fetch(ctx, "cluster.idx", start, end)
			if err != nil {
				return err
			}
			defer body.Close()
			_, err = io.ReadFull(body, val)
			if err != nil && err != io.ErrUnexpectedEOF {
				return err
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
		c.Chunks[cidx] = val
	}
	return c.Chunks[cidx][offset%chunkSize], nil
}

type segmentRef struct {
	Key    string
	File   string
	Offset int
	Length int
}

func segmentRefAt(ctx context.Context, c clusterReader, offset int) (segmentRef, int, error) {
	lineEnd, err := nextNewlineFrom(ctx, c, offset)
	if err != nil {
		return segmentRef{}, -1, err
	}
	lineStart, err := prevNewlineFrom(ctx, c, lineEnd-1)
	if err != nil {
		return segmentRef{}, -1, err
	}
	lineStart++
	lineLength := lineEnd - lineStart
	bs := make([]byte, lineLength)
	j := 0
	for j < lineLength {
		bs[j], err = c.ByteAt(ctx, j + lineStart)
		if err != nil {
			return segmentRef{}, -1, err
		}
		j++
	}
	ref, err := parseSegmentRef(string(bs), offset)
	if err != nil {
		return segmentRef{}, -1, err
	}
	return ref, lineEnd+1, nil
}


func parseSegmentRef(line string, offset int) (segmentRef, error) {
	ls := bufio.NewScanner(strings.NewReader(line))
	ls.Split(bufio.ScanWords)
	if !ls.Scan() {
		return segmentRef{}, fmt.Errorf("expected index entry key at offset %d", offset)
	}
	key := ls.Text()
	if !ls.Scan() {
		return segmentRef{}, fmt.Errorf("expected index entry timestamp at offset %d", offset)
	}
	if !ls.Scan() {
		return segmentRef{}, fmt.Errorf("expected index entry filename at offset %d", offset)
	}
	filename := ls.Text()
	if !ls.Scan() {
		return segmentRef{}, fmt.Errorf("expected index entry offset at offset %d", offset)
	}
	offsetstr := ls.Text()
	if !ls.Scan() {
		return segmentRef{}, fmt.Errorf("expected index entry length at offset %d", offset)
	}
	lengthstr := ls.Text()
	ioffset, err := strconv.Atoi(offsetstr)
	if err != nil {
		return segmentRef{}, fmt.Errorf("parsed index entry offset failed at offset %d: %v", ioffset, err)
	}
	length, err := strconv.Atoi(lengthstr)
	if err != nil {
		return segmentRef{}, fmt.Errorf("parsed index entry length failed at offset %d: %v", offset, err)
	}
	return segmentRef{key, filename, ioffset, length}, nil
}

func nextNewlineFrom(ctx context.Context, r clusterReader, offset int) (int, error) {
	m := offset
	b, err := r.ByteAt(ctx, m)
	for err == nil && b != byte('\n') {
		m++
		b, err = r.ByteAt(ctx, m)
	}
	return m, err
}

func prevNewlineFrom(ctx context.Context, r clusterReader, offset int) (int, error) {
	n := offset
	b, err := r.ByteAt(ctx, n)
	for err == nil && b != byte('\n') && n >= 0 {
		n--
		if n == -1 {
			return n, nil
		}
		b, err = r.ByteAt(ctx, n)
	}
	return n, err
}

func getPrefixes(prefix string) ([]string, string) {
	prefixes := []string{prefix}
	search := prefix
	if strings.HasSuffix(prefix, ",") {
		prefixes = append(prefixes, prefix[0:len(prefix)-1]+")")
		search = prefixes[1]
	}
	return prefixes, search
}

func matchesPrefixes(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func segmentRefsForPrefix(ctx context.Context, c clusterReader, prefix string) ([]segmentRef, error) {
	prefixes, search := getPrefixes(prefix)
	var capturederr error
	idx := sort.Search(int(c.Length), func(idx int) bool {
		if capturederr != nil {
			return false
		}
		s, _, err := segmentRefAt(ctx, c, idx)
		if err != nil {
			capturederr = err
			return false
		}
		return s.Key >= search
	})
	if capturederr != nil {
		return nil, capturederr
	}
	if idx > 0 {
		idx--
	}
	s, next, err := segmentRefAt(ctx, c, idx)
	if err != nil {
		return nil, err
	}
	res := []segmentRef{s}
	idx = next
	for idx < int(c.Length) {
		s, next, err := segmentRefAt(ctx, c, idx)
		if err != nil {
			return nil, err
		}
		if !matchesPrefixes(s.Key, prefixes) {
			break
		}
		res = append(res, s)
		idx = next
	}
	return res, nil
}
