// Copyright 2020 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
        "bufio"
        "bytes"
        "compress/gzip"
        "encoding/json"
        "fmt"
        "io"
        "net/http"
        "os"
        "strings"
        "sync"
)

const MaxBufferSize = 1 * 1024 * 1024

type IndexInS3 struct {
        BaseURL     string
        Crawl       string
        NumSegments int
}

type IndexFile struct {
        Segment string
        URL     string
}

var index IndexInS3 = IndexInS3{
        BaseURL:     "https://commoncrawl.s3.us-east-1.amazonaws.com/cc-index/collections/CC-MAIN-2020-16/indexes/",
        Crawl:       "CC-MAIN-2020-16",
        NumSegments: 300,
}

func (is3 IndexInS3) Files() []IndexFile {
        res := make([]IndexFile, is3.NumSegments)
        for i := 0; i < is3.NumSegments; i++ {
                segment := fmt.Sprintf("%05d", i)
                res[i] = IndexFile{
                        Segment: segment,
                        URL:     is3.BaseURL + "cdx-" + segment + ".gz",
                }
        }
        return res
}

func DownloadAndCreateSQLFile(collection string, f IndexFile) {
        fmt.Fprintln(os.Stderr, "Loading", collection, f.Segment)
        resp, err := http.Get(f.URL)
        if err != nil {
                panic(err)
        }
        filteredin, filteredout := io.Pipe()
        wg := &sync.WaitGroup{}
        wg.Add(2)
        go func() {
                defer wg.Done()
                defer filteredout.Close()
                FilterMatchingEntries(resp.Body, filteredout)
        }()
        go func() {
                defer wg.Done()
                outf, err := os.Create(collection + "-" + f.Segment + ".sql.gz")
                if err != nil {
                        panic(err)
                }
                defer func() {
                        if err := outf.Close(); err != nil {
                                panic(err)
                        }
                }()
                outw := gzip.NewWriter(outf)
                defer func() {
                        if err := outw.Close(); err != nil {
                                panic(err)
                        }
                }()
                EntriesToSQL(filteredin, outw, collection, f.Segment)
        }()
        wg.Wait()
        fmt.Fprintln(os.Stderr, "Finished loading", collection, f.Segment)
}

func FilterMatchingEntriesFromFile(filename string) {
        f, err := os.Open(filename)
        if err != nil {
                panic(err)
        }
        defer f.Close()
        FilterMatchingEntries(f, os.Stdout)
}

func FilterMatchingEntries(f io.Reader, out io.Writer) {
        gzr, err := gzip.NewReader(f)
        if err != nil {
                panic(err)
        }
        defer gzr.Close()
        s := bufio.NewScanner(gzr)
        buffer := make([]byte, MaxBufferSize)
        s.Buffer(buffer, MaxBufferSize)
        i := 0
        for s.Scan() {
                i++
                if bytes.Contains(s.Bytes(), []byte("privacy")) {
                        fmt.Fprintln(out, s.Text())
                } else if bytes.Contains(s.Bytes(), []byte("policy")) {
                        fmt.Fprintln(out, s.Text())
                } else if bytes.Contains(s.Bytes(), []byte("terms")) {
                        fmt.Fprintln(out, s.Text())
                }
        }
        if s.Err() != nil {
                panic(s.Err())
        }
        fmt.Fprintln(os.Stderr, "Filtered", i, "entries")
}

type EntryJSON struct {
        URL          string `json:"url"`
        Mime         string `json:"mime"`
        MimeDetected string `json:"mime-detected"`
        Status       string `json:"status"`
        Digest       string `json:"digest"`
        Length       string `json:"length"`
        Offset       string `json:"offset"`
        Filename     string `json:"filename"`
        Charset      string `json:"charset"`
        Languages    string `json:"languages"`
        Redirect     string `json:"redirect"`
        Truncated    string `json:"truncated"`
}

func SplitEntryLine(line string) (string, string, string) {
        tsi := strings.IndexByte(line, byte(' '))
        jsoni := strings.IndexByte(line[tsi+1:], byte(' '))
        return line[:tsi], line[tsi+1:tsi+1+jsoni], line[tsi+1+jsoni+1:]
}

func SQLEscape(field string) string {
        return strings.ReplaceAll(strings.ReplaceAll(field, "'", "''"), "\\", "\\\\")
}

func EntryToSQL(collection, segment, line string) string {
        if len(line) > 1024 {
                return ""
        }
        key, timestamp, entry := SplitEntryLine(line)
        d := json.NewDecoder(bytes.NewBuffer([]byte(entry)))
        d.DisallowUnknownFields()
        var ej EntryJSON
        err := d.Decode(&ej)
        if err != nil {
                panic(err)
        }
        return "insert into matching_index_entries values ('" +
                strings.Join([]string{
                        collection,
                        segment,
                        SQLEscape(key),
                        timestamp,
                        SQLEscape(ej.URL),
                        SQLEscape(ej.Mime),
                        ej.MimeDetected,
                        ej.Status,
                        ej.Digest,
                        ej.Length,
                        ej.Offset,
                        ej.Filename,
                        ej.Charset,
                        ej.Languages,
                        SQLEscape(ej.Redirect),
                }, "',\n'") + "');"
}

func EntriesToSQL(r io.Reader, w io.Writer, collection, segment string) {
        s := bufio.NewScanner(r)
        buffer := make([]byte, MaxBufferSize)
        s.Buffer(buffer, MaxBufferSize)
        i := 0
        for s.Scan() {
                i++
                line := EntryToSQL(collection, segment, s.Text())
                fmt.Fprintln(w, line)
                if i % 5000 == 0 {
                        fmt.Fprintln(os.Stderr, "Processed", i, "SQL entries.")
                }
        }
        if s.Err() != nil {
                panic(s.Err())
        }
}

var Usage = "Usage: reltuk_cc [filter FILENAME | tosql | indexfiles | loadsegments]"

func main() {
        os.Args = os.Args[1:]
        if len(os.Args) < 1 {
                panic(Usage)
        }
        if os.Args[0] == "filter" {
                os.Args = os.Args[1:]
                if len(os.Args) != 1 {
                        panic("Must supply filename.")
                }
                FilterMatchingEntriesFromFile(os.Args[0])
        } else if os.Args[0] == "tosql" {
                os.Args = os.Args[1:]
                EntriesToSQL(os.Stdin, os.Stdout, os.Args[0], os.Args[1])
        } else if os.Args[0] == "indexfiles" {
                files := index.Files()
                for _, f := range files {
                        fmt.Println(f.URL)
                }
        } else if os.Args[0] == "loadsegment" {
                files := index.Files()
                DownloadAndCreateSQLFile(index.Crawl, files[0])
        } else if os.Args[0] == "loadsegments" {
                files := index.Files()
                wg := &sync.WaitGroup{}
                work := make(chan func())
                for i := 0; i < 8; i++ {
                        wg.Add(1)
                        go func() {
                                defer wg.Done()
                                for w := range work {
                                        w()
                                }
                        }()
                }
                for i := 0; i < len(files); i++ {
                        f := files[i]
                        work <- func() {
                                DownloadAndCreateSQLFile(index.Crawl, f)
                        }
                }
                close(work)
                wg.Wait()
        } else {
                panic(Usage)
        }
}
