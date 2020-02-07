package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/liquidata-inc/liquidata-etl-jobs/airflow_dags/common_crawl_index_summary/index"
)

var IndexFetchers []index.S3Fetcher = []index.S3Fetcher{
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-05/indexes/"}, // 0
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-09/indexes/"}, // 1
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-13/indexes/"}, // 2
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-17/indexes/"}, // 3
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-22/indexes/"}, // 4
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-26/indexes/"}, // 5
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-30/indexes/"}, // 6
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-34/indexes/"}, // 7
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-39/indexes/"}, // 8
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-43/indexes/"}, // 9
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-47/indexes/"}, // 10
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2018-51/indexes/"}, // 11
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-04/indexes/"}, // 12
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-09/indexes/"}, // 13
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-13/indexes/"}, // 14
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-18/indexes/"}, // 15
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-22/indexes/"}, // 16
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-26/indexes/"}, // 17
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-30/indexes/"}, // 18
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-35/indexes/"}, // 19
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-39/indexes/"}, // 20
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-43/indexes/"}, // 21
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-47/indexes/"}, // 22
	index.S3Fetcher{Client: nil, Bucket: "commoncrawl", Prefix: "cc-index/collections/CC-MAIN-2019-51/indexes/"}, // 23
}

func DomainToWebsite(domain string) Website {
	parts := strings.Split(domain, ".")
	key := ""
	for i := len(parts) - 1; i >= 0; i-- {
		key += parts[i]
		key += ","
	}
	return Website{domain, key}
}

type Website struct {
	Domain string
	Key    string
}

var domains []string = []string{
	"wikipedia.org",
	"google.com",
	"youtube.com",
	"twitter.com",
	"facebook.com",
	"amazon.com",
	"imdb.com",
	"merriam-webster.com",
	"apple.com",
	"dictionary.com",
	"instagram.com",
	"wiktionary.org",
	"tripadvisor.com",
	"urbandictionary.com",
	"fandom.com",
	"pinterest.com",
	"yahoo.com",
	"yelp.com",
	"cambridge.org",
	"weather.com",
	"craigslist.org",
	"espn.com",
	"roblox.com",
	"britannica.com",
	"webmd.com",
	"bbc.com",
	"espncricinfo.com",
	"linkedin.com",
	"cricbuzz.com",
	"live.com",
	"thesaurus.com",
	"microsoft.com",
	"healthline.com",
	"bestbuy.com",
	"whatsapp.com",
	"spanishdict.com",
	"genius.com",
	"rottentomatoes.com",
	"walmart.com",
	"xvideos.com",
	"timeanddate.com",
	"pornhub.com",
	"indiatimes.com",
	"mayoclinic.org",
	"theguardian.com",
	"poki.com",
	"office.com",
	"gsmarena.com",
	"ytmp3.cc",
	"livescore.com",
	"ryanair.com",
	"thefreedictionary.com",
	"steampowered.com",
	"accuweather.com",
	"samsung.com",
	"homedepot.com",
	"spotify.com",
	"cnn.com",
	"vocabulary.com",
	"y2mate.com",
	"xnxx.com",
	"cnet.com",
	"uptodown.com",
	"nytimes.com",
	"reddit.com",
	"netflix.com",
	"nih.gov",
	"bbc.co",
	"thepiratebay.org",
	"mp3-youtube.download",
	"ndtv.com",
	"collinsdictionary.com",
	"investopedia.com",
	"blog.google",
	"apkpure.com",
	"ebay.com",
	"flashscore.com",
	"playstation.com",
	"speedtest.net",
	"softonic.com",
	"skyscanner.com",
	"foxnews.com",
	"thepiratebays3.com",
	"indeed.com",
	"techradar.com",
	"yourdictionary.com",
	"mcdonalds.com",
	"medicalnewstoday.com",
	"onlinesbi.com",
	"globo.com",
	"dominos.com",
	"nba.com",
	"target.com",
	"xe.com",
	"adobe.com",
	"twitch.tv",
	"macys.com",
	"usnews.com",
	"friv.com",
	"github.com",
}

type StatKey struct {
	Host         string
	Status       string
	MimeDetected string
	Languages    string
}

type StatVal struct {
	Count int
	Size  int
}

type StatResults struct {
	Prefix string
	Stats  map[StatKey]StatVal
}

func cleanStr(s string) string {
	if s == "" {
		return `""`
	}
	return s
}

func (sr StatResults) Print(w io.Writer) {
	for k, v := range sr.Stats {
		fmt.Fprintf(
			w,
			"%s|%s|%s|%s|%s|%d|%d\n",
			k.Host,
			sr.Prefix,
			cleanStr(k.Status),
			cleanStr(k.MimeDetected),
			cleanStr(k.Languages),
			v.Count,
			v.Size,
		)
	}
}

const CountByHost = true

func Run(ctx context.Context, client *s3.S3, i index.Index, website Website) (StatResults, error) {
	stats := make(map[StatKey]StatVal)

	entries := i.EntriesForPrefix(ctx, website.Key)
	for entries.Next(ctx) {
		entry := entries.Entry()
		host := entry.Host()
		if host == website.Domain || strings.HasSuffix(host, "."+website.Domain) {
			hostkey := host
			if !CountByHost {
				hostkey = website.Domain
			}
			key := StatKey{hostkey, entry.Status, entry.MimeDetected, entry.Languages}
			cur := stats[key]
			cur.Count++
			cur.Size += entry.LengthI
			stats[key] = cur
		}
		break
	}
	if entries.Err() != nil {
		return StatResults{}, entries.Err()
	}

	return StatResults{i.Name(), stats}, nil
}

func main() {
	ctx := context.Background()

	client := s3.New(session.Must(session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.AnonymousCredentials,
	})))

	indexes := make([]index.Index, len(IndexFetchers))
	for i, f := range IndexFetchers {
		f.Client = client
		index, err := index.NewIndex(ctx, f)
		if err != nil {
			log.Fatalf("err constructing index: %v", err)
		}
		indexes[i] = index
	}

	results := make(chan StatResults)

	type work struct {
		w Website
		i index.Index
	}

	doWork := func(w work) StatResults {
		res, err := Run(ctx, client, w.i, w.w)
		if err != nil {
			log.Fatalf("err running search: %v", err)
		}
		return res
	}

	works := make(chan work)

	wg := &sync.WaitGroup{}
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			for w := range works {
				results <- doWork(w)
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	if len(os.Args) != 2 {
		log.Fatalf("Usage: common_crawl_index_summary OUTPUT_FILE.PSV")
	}
	outfilename := os.Args[1]
	outfile, err := os.OpenFile(outfilename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatalf("Error opening %s: %v", outfilename, err)
	}
	defer outfile.Close()

	workcount := len(indexes) * len(domains)

	go func() {
		for _, i := range indexes {
			for _, w := range domains {
				works <- work{DomainToWebsite(w), i}
			}
		}
		close(works)
	}()

	done := 0
	lastprint := time.Now()
	for r := range results {
		r.Print(outfile)
		done++
		if done%25 == 0 || time.Now().After(lastprint.Add(60*time.Second)) {
			log.Printf("%4d / %4d done.", done, workcount)
			lastprint = time.Now()
		}
	}
}
