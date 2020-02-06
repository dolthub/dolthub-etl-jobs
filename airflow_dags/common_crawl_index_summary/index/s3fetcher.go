package index

import (
	"context"
	"io"
	"strconv"

        "github.com/aws/aws-sdk-go/aws"
        "github.com/aws/aws-sdk-go/service/s3"
)

type S3Fetcher struct {
	Client *s3.S3
	Bucket string
	Prefix string
}

func (f S3Fetcher) Fetch(ctx context.Context, key string, start, end int) (io.ReadCloser, error) {
	req := &s3.GetObjectInput{
		Bucket: aws.String(f.Bucket),
		Key:    aws.String(f.Prefix + key),
		Range:  aws.String("bytes=" + strconv.Itoa(start) + "-" + strconv.Itoa(end)),
	}
	resp, err := f.Client.GetObjectWithContext(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (f S3Fetcher) Length(ctx context.Context, key string) (int64, error) {
	req := &s3.HeadObjectInput{
		Bucket: aws.String(f.Bucket),
		Key:    aws.String(f.Prefix + key),
	}
	resp, err := f.Client.HeadObjectWithContext(ctx, req)
	if err != nil {
		return -1, err
	}
	return *resp.ContentLength, nil
}
