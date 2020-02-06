package index

import (
	"context"
	"log"
	"time"
)

const MaxRetries = 5

var BackoffIntervals []time.Duration = []time.Duration{
	time.Millisecond * 250,
	time.Millisecond * 1000,
	time.Millisecond * 3000,
	time.Millisecond * 7000,
	time.Millisecond * 15000,
}

func Retry(ctx context.Context, f func() error) error {
	var err error
	for i := 0; i < MaxRetries; i++ {
		err = f()
		if err == nil {
			return nil
		}
		log.Printf("retrying call: %v", err)
		timeout := time.Tick(BackoffIntervals[i])
		select {
		case <- ctx.Done():
			return err
		case <- timeout:
		}
	}
	return err
}
