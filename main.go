package main

import (
	"context"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const zeroTimeout = time.Duration(0)

type uploadObject struct {
	bucket string
	key    string
	rc     io.ReadCloser
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func(cancel context.CancelFunc) {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		sig := <-ch
		log.Printf("received signal %s, shutting down", sig)
		cancel()
	}(cancel)

	opts, err := processFlags(ctx, os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	// if profiling or tracing flags were specified, activate them
	if shutdown, err := profilers(opts); err != nil {
		log.Printf("unable to initialize profilers: %s", err)
	} else {
		defer shutdown()
	}

	// if -media-types was specified, load them
	if opts.MediaTypes != "" {
		fh, err := os.Open(opts.MediaTypes)
		if err != nil {
			log.Fatalf("unable to open -media-types file: %s: %s",
				opts.MediaTypes, err)
		}

		err = ExtendMediaTypes(fh)
		fh.Close()

		if err != nil {
			log.Fatalf("unable to load -media-types: %s: %s",
				opts.MediaTypes, err)
		}
	}

	// initialize the uploader
	uploader := NewUploader(ctx, opts)

	// setup result handler
	completed := make(chan *UploadResults)
	reporting := &sync.WaitGroup{}

	reporting.Add(1)
	go func(ch chan *UploadResults, reporting *sync.WaitGroup) {
		defer reporting.Done()

		manifest := Manifest(opts.Manifest, os.Stdout)
		defer manifest.End()

		for res := range ch {
			if res.Error != nil {
				log.Printf("error uploading object %s/%s: %s", res.Bucket, res.Key, res.Error)

			} else {
				if opts.Verbose {
					log.Printf("completed uploading object %s/%s", res.Bucket, res.Key)
				}

				obj, err := NewObjectReporting(res.State)
				if err != nil {
					log.Printf("error creating manfiest for object: %s", err)
				} else {
					err = manifest.Write(obj)
					if err != nil {
						log.Printf("error writing manifest: %s", err)
					}
				}
			}
		}
	}(completed, reporting)

	// start processing file globs for objects to upload
	to_upload, err := processGlobs(
		opts.globs, opts.bucket, opts.key, opts.Recursive, opts.Verbose)
	if err != nil {
		log.Fatal(err)
	}

	for obj := range to_upload {
		uploaded := uploader.Upload(ctx, obj.rc, obj.bucket, obj.key)
		go func(rc io.ReadCloser, uploaded, completed chan *UploadResults) {
			defer rc.Close()
			res := <-uploaded
			completed <- res
		}(obj.rc, uploaded, completed)
	}

	// wait until uploader has completed (or been canceled)
	uploader.Wait(zeroTimeout)

	if pending := uploader.Pending(); len(pending) != 0 {
		if opts.LeavePartsOnError {
			for i := range pending {
				target := uploader.PendingTarget(pending[i])
				if target != "" {
					log.Printf("pending uploads detected: %s (upload-id %s)",
						target, *pending[i])
				}
			}

		} else {
			if context.Cause(ctx) != nil {
				ctx, cancel = context.WithCancel(context.Background())
				defer cancel()

				go func(cancel context.CancelFunc) {
					ch := make(chan os.Signal, 1)
					signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
					sig := <-ch
					log.Printf("received signal %s, giving up on pending aborts...", sig)
					cancel()
				}(cancel)
			}

			for i := range pending {
				target := uploader.PendingTarget(pending[i])
				if target != "" {
					log.Printf("attempting to abort pending upload: %s (upload-id %s)",
						target, *pending[i])
				}
			}

			uploader.AbortPending(ctx)
		}
	}

	// wait until reporting has completed
	close(completed)
	reporting.Wait()
}
