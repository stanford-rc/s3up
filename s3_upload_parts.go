package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3UploadParts manages the process of a multi-part upload.  Callers should
// use UploadPart, CompleteUpload, and AbortUpload to add parts and finalize
// the process.
type S3UploadParts struct {
	// ctx is a cancelable context that may be used to short-circuit
	// processing of in-flight part uploads
	ctx context.Context

	// cancel may be used to cancel the context
	cancel context.CancelCauseFunc

	// st tracks the state of this upload
	st *S3UploadState

	// ch provides a channel for submitting parts to upload
	ch chan *queuedPart

	// pending tracks the number of queued parts still pending
	pending *sync.WaitGroup

	// opts records the Options provided when this S3UploadParts was
	// created
	opts *Options

	// mu is a shared lock for any operations that might need to be gated
	// for concurrency safety
	mu *sync.Mutex

	// callers may optionally use NextPartID to generate the PartID for
	// their uploaded parts, and this counter tracks the next available
	// PartID.
	lastPartID int32
}

// NewS3UploadParts initializes a new S3UploadPart.  The context may be used to
// cancel any in-flight uploads.  The S3Hasher hr should be used to provide the
// hashed signatures of parts submitted via UploadPart (see S3HashReader and
// S3HashWriter).
func NewS3UploadParts(
	ctx context.Context,
	hr *S3Hasher,
	create *s3.CreateMultipartUploadInput,
	opts *Options) (*S3UploadParts, error) {

	ctx, cancel := context.WithCancelCause(ctx)

	s3client := opts.s3.Get()
	out, err := s3client.CreateMultipartUpload(ctx, create)
	opts.s3.Put(s3client)

	if err != nil {
		return nil, err
	}

	if opts.Verbose {
		log.Printf("started upload of multi-part object %s/%s using UploadId %s",
			*create.Bucket, *create.Key, *out.UploadId)
	}

	p := &S3UploadParts{
		st: &S3UploadState{
			hr:           hr,
			create:       create,
			createOutput: out,

			uploadPartOutputs: make(map[int32]*s3.UploadPartOutput),
			uploadPartErrors:  make(map[int32]error),
		},

		ctx:    ctx,
		cancel: cancel,

		ch: make(chan *queuedPart),

		pending: &sync.WaitGroup{},

		opts: opts,

		mu: &sync.Mutex{},
	}

	for i := 0; i < p.opts.ConcurrentParts; i++ {
		go func() {
			for {
				select {
				case q := <-p.ch:
					// received queuedPart
					select {
					case q.ch <- p.uploadPart(q.part):
						// return results of upload
					case <-p.ctx.Done():
						// aborted due to canceled context
						return
					}
				case <-p.ctx.Done():
					// aborted due to canceled context
					return
				}
			}
		}()
	}

	return p, nil
}

var ErrMaxPartID = errors.New("partID limit reached")

// UploadID returns the in-flight UploadID for this multi-part upload, note
// that the id can be invalidated once CompleteMultipartUpload or
// AbortMultipartUpload are called.
func (p *S3UploadParts) UploadID() *string {
	if p.st.createOutput != nil && p.st.createOutput.UploadId != nil {
		return p.st.createOutput.UploadId
	}
	return nil
}

func (p *S3UploadParts) Bucket() *string {
	if p.st.create != nil && p.st.create.Bucket != nil {
		return p.st.create.Bucket
	}
	return nil
}

func (p *S3UploadParts) Key() *string {
	if p.st.create != nil && p.st.create.Key != nil {
		return p.st.create.Key
	}
	return nil
}

// NextPartID is a convenience method to return a sequence of PartID starting
// at 1 and ending at Options.MaxPartID.  If MaxPartID has been reached then
// (0, ErrMaxPartID) is returned.
func (p *S3UploadParts) NextPartID() (int32, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.lastPartID == p.opts.MaxPartID {
		return 0, ErrMaxPartID
	}

	p.lastPartID += 1

	return p.lastPartID, nil
}

// Cancel will cancel the context associated with this S3UploadPart.  After
// Cancel has been called the S3UploadPart will not process any more UploadPart
// requests, but AbortUpload and CompleteUpload may still be called.
func (p *S3UploadParts) Cancel(err error) {
	p.cancel(err)
}

// Canceled returns true if the context associated with this S3UploadPart has
// been canceled.
func (p *S3UploadParts) Canceled() bool {
	select {
	case <-p.ctx.Done():
		return true
	default:
		return false
	}
}

// UploadPart submits part *s3.UploadPartInput to the worker routines for
// processing.
//
// The returned channel may optionally be polled to determine if there was an
// error processing the request, either because the context was canceled or
// because of an upload error.  If there was an error it will also be available
// via PartResults usng the part's PartNumber.
//
// A caller may access the p.PartResults values after the return channel has
// been written to or after p.Wait unblocks.
func (p *S3UploadParts) UploadPart(part *s3.UploadPartInput) chan error {
	// increment the pending WaitGroup by one
	p.pending.Add(1)

	q := &queuedPart{
		// record the record the s3.UploadPartInput to process
		part: part,

		// channel is size 1 so that reading the result is optional for
		// the caller
		ch: make(chan error, 1),
	}

	go func(q *queuedPart) {
		select {
		case p.ch <- q:
			// accepted for processing by a worker, it is now the
			// responsibility of the uploadPart method to decrement
			// the pending WaitGroup.
		case <-p.ctx.Done():
			// aborted due to canceled context

			// decrement pending WaitGroup by one
			p.pending.Done()

			// record the cause of the cancelation
			err := context.Cause(p.ctx)

			// for this part number record the cancelation error a
			// the results
			p.st.setPartResults(q.part.PartNumber, nil, err)

			// and return the cancelation error back to the caller
			// if they are waiting for it
			q.ch <- err
		}
	}(q)

	return q.ch
}

// uploadPart actually submits the s3 client request to upload the part,
// records the outcome, and returns any error
func (p *S3UploadParts) uploadPart(part *s3.UploadPartInput) error {
	defer p.pending.Done()

	s3client := p.opts.s3.Get()
	defer p.opts.s3.Put(s3client)

	if p.opts.Verbose {
		log.Printf("starting upload of %s/%s part %d using UploadId %s",
			*part.Bucket, *part.Key, *part.PartNumber, *part.UploadId)
	}

	out, err := s3client.UploadPart(p.ctx, part)

	p.st.setPartResults(part.PartNumber, out, err)

	return err
}

// Wait blocks until all the parts submitted via p.UploadPart have finished
// processing or have been rejected due to a canceled context, or until the
// underlying context has been canceled, or until any > 0 timeout has been
// reached.
func (p *S3UploadParts) Wait(timeout time.Duration) error {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	done := make(chan bool, 1)
	go func() {
		p.pending.Wait()
		done <- true
	}()

	select {
	case <-done:
		return nil
	case <-p.ctx.Done():
		return context.Cause(p.ctx)
	case t := <-timer:
		return fmt.Errorf("%w: %s (%s)", ErrTimeout, t, timeout)
	}
}

// PartResults returns the results for a part's PartNumber.  It is only
// guaranteed to return a result after p.Wait unblocks or after the original
// UploadPart's error channel has returned a value.
func (p *S3UploadParts) PartResults(partID int32) (*s3.UploadPartOutput, error) {
	return p.st.uploadPartOutputs[partID], p.st.uploadPartErrors[partID]
}

// CompleteUpload attempts to complete an upload of parts.  It should only be
// called once all the parts have been submitted via p.UploadPart and p.Wait
// has unblocked.  If timeout is > 0 then the complete upload process will try
// to cancel the process if it takes longer than the specified timeout.
func (p *S3UploadParts) CompleteUpload(timeout time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	s3client := p.opts.s3.Get()
	defer p.opts.s3.Put(s3client)

	var ctx context.Context
	var cancelTimeout context.CancelFunc
	if timeout > 0 {
		ctx, cancelTimeout = context.WithTimeout(context.Background(), timeout)
		defer cancelTimeout()
	} else {
		ctx = context.Background()
	}

	params, err := p.st.completeParts()
	if err != nil {
		p.st.completedError = err
	} else {
		if p.opts.Verbose {
			log.Printf("completing upload for multi-part object %s/%s using UploadId %s",
				*params.Bucket, *params.Key, *params.UploadId)
		}

		out, err := s3client.CompleteMultipartUpload(ctx, params)
		p.st.completedOutput = out
		p.st.completedError = err
		if err == nil {
			attr, err := getObjectAttributes(
				ctx, *params.Bucket, *params.Key, p.opts)
			p.st.objectAttributesOutput = attr
			p.st.objectAttributesError = err
		}
	}

	return p.st.completedError
}

// AbortUpload attempts to abort an upload of parts, if timeout is > 0 then the
// abort process will try to cancel the process if it takes longer than the
// specified timeout.
func (p *S3UploadParts) AbortUpload(timeout time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	s3client := p.opts.s3.Get()
	defer p.opts.s3.Put(s3client)

	var ctx context.Context
	var cancelTimeout context.CancelFunc
	if timeout > 0 {
		ctx, cancelTimeout = context.WithTimeout(context.Background(), timeout)
		defer cancelTimeout()
	} else {
		ctx = context.Background()
	}

	params := &s3.AbortMultipartUploadInput{
		Bucket:   p.st.create.Bucket,
		Key:      p.st.create.Key,
		UploadId: p.st.createOutput.UploadId,
	}

	if p.opts.Verbose {
		log.Printf("aborting upload multi-part object %s/%s using UploadId %s",
			*params.Bucket, *params.Key, *params.UploadId)
	}

	out, err := s3client.AbortMultipartUpload(ctx, params)

	p.st.abortedOutput = out
	p.st.abortedError = err

	return err
}

// queuedPar combines a submitted part for upload with an error channel to
// return any error outcome to the caller.  The channel will be size 1 to make
// polling the channel optional for the caller (since the results are also
// recorded in the S3UploadState)
type queuedPart struct {
	part *s3.UploadPartInput
	ch   chan error
}
