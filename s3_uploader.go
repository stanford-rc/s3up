package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var ErrTimeout error = errors.New("timeout")

// queueUpload represents an in-flight upload with a channel to return the
// results of processing
type queueUpload struct {
	ctx    context.Context
	r      io.Reader
	bucket string
	key    string
	res    chan *UploadResults
}

// UploadResults represents the final disposition of an upload
type UploadResults struct {
	Bucket string
	Key    string
	State  *S3UploadState
	Error  error
}

// Uploader accepts incoming queueUpload and uploads them as single or
// multi-part objects.
type Uploader struct {
	ctx       context.Context
	opts      *Options
	pending   *sync.WaitGroup
	queued    chan *queueUpload
	cancel    context.CancelFunc
	abortable map[*string]*S3UploadParts
	mu        *sync.Mutex
}

func NewUploader(ctx context.Context, opts *Options) *Uploader {
	ctx, cancel := context.WithCancel(ctx)

	p := &Uploader{
		ctx:       ctx,
		opts:      opts,
		pending:   &sync.WaitGroup{},
		queued:    make(chan *queueUpload),
		cancel:    cancel,
		abortable: map[*string]*S3UploadParts{},
		mu:        &sync.Mutex{},
	}

	for i := 0; i < opts.ConcurrentObjects; i++ {
		go func() {
			for {
				select {
				case q := <-p.queued:
					state, err := p.upload(q.ctx, q.r, q.bucket, q.key)
					q.res <- &UploadResults{
						Bucket: q.bucket,
						Key:    q.key,
						State:  state,
						Error:  err,
					}
				case <-p.ctx.Done():
					return
				}
			}
		}()
	}

	return p
}

func (p *Uploader) registerAbortable(s3multi *S3UploadParts) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.abortable[s3multi.UploadID()] = s3multi
}

func (p *Uploader) unregisterAbortable(s3multi *S3UploadParts) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.abortable, s3multi.UploadID())
}

// Pending returns a list of any multi-part object UploadID that are still
// in-flight or that encountered failures during the upload process
func (p *Uploader) Pending() []*string {
	p.mu.Lock()
	defer p.mu.Unlock()

	pending := []*string{}

	for pUploadID := range p.abortable {
		pending = append(pending, pUploadID)
	}

	return pending
}

// PendingTarget returns the target bucket / key path for pending upload.
func (p *Uploader) PendingTarget(uploadID *string) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if s3multi, ok := p.abortable[uploadID]; ok {
		pBucket := s3multi.Bucket()
		pKey := s3multi.Key()

		if pBucket != nil && pKey != nil {
			return path.Join(*pBucket, *pKey)
		}
	}

	return ""
}

// AbortPending attempts to abort any pending uploads.
func (p *Uploader) AbortPending(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	done := make(chan bool)
	go func() {
		for _, s3multi := range p.abortable {
			s3multi.AbortUpload(p.opts.AbortUploadTimeout)
			delete(p.abortable, s3multi.UploadID())
		}
		done <- true
	}()

	select {
	case <-done:
		// finished processing pending aborts
	case <-ctx.Done():
		// caller canceled pending aborts
	}
}

// Wait blocks until either all pending uploads have completed or the parent
// context was canceled.  After Wait returns the caller should check Pending to
// see if there were any uploads have failed to complete and that may need to
// be aborted.
func (p *Uploader) Wait(timeout time.Duration) error {
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

// Close cancels any pending Uploads that have not yet start processing, it
// does not cancel any in-flight uploads
func (p *Uploader) Close() {
	p.cancel()
}

// Upload processes queues an upload process, and returns a channel that may
// optionally be read to check the results.  If the context provided is
// canceled then the upload will be canceled.
func (p *Uploader) Upload(ctx context.Context, r io.Reader, Bucket, Key string) chan *UploadResults {
	p.pending.Add(1)

	q := &queueUpload{
		ctx:    ctx,
		r:      r,
		bucket: Bucket,
		key:    Key,
		res:    make(chan *UploadResults, 1),
	}

	select {
	case p.queued <- q:
		// submitted, it is now the reponsibility of p.upload
		// to call p.pending.Done()
	case <-p.ctx.Done():
		// failed to submit, call p.pending.Done() to clear the
		// pending state for this upload
		p.pending.Done()

		// gather any error available from the context and set
		// that in the results
		err := context.Cause(p.ctx)
		q.res <- &UploadResults{
			Bucket: Bucket,
			Key:    Key,
			State:  nil,
			Error:  err,
		}
	}

	return q.res
}

// upload processes an input io.Reader r, and uploads it to S3 using the
// specified Bucket and Key name.
//
// If the underlying io.Reader implements io.ReaderAt and io.Seeker then the
// ReadAt and Seek methods will be used to directly access the data for the
// upload.  Otherwise Options.PartSize chunks will be read serially and
// buffered either via temporary files or via memory buffers, depending on
// whether or not Options.UseMemoryBuffers was set to true.
//
// The Options.ConcurrentParts objects will control how many parts are uploaded
// in parallel per individual call to Upload.  To estimate the amount of extra
// free disk space or free memory required to process the io.Reader, the caller
// needs to multiply Options.ConcurrentObjects, Options.ConcurrentParts, and
// Options.PartSize together.
//
// If the io.Reader input size is equal to or less than Options.PartSize then
// S3 PutObject will be used to create the object, otherwise a multi-part
// object will be created.
func (p *Uploader) upload(ctx context.Context, r io.Reader, Bucket, Key string) (*S3UploadState, error) {
	defer p.pending.Done()

	var src Source
	var err error

	if p.opts.UseMemoryBuffers {
		src, err = MemorySource(r, p.opts.PartSize, p.opts.partBuf)
	} else {
		src, err = TempfileSource(r, p.opts.PartSize, p.opts.UseTempDir)
	}

	if err != nil {
		return nil, err
	}

	// S3HashWriter will track the hash signature of the parts and of the
	// whole body
	s3hw := NewS3HashWriter(p.opts.ChecksumAlgorithm, p.opts.PartSize)

	// s3multi will be initialized once we have a SourceReader derived from
	// the Source and know we want to upload a multi-part object instead of
	// using putObject
	var s3multi *S3UploadParts

	// AWS api wants pointers
	var pBucket *string = &Bucket
	var pKey *string = &Key
	var pUploadID *string
	var pPartID *int32

	// peeked may be set to store the read-ahead value of the next
	// SourceReader and/or error
	var peeked func() (*SourceReader, error)

	for {
		var sr *SourceReader
		var err error

		if peeked != nil {
			sr, err = peeked()
			peeked = nil
		} else {
			sr, err = src.Next()
		}

		if err != nil {
			// return any error that isn't an io.EOF
			if !errors.Is(err, io.EOF) {
				return nil, err
			}

			// if we hit an io.EOF before we initialized s3multi
			// then this was a zero length input
			if s3multi == nil {
				// register a zero length part in the S3Hasher
				s3hw.Write([]byte{})

				// call putObject with a zeroReadCloser
				zr := ZeroReadCloser()
				return putObject(ctx, zr, Bucket, Key, p.opts, s3hw.S3Hasher)
			}

			break
		}

		// copy SourceReader into the S3Hasher
		buf := copyBuf.Get(copyBufSize)
		defer copyBuf.Put(buf)
		if _, err := io.CopyBuffer(s3hw, sr, buf); err != nil {
			return nil, err
		}

		// rewind SourceReader so that we can upload it to S3
		if _, err = sr.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}

		// check for the special case of a single part upload, which we
		// will convert into a putObject request.
		if s3multi == nil {
			if size := s3hw.S3Hasher.PartSize(1); size < p.opts.PartSize {
				return putObject(
					ctx, sr, Bucket, Key, p.opts, s3hw.S3Hasher)
			} else {
				next_sr, next_err := src.Next()

				if next_sr == nil && errors.Is(next_err, io.EOF) {
					return putObject(
						ctx, sr, Bucket, Key, p.opts, s3hw.S3Hasher)
				}

				peeked = func() (*SourceReader, error) {
					return next_sr, next_err
				}
			}
		}

		if s3multi == nil {

			pMediaType := aws.String(MediaType(Key))

			algo := s3hw.S3Hasher.ChecksumAlgorithm()

			s3multi, err = NewS3UploadParts(
				ctx,
				s3hw.S3Hasher,
				&s3.CreateMultipartUploadInput{
					Bucket:            pBucket,
					Key:               pKey,
					ContentType:       pMediaType,
					ChecksumAlgorithm: algo.Type(),
				},
				p.opts)

			if err != nil {
				return nil, err
			}

			pUploadID = s3multi.UploadID()

			p.registerAbortable(s3multi)
		}

		partID, err := s3multi.NextPartID()
		if err != nil {
			return nil, err
		}

		pPartID = &partID

		part := &s3.UploadPartInput{
			Bucket:     pBucket,
			Key:        pKey,
			UploadId:   pUploadID,
			PartNumber: pPartID,
			Body:       sr,
		}

		s3hw.S3Hasher.SetUploadPartChecksums(*pPartID, part)

		errch := s3multi.UploadPart(part)
		go func(errch chan error, sr *SourceReader) {
			<-errch
			sr.Close()
		}(errch, sr)
	}

	err = s3multi.Wait(p.opts.UploadPartTimeout)
	if err != nil {
		return s3multi.st, err
	}

	if len(s3multi.st.Errors()) == 0 {
		s3multi.CompleteUpload(p.opts.CompleteUploadTimeout)
		if len(s3multi.st.Errors()) == 0 {
			p.unregisterAbortable(s3multi)
		}
	}

	return s3multi.st, errors.Join(s3multi.st.Errors()...)
}

// putObject uploads an io.ReadCloser as a stand-alone object
func putObject(ctx context.Context, rc io.ReadCloser, Bucket, Key string, opts *Options, hr *S3Hasher) (*S3UploadState, error) {
	defer rc.Close()

	// AWS api wants pointers
	pBucket := &Bucket
	pKey := &Key

	pMediaType := aws.String(MediaType(Key))

	obj := &s3.PutObjectInput{
		Bucket:      pBucket,
		Key:         pKey,
		Body:        rc,
		ContentType: pMediaType,
	}

	hr.SetPutObjectChecksums(obj)

	s3client := opts.s3.Get()
	defer opts.s3.Put(s3client)

	if opts.Verbose {
		log.Printf("started upload for object %s/%s", Bucket, Key)
	}

	out, err := s3client.PutObject(ctx, obj)

	p := &S3UploadState{
		hr:        hr,
		obj:       obj,
		objOutput: out,
		objError:  err,
	}

	if err == nil {
		attr, err := getObjectAttributes(ctx, Bucket, Key, opts)
		p.objectAttributesOutput = attr
		p.objectAttributesError = err
	}

	return p, err
}

// getObjectAttributes gets the current state of an object
func getObjectAttributes(ctx context.Context, Bucket, Key string, opts *Options) (*s3.GetObjectAttributesOutput, error) {
	s3client := opts.s3.Get()
	defer opts.s3.Put(s3client)

	if opts.Verbose {
		log.Printf("fetching attributes for object %s/%s", Bucket, Key)
	}

	// AWS api wants pointers
	pBucket := &Bucket
	pKey := &Key

	params := &s3.GetObjectAttributesInput{
		Bucket:   pBucket,
		Key:      pKey,
		MaxParts: aws.Int32(DefaultMaxPartID),
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesEtag,
			types.ObjectAttributesChecksum,
			types.ObjectAttributesObjectParts,
			types.ObjectAttributesObjectSize,
		},
	}

	return s3client.GetObjectAttributes(ctx, params)
}

// zeroReadCloser implements io.ReadCloser and io.Seeker for the special edge
// case of a zero length input
type zeroReadCloser struct {
	*bytes.Reader
}

func (p *zeroReadCloser) Close() error {
	return nil
}

func ZeroReadCloser() *zeroReadCloser {
	return &zeroReadCloser{Reader: bytes.NewReader([]byte{})}
}
