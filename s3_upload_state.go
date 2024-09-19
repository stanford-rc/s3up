package main

import (
	"cmp"
	"fmt"
	"slices"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3UploadState tracks the state of an attempt to create an object or a
// multi-part object
type S3UploadState struct {
	hr *S3Hasher

	obj       *s3.PutObjectInput
	objOutput *s3.PutObjectOutput
	objError  error

	create       *s3.CreateMultipartUploadInput
	createOutput *s3.CreateMultipartUploadOutput

	uploadPartOutputs map[int32]*s3.UploadPartOutput
	uploadPartErrors  map[int32]error

	completedOutput *s3.CompleteMultipartUploadOutput
	completedError  error

	abortedOutput *s3.AbortMultipartUploadOutput
	abortedError  error

	objectAttributesOutput *s3.GetObjectAttributesOutput
	objectAttributesError  error

	mu *sync.Mutex
}

func (p *S3UploadState) Errors() []error {
	var err []error

	if p.objError != nil {
		err = append(err, fmt.Errorf(
			"put-object error: %w", p.objError))
	}

	for k, v := range p.uploadPartErrors {
		if v != nil {
			err = append(err, fmt.Errorf(
				"upload part %d error: %w", k, v))
		}
	}

	if p.completedError != nil {
		err = append(err, fmt.Errorf(
			"complete multi-part upload error: %w", p.completedError))
	}

	if p.abortedError != nil {
		err = append(err, fmt.Errorf(
			"abort multi-part upload error: %w", p.abortedError))
	}

	return err
}

// setPartResults records the results of processing an S3UploadParts.UploadPart
// request.  It may be called either as part of processing the upload and
// recording the results from the s3.Client or recording any errors encountered
// before the the part could be passed off to the s3.Client (e.g., if the
// context was canceled)
func (p *S3UploadState) setPartResults(partID *int32, out *s3.UploadPartOutput, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.uploadPartOutputs[*partID] = out
	p.uploadPartErrors[*partID] = err
}

// completeParts returns a *s3.CompleteMultipartUploadInput for the parts
// completed to this point.  If there is a gap in the sequence of part numbers
// an error is returned.
func (p *S3UploadState) completeParts() (*s3.CompleteMultipartUploadInput, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var completedParts []types.CompletedPart

	for partID, out := range p.uploadPartOutputs {
		completedPart := types.CompletedPart{
			ETag:       out.ETag,
			PartNumber: &partID,
		}

		checksumBase64 := aws.String(
			HashSum(p.hr.SumPart(partID)).Base64())

		switch p.hr.ChecksumAlgorithm() {
		case ChecksumAlgorithmCRC32:
			completedPart.ChecksumCRC32 = checksumBase64
		case ChecksumAlgorithmCRC32C:
			completedPart.ChecksumCRC32C = checksumBase64
		case ChecksumAlgorithmSHA1:
			completedPart.ChecksumSHA1 = checksumBase64
		case ChecksumAlgorithmSHA256:
			completedPart.ChecksumSHA256 = checksumBase64
		}

		completedParts = append(completedParts, completedPart)
	}

	slices.SortFunc(completedParts, func(a, b types.CompletedPart) int {
		return cmp.Compare(*a.PartNumber, *b.PartNumber)
	})

	for i := 1; i < len(completedParts); i++ {
		partID := *completedParts[i].PartNumber

		if partID != int32(i+1) {
			var err error

			if i == 0 {
				err = fmt.Errorf(
					"out-of-order partID: started at %d (expected %d)",
					partID, (i + 1))
			} else {
				err = fmt.Errorf(
					"out-of-order partID: %d -> %d (expected %d)",
					i, partID, (i + 1))
			}

			return nil, err
		}
	}

	return &s3.CompleteMultipartUploadInput{
		Bucket:   p.create.Bucket,
		Key:      p.create.Key,
		UploadId: p.createOutput.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}, nil
}
