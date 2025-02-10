package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ObjectReporting representins a JSON serializable representation of an
// S3UploadState record.
type ObjectReporting struct {
	Bucket           string
	Key              string
	UploadId         string `json:",omitempty"`
	Completed        bool
	Aborted          bool
	FullChecksums    *ObjectChecksums  `json:",omitempty"`
	ObjectChecksum   *ObjectChecksums  `json:",omitempty"`
	ObjectAttributes *ObjectAttributes `json:",omitempty"`
	Errors           *ObjectErrors     `json:",omitempty"`
}

func NewObjectReporting(st *S3UploadState) (*ObjectReporting, error) {

	isPutObject := (st.obj != nil && st.objOutput != nil)

	isMultipartObject := (st.create != nil && st.createOutput != nil)

	isCompleted := ((isPutObject && st.objError == nil) ||
		(st.create != nil && st.createOutput != nil && st.completedOutput != nil && st.completedError == nil))

	isAborted := ((isPutObject && st.objError != nil) ||
		(st.create != nil && st.createOutput != nil && st.abortedOutput != nil && st.abortedError == nil))

	var Bucket string
	var Key string
	var uploadID string

	if isPutObject {
		Bucket = *st.obj.Bucket
		Key = *st.obj.Key
	} else if isMultipartObject {
		Bucket = *st.create.Bucket
		Key = *st.create.Key

		if !(isCompleted || isAborted) {
			uploadID = *st.createOutput.UploadId
		}
	} else {
		err := fmt.Errorf("invalid S3UploadState: it is not a PutObject nor a MultipartObject")
		return nil, err
	}

	var fullChecksums *ObjectChecksums
	var objChecksums *ObjectChecksums
	var objAttributes *ObjectAttributes
	var err error
	if isCompleted {
		fullChecksums, err = NewObjectChecksums(st.hr)
		if err != nil {
			return nil, err
		}

		if st.hr.Count() == 1 {
			objChecksums = AWSObjectChecksums(
				st.hr.ChecksumAlgorithm(), st.hr.Sum())
		} else {
			objChecksums = AWSObjectChecksums(
				st.hr.ChecksumAlgorithm(), st.hr.SumOfSums())
		}

		if st.objectAttributesError != nil {
			return nil, st.objectAttributesError
		}

		objAttributes, err = NewObjectAttributes(st.hr, st.objectAttributesOutput)
		if err != nil {
			return nil, err
		}

	}

	var partErrors []*UploadPartError
	if isMultipartObject {
		for i, e := range st.uploadPartErrors {
			if e == nil {
				continue
			}

			partErrors = append(partErrors, &UploadPartError{
				PartNumber: i,
				Error:      errorString(e),
			})
		}
	}

	errors := &ObjectErrors{
		PutObjectError:               errorString(st.objError),
		UploadPartErrors:             partErrors,
		CompleteMultipartUploadError: errorString(st.completedError),
		AbortMultipartUploadError:    errorString(st.abortedError),
		GetObjectAttributesError:     errorString(st.objectAttributesError),
	}

	if len(errors.PutObjectError) == 0 &&
		len(errors.UploadPartErrors) == 0 &&
		len(errors.CompleteMultipartUploadError) == 0 &&
		len(errors.AbortMultipartUploadError) == 0 &&
		len(errors.GetObjectAttributesError) == 0 {
		errors = nil
	}

	return &ObjectReporting{
		Bucket:           Bucket,
		Key:              Key,
		UploadId:         uploadID,
		Completed:        isCompleted,
		Aborted:          isAborted,
		FullChecksums:    fullChecksums,
		ObjectChecksum:   objChecksums,
		ObjectAttributes: objAttributes,
		Errors:           errors,
	}, nil
}

// ObjectChecksum provides human-readable representations of a HashSum checksum.
type ObjectChecksum struct {
	Hex    string
	Base64 string
}

func NewObjectChecksum(sum HashSum) *ObjectChecksum {
	p := &ObjectChecksum{
		Hex:    sum.Hex(),
		Base64: sum.Base64(),
	}

	return p
}

// ObjectChecksums represents one or more nested ObjectChecksum.
type ObjectChecksums struct {
	ChecksumMD5    *ObjectChecksum `json:"ChecksumMD5,omitempty"`
	ChecksumCRC32  *ObjectChecksum `json:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C *ObjectChecksum `json:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1   *ObjectChecksum `json:"ChecksumSHA1,omitempty"`
	ChecksumSHA256 *ObjectChecksum `json:"ChecksumSHA256,omitempty"`
}

// AWSObjectChecksums returns an ObjectChecksums for a specified algorithm and
// HashSum checksum.
func AWSObjectChecksums(algo *ChecksumAlgorithm, sum HashSum) *ObjectChecksums {
	p := &ObjectChecksums{}

	switch algo {
	case ChecksumAlgorithmCRC32:
		p.ChecksumCRC32 = NewObjectChecksum(sum)
	case ChecksumAlgorithmCRC32C:
		p.ChecksumCRC32C = NewObjectChecksum(sum)
	case ChecksumAlgorithmSHA1:
		p.ChecksumSHA1 = NewObjectChecksum(sum)
	case ChecksumAlgorithmSHA256:
		p.ChecksumSHA256 = NewObjectChecksum(sum)
	}

	return p
}

// NewObjectChecksums accepts either *S3Hasher or *types.Checksum and converts
// it into an *ObjectChecksum.  If passed nil then nil will be returned.
func NewObjectChecksums(t interface{}) (*ObjectChecksums, error) {
	var md5sum []byte
	var sum HashSum
	var algo *ChecksumAlgorithm
	var err error

	if t == nil {
		return nil, nil
	} else if hr, ok := t.(*S3Hasher); ok {
		algo = hr.ChecksumAlgorithm()
		sum = hr.Sum()
		md5sum = hr.MD5Sum()
	} else if x, ok := t.(*types.Checksum); ok {
		var b64 HashSumBase64
		p := &b64

		if x.ChecksumCRC32 != nil {
			algo = ChecksumAlgorithmCRC32
			err = p.UnmarshalText([]byte(*x.ChecksumCRC32))
		} else if x.ChecksumCRC32C != nil {
			algo = ChecksumAlgorithmCRC32C
			err = p.UnmarshalText([]byte(*x.ChecksumCRC32C))
		} else if x.ChecksumSHA1 != nil {
			algo = ChecksumAlgorithmSHA1
			err = p.UnmarshalText([]byte(*x.ChecksumSHA1))
		} else if x.ChecksumSHA256 != nil {
			algo = ChecksumAlgorithmSHA256
			err = p.UnmarshalText([]byte(*x.ChecksumSHA256))
		} else {
			err = fmt.Errorf("unknown types.Checksum: %#v", x)
		}

		if err != nil {
			err = fmt.Errorf("error decoding %#v: %w", x, err)
			return nil, err
		} else {
			sum = p.HashSum
		}
	} else {
		err := fmt.Errorf("unhandled type passed to NewObjectChecksums: %t", t)
		return nil, err
	}

	p := &ObjectChecksums{}
	if md5sum != nil {
		p.ChecksumMD5 = NewObjectChecksum(md5sum)
	}

	switch algo {
	case ChecksumAlgorithmCRC32:
		p.ChecksumCRC32 = NewObjectChecksum(sum)
	case ChecksumAlgorithmCRC32C:
		p.ChecksumCRC32C = NewObjectChecksum(sum)
	case ChecksumAlgorithmSHA1:
		p.ChecksumSHA1 = NewObjectChecksum(sum)
	case ChecksumAlgorithmSHA256:
		p.ChecksumSHA256 = NewObjectChecksum(sum)
	}

	return p, nil
}

// ObjectAttributes represents the available fields in
// s3.GetObjectAttributesOutput.
type ObjectAttributes struct {
	DeleteMarker *bool                 `json:",omitempty"`
	VersionId    *string               `json:",omitempty"`
	LastModified *time.Time            `json:",omitempty"`
	ETag         *string               `json:",omitempty"`
	Checksum     *ObjectChecksums      `json:",omitempty"`
	ObjectParts  *ObjectPartAttributes `json:",omitempty"`
}

func NewObjectAttributes(hr *S3Hasher, p *s3.GetObjectAttributesOutput) (*ObjectAttributes, error) {
	if p == nil {
		return nil, fmt.Errorf("nil GetObjectAttributesOutput")
	}

	checksum, err := NewObjectChecksums(p.Checksum)
	if err != nil {
		return nil, err
	}

	return &ObjectAttributes{
		DeleteMarker: p.DeleteMarker,
		VersionId:    p.VersionId,
		LastModified: p.LastModified,
		ETag:         p.ETag,
		Checksum:     checksum,
		ObjectParts:  NewObjectPartAttributes(hr, p.ObjectParts),
	}, nil
}

// ObjectPartAttributes represents the available fields in
// types.GetObjectAttributeParts.
type ObjectPartAttributes struct {
	IsTruncated     *bool         `json:",omitempty"`
	TotalPartsCount *int32        `json:",omitempty"`
	Parts           []*ObjectPart `json:",omitempty"`
}

func NewObjectPartAttributes(hr *S3Hasher, p *types.GetObjectAttributesParts) *ObjectPartAttributes {
	if p == nil {
		return nil
	}

	return &ObjectPartAttributes{
		IsTruncated:     p.IsTruncated,
		TotalPartsCount: p.TotalPartsCount,
		Parts:           NewObjectParts(hr, p.Parts),
	}
}

// NewObjectParts reprsents one or more types.ObjectPart, if the parts slice is
// empty or nil then nil is returned.
func NewObjectParts(hr *S3Hasher, parts []types.ObjectPart) []*ObjectPart {
	if len(parts) == 0 {
		return nil
	}

	var op []*ObjectPart

	checksumObject := func(s *string) *ObjectChecksum {
		var sum HashSumBase64

		if s == nil {
			return nil
		}

		err := (&sum).UnmarshalText([]byte(*s))
		if err != nil {
			panic(fmt.Sprintf("invalid base64 checksum returned by AWS! %s: %s", *s, err))
		}

		return NewObjectChecksum(sum.HashSum)
	}

	for _, p := range parts {
		if p.PartNumber == nil {
			continue
		}

		md5sum := hr.MD5SumPart(*p.PartNumber)

		op = append(op, &ObjectPart{
			PartNumber:     p.PartNumber,
			Size:           p.Size,
			ChecksumCRC32:  checksumObject(p.ChecksumCRC32),
			ChecksumCRC32C: checksumObject(p.ChecksumCRC32C),
			ChecksumSHA1:   checksumObject(p.ChecksumSHA1),
			ChecksumSHA256: checksumObject(p.ChecksumSHA256),
			ChecksumMD5:    NewObjectChecksum(md5sum),
		})
	}

	return op
}

type ObjectPart struct {
	PartNumber     *int32
	Size           *int64
	ChecksumCRC32  *ObjectChecksum `json:",omitempty"`
	ChecksumCRC32C *ObjectChecksum `json:",omitempty"`
	ChecksumSHA1   *ObjectChecksum `json:",omitempty"`
	ChecksumSHA256 *ObjectChecksum `json:",omitempty"`
	ChecksumMD5    *ObjectChecksum `json:",omitempty"`
}

// UploadPartError represents an error recorded in an
// S3UploadState.uploadPartsError entry.
type UploadPartError struct {
	PartNumber int32
	Error      string
}

// ObjectErrors captures any errors recorded in an S3UploadState
type ObjectErrors struct {
	PutObjectError               string             `json:",omitempty"`
	UploadPartErrors             []*UploadPartError `json:",omitempty"`
	CompleteMultipartUploadError string             `json:",omitempty"`
	AbortMultipartUploadError    string             `json:",omitempty"`
	GetObjectAttributesError     string             `json:",omitempty"`
}

func NewObjectErrors(st *S3UploadState) *ObjectErrors {
	var uploadPartErrors []*UploadPartError
	for i, err := range st.uploadPartErrors {
		if err == nil {
			continue
		}
		uploadPartErrors = append(uploadPartErrors, &UploadPartError{
			PartNumber: int32(i + 1),
			Error:      errorString(err),
		})
	}

	return &ObjectErrors{
		PutObjectError:               errorString(st.objError),
		UploadPartErrors:             uploadPartErrors,
		CompleteMultipartUploadError: errorString(st.completedError),
		AbortMultipartUploadError:    errorString(st.abortedError),
		GetObjectAttributesError:     errorString(st.objectAttributesError),
	}
}

// errorString returns the string form of an error
func errorString(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
