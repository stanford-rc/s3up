package main

import (
	"fmt"
	"hash"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Hasher can be used to compute the various per-part and full-body HashSum
// for objects uploaded to S3.
type S3Hasher struct {
	algo *ChecksumAlgorithm
	size int64

	full_algo  hash.Hash
	algo_parts *HashParts

	full_md5  hash.Hash
	md5_parts *HashParts
}

// NewS3Hasher initializes a new S3Hasher using the specified algorithm and
// maximum part size.
func NewS3Hasher(algo *ChecksumAlgorithm, partSize int64) *S3Hasher {
	return &S3Hasher{
		algo:       algo,
		size:       partSize,
		full_algo:  NewHasher(algo)(),
		algo_parts: NewHashParts(algo, partSize),
		full_md5:   NewHasher(ChecksumAlgorithmMD5)(),
		md5_parts:  NewHashParts(ChecksumAlgorithmMD5, partSize),
	}
}

// write adds b to the hash signatures for the S3Hasher
func (hr *S3Hasher) write(b []byte) (int, error) {
	hr.full_algo.Write(b)
	hr.algo_parts.Write(b)
	hr.full_md5.Write(b)
	hr.md5_parts.Write(b)
	return len(b), nil
}

// SetPutObjectChecksums sets the ContentMD5 and Checksum<algo> fields on an
// s3.PutObjectInput using the full body checksums
func (hr *S3Hasher) SetPutObjectChecksums(obj *s3.PutObjectInput) {
	md5Sum := hr.MD5Sum().Base64()
	obj.ContentMD5 = &md5Sum

	algoSum := hr.Sum().Base64()
	switch hr.ChecksumAlgorithm() {
	case ChecksumAlgorithmSHA256:
		obj.ChecksumSHA256 = &algoSum
	case ChecksumAlgorithmSHA1:
		obj.ChecksumSHA1 = &algoSum
	case ChecksumAlgorithmCRC32C:
		obj.ChecksumCRC32C = &algoSum
	case ChecksumAlgorithmCRC32:
		obj.ChecksumCRC32 = &algoSum
	}
}

// SetUploadPartChecksum sets the ContentMD5 and Checksum<algo> fields on an
// s3.UploadPartInput using the checksums for the specified partID.
func (hr *S3Hasher) SetUploadPartChecksums(partID int32, part *s3.UploadPartInput) {
	md5Sum := hr.MD5SumPart(partID).Base64()
	part.ContentMD5 = &md5Sum

	algoSum := hr.SumPart(partID).Base64()
	switch hr.ChecksumAlgorithm() {
	case ChecksumAlgorithmSHA256:
		part.ChecksumSHA256 = &algoSum
	case ChecksumAlgorithmSHA1:
		part.ChecksumSHA1 = &algoSum
	case ChecksumAlgorithmCRC32C:
		part.ChecksumCRC32C = &algoSum
	case ChecksumAlgorithmCRC32:
		part.ChecksumCRC32 = &algoSum
	}
}

// SetCompletedPartChecksum sets the Checksum<algo> fields on an
// s3.CompletedPart using the checksum for the specified partID.
func (hr *S3Hasher) SetCompletedPartChecksum(partID int32, completed *types.CompletedPart) {
	algoSum := hr.SumPart(partID).Base64()
	switch hr.ChecksumAlgorithm() {
	case ChecksumAlgorithmSHA256:
		completed.ChecksumSHA256 = &algoSum
	case ChecksumAlgorithmSHA1:
		completed.ChecksumSHA1 = &algoSum
	case ChecksumAlgorithmCRC32C:
		completed.ChecksumCRC32C = &algoSum
	case ChecksumAlgorithmCRC32:
		completed.ChecksumCRC32 = &algoSum
	}
}

// ChecksumAlgorithm returns the identifier for the checksum algorithm used by
// this S3Hasher.
func (hr *S3Hasher) ChecksumAlgorithm() *ChecksumAlgorithm {
	return hr.algo_parts.ChecksumAlgorithm()
}

// Count returns the number of parts read by this S3Hasher.  Note that partID
// values start at 1 and end at the value returned by this method.
func (hr *S3Hasher) Count() int {
	return hr.algo_parts.Count()
}

// Sum returns the full-body HashSum using the configured checksum algorithm
func (hr *S3Hasher) Sum() HashSum {
	return hr.full_algo.Sum(nil)
}

// Size returns the number of bytes used for across all parts
func (hr *S3Hasher) Size() int64 {
	var size int64
	for i := 0; i < hr.Count(); i++ {
		size += hr.algo_parts.PartSize(int32(i + 1))
	}
	return size
}

// PartSize returns the number of bytes used for part partID.  Valid values for
// partID are 1 >= partID <= S3Hasher.Count()
func (hr *S3Hasher) PartSize(partID int32) int64 {
	return hr.algo_parts.PartSize(partID)
}

// Sum returns the HashSum for the specified partID using the configured
// checksum algorithm.  Valid values for partID are 1 >= partID <=
// S3Hasher.Count()
func (hr *S3Hasher) SumPart(partID int32) HashSum {
	return hr.algo_parts.Sum(partID)
}

// SumOfSums returns the hash-of-hashes HashSum for the current parts using the
// configured checksum algorithm
func (hr *S3Hasher) SumOfSums() HashSum {
	return hr.algo_parts.SumOfSums()
}

// MD5Sum returns the full-body HashSum checksum using MD5
func (hr *S3Hasher) MD5Sum() HashSum {
	return hr.full_md5.Sum(nil)
}

// MD5SumPart returns the HashSum for partID using MD5.  Valid values for
// partID are 1 >= partID <= S3Hasher.Count()
func (hr *S3Hasher) MD5SumPart(partID int32) HashSum {
	return hr.md5_parts.Sum(partID)
}

// ETag returns the hex md5 hash-of-hashes plus part count used to generate an
// ETag header value in minio
func (hr *S3Hasher) ETag() string {
	return fmt.Sprintf("%s-%d",
		hr.md5_parts.SumOfSums().Hex(),
		hr.md5_parts.Count())
}

// S3HashWriter can be used to compute the various per-part and full-body
// hashes for the bytes written to it.
type S3HashWriter struct {
	*S3Hasher
}

// NewS3HashWriter initializes a new S3HashWriter which populates an S3Hasher
// with the data written to it via Write.
func NewS3HashWriter(algo *ChecksumAlgorithm, partSize int64) *S3HashWriter {
	return &S3HashWriter{
		S3Hasher: NewS3Hasher(algo, partSize),
	}
}

func (w *S3HashWriter) Write(b []byte) (int, error) {
	return w.write(b)
}

// S3HashReader can be used to read from an underlying io.Reader and to compute
// the various per-part and full-body hashes for the bytes read
type S3HashReader struct {
	*S3Hasher
	r io.Reader
}

// NewS3HashReader initializes a new S3HashReader which reads from r and
// populates an S3Hasher with the data read out of it via Read.
func NewS3HashReader(r io.Reader, algo *ChecksumAlgorithm, partSize int64) *S3HashReader {
	return &S3HashReader{
		S3Hasher: NewS3Hasher(algo, partSize),
		r:        r,
	}
}

// Read fills b from its underlying io.Reader and adds them to its underlying
// S3Hasher before returning the bytes read
func (r *S3HashReader) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	if n > 0 {
		r.write(b[0:n])
	}
	return n, err
}
