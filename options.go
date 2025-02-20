package main

import (
	"time"
)

// Minimum allowed size of a part in bytes
const MinPartSize int64 = 5 * 1024 * 1024

// Maximum allowed size of a part in bytes
const MaxPartSize int64 = 5 * 1024 * 1024 * 1024

// Default part size in bytes
const DefaultPartSize int64 = MaxPartSize

// Default buffer size for copyBuf in bytes
const DefaultCopyBufSize int64 = 256 * 1024

// Default limit on the number of parts in a multi-part upload
const DefaultMaxPartID int32 = 1e4

// Options captures command line flags to configure the upload process
type Options struct {
	// Optionally specify cpu profiling output file
	CpuProfile string

	// Optionally specify memory profiling output file
	MemProfile string

	// Optionally specify trace output file
	Trace string

	// Optionally enable verbose logging
	Verbose bool

	// If keys contain bytes that are not UTF8, escape them using
	// percent-encoding
	EncodeKey bool

	// Optionally specify a tab-separated file listing filepath extensions
	// and IANA media types to register in the process
	MediaTypes string

	// Optionally specify that memory buffers should be used instead of
	// file buffers when uploading a stream
	UseMemoryBuffers bool

	// Optionally set the temp directory to use when file buffers are in use
	UseTempDir string

	// Optionally specify the maximum time to wait for an s3 UploadPart
	// call to complete, if set to the zero value then no timeout will be
	// triggered
	UploadPartTimeout time.Duration

	// Optionally specify the maximum time to wait for an s3 CompleteUpload
	// call to complete, if set to the zero value then no timeout will be
	// triggered
	CompleteUploadTimeout time.Duration

	// Optionally specifieis the maximum time to wait for an s3 AbortUpload
	// call to complete, if set to the zero value then no timeout will be
	// triggered
	AbortUploadTimeout time.Duration

	// Optionally specify that subdirectories should be walked to find
	// files to upload.
	Recursive bool

	// Optionally specify a profile name to use from the AWS configuration
	// files
	Profile string

	// Optionally specify that newer virtual-host style paths should be
	// used (AWS S3 uses virtual-host style paths, Elm uses the older path
	// style).
	DisablePathStyle bool

	// Optionally specify that only a single s3 Client shoudl be used (with
	// AWS S3 the S3 SDK used by s3up is able to open multiple connections,
	// with Elm that does not appear to be the case and so using multiple
	// s3 Client is the default)
	DisableS3ClientPool bool

	// Optionally select the checksum algorithm to validate each part
	// uploaded, by default SHA256 is used.
	ChecksumAlgorithm *ChecksumAlgorithm

	// Optionally override the default buffer size (in bytes) to use when
	// copying source parts to temporary files, by default this will be
	// 256KiB.
	CopySize int64

	// Optionally override the size (in bytes) to use for individual parts
	// of a multi-part upload.  The minimum allowed part size is 5MiB and
	// the maximum is 5GiB.
	PartSize int64

	// Optionally specify the maximum number of parts allowed to be
	// created, by default this will be DefaultMaxPartID
	MaxPartID int32

	// Optionally specify the number of goroutines used to process uploaded
	// objects, the default is 1.
	ConcurrentObjects int

	// Optionally specify thne number of goroutines to use per part for a
	// multi-part object upload.  T The pool of goroutines is not shared
	// between calls to Upload.  The default value is 1.
	ConcurrentParts int

	// Optionally direct s3up to not abort any failed uploads or any
	// uploads still pending when an interrupt signal is received.
	LeavePartsOnError bool

	// Optionally specify a manifest format to produce detailing checksums,
	// paths, etc. that were uploaded.
	Manifest manifestType

	// Required S3 Bucket identifier
	bucket string

	// S3 Key name or prefix.  This is optional when processing filepath
	// globs, when uploading from the standard input stream a key name is
	// required.
	key string

	// Optional filepath globs to upload, these will be processed by
	// processGlobs
	globs []string

	// s3 manages whether or not a single s3.Client is shared across all
	// goroutines
	s3 *S3ClientPool

	// partBuf manages the in-memory PartSize buffer pool, if one was set
	// up per the UseMemoryBuffers options
	partBuf BufferPool
}
