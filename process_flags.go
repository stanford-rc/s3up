package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var errMissingBucket = errors.New(
	"missing required -bucket flag")

var errBadChecksum = errors.New(
	"-checksum must be one of SHA256, SHA1, CRC32C, or CRC32")

var errBadPartSize = errors.New(
	"-part-size must be >= 5MiB and <= 5GiB")

// processFlags processes the os.Argv[1:] command line options, parsing flags
// and trailing arguments.
func processFlags(ctx context.Context, args []string) (*Options, error) {
	var err error

	opts := &Options{}

	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	flags.StringVar(&opts.CpuProfile, "cpu-profile", "",
		"optionally specify a cpu profile output path")
	flags.StringVar(&opts.MemProfile, "mem-profile", "",
		"optionally specify a memory profile output path")
	flags.StringVar(&opts.Trace, "trace", "",
		"optionally specify a trace output file path")

	flags.BoolVar(&opts.Verbose, "verbose", false,
		"optionally enable verbose logging to standard error")

	flags.BoolVar(&opts.EncodeKey, "encode-key", false,
		"optionally percent-encode bytes in Key that are not valid UTF-8 non-control characters")

	flags.StringVar(&opts.MediaTypes, "media-types", "",
		"optionally specify a path to a TSV listing extension to media-type mappings")

	flags.BoolVar(&opts.UseMemoryBuffers, "use-memory", false,
		"optionally specify that memory buffers should be used instead of temporary files")
	flags.StringVar(&opts.UseTempDir, "use-temp-dir", "",
		"optionally specify a directory to use when creating temporary files")

	flags.DurationVar(&opts.UploadPartTimeout, "upload-part-timeout", time.Duration(0),
		"optionally set a timeout for any UploadPart requests")
	flags.DurationVar(&opts.CompleteUploadTimeout, "complete-multipart-timeout", time.Duration(0),
		"optionally set a timeout for any CompleteMultipartUpload requests")
	flags.DurationVar(&opts.AbortUploadTimeout, "abort-multipart-timeout", time.Duration(0),
		"optionally set a timeout for any AbortMultipartUpload requests")

	flags.StringVar(&opts.Profile, "profile", "",
		"optional AWS profile name to use")

	flags.BoolVar(&opts.Recursive, "recursive", false,
		"recursively process directories for files to upload")

	flags.BoolVar(&opts.DisablePathStyle, "disable-path-style", false,
		"disable use of older AWS S3 path-style requests")

	flags.BoolVar(&opts.DisableS3ClientPool, "disable-s3-pool", false,
		"disable use multiple s3 clients")

	var checksumAlgo string
	flags.StringVar(&checksumAlgo, "checksum", "SHA256",
		"checksum algorithm to use, one of SHA256, SHA1, CRC32, or CRC32C")

	var copySize ByteSize
	flags.Var(&copySize, "copy-buf",
		"I/O buffer size for copy operations (default: 128KiB)")

	var partSize ByteSize
	flags.Var(&partSize, "part-size",
		"Size of parts to upload (min: 5MiB, max: 5GiB, default: 5GiB)")

	var maxPartID MaxPartID
	flags.Var(&maxPartID, "max-part-id", fmt.Sprintf(
		"Maximum number of parts to upload in a multi-part object (default: %d)",
		DefaultMaxPartID))

	flags.IntVar(&opts.ConcurrentObjects, "concurrent-objects", 1,
		"number of concurrent objects to upload")
	flags.IntVar(&opts.ConcurrentParts, "concurrent-parts", 1,
		"number of concurrent parts to upload per object")
	flags.BoolVar(&opts.LeavePartsOnError, "leave-parts-on-error", false,
		"do not abort failed uploads, leaving parts for manual recovery")

	var manifest ManifestType
	flags.Var(&manifest, "manifest",
		"Optionally specify a manifest: json, md5, checksum, aws, etag")

	flags.StringVar(&opts.bucket, "bucket", "",
		"name of the bucket to upload objects to")

	flags.StringVar(&opts.key, "key", "",
		"optional name of the object, or a prefix ending in '/' when uploading multiple files")

	var help bool
	flags.BoolVar(&help, "h", false, "print help and exit")
	flags.BoolVar(&help, "help", false, "print help and exit")

	flags.Parse(args)

	if help {
		fmt.Print(godoc_cmd_pkg)
		os.Exit(0)
	}

	// bucket
	if opts.bucket == "" {
		return nil, errMissingBucket
	}

	// ChecksumAlgorithm
	switch strings.ToUpper(checksumAlgo) {
	case "SHA256":
		opts.ChecksumAlgorithm = ChecksumAlgorithmSHA256
	case "SHA1":
		opts.ChecksumAlgorithm = ChecksumAlgorithmSHA1
	case "CRC32C":
		opts.ChecksumAlgorithm = ChecksumAlgorithmCRC32C
	case "CRC32":
		opts.ChecksumAlgorithm = ChecksumAlgorithmCRC32
	default:
		err = fmt.Errorf("%w: %s", errBadChecksum, checksumAlgo)
		return nil, err
	}

	// ConcurrentObjects
	if opts.ConcurrentObjects < 0 {
		opts.ConcurrentObjects = 1
	}

	// ConcurrentParts
	if opts.ConcurrentParts < 0 {
		opts.ConcurrentParts = 1
	}

	// CopySize
	if i64 := int64(copySize); i64 <= 0 {
		opts.CopySize = DefaultCopyBufSize
	} else {
		opts.CopySize = i64
	}

	// PartSize
	if i64 := int64(partSize); i64 < MinPartSize || i64 > MaxPartSize {
		if i64 == 0 {
			opts.PartSize = DefaultPartSize
		} else {
			err = fmt.Errorf("%w: %s", errBadPartSize, partSize)
			return nil, err
		}
	} else {
		opts.PartSize = i64
	}

	// MaxPartID
	opts.MaxPartID = int32(maxPartID)
	if opts.MaxPartID <= 0 {
		opts.MaxPartID = DefaultMaxPartID
	}

	// Manifest
	opts.Manifest = manifestType(manifest)

	// s3
	awsCfg, err := config.LoadDefaultConfig(
		ctx, config.WithSharedConfigProfile(opts.Profile))
	if err != nil {
		return nil, err
	}

	opts.s3 = NewS3ClientPool(
		!opts.DisableS3ClientPool,
		awsCfg,
		func(o *s3.Options) {
			o.UsePathStyle = !opts.DisablePathStyle
		},
	)

	// Buffer for io.CopyBuffer
	if opts.CopySize != copyBufSize {
		copyBufSize = opts.CopySize
		copyBuf = NewBufferPool(opts.CopySize)
	}

	// Buffer for streaming parts
	if opts.UseMemoryBuffers {
		opts.partBuf = NewBufferPool(opts.PartSize)
	}

	// optional globs (files / directories to upload)
	opts.globs = flags.Args()

	return opts, nil
}
