NAME

    s3up - s3 uploader proof-of-concept

SYNOPSIS

    s3up [ <options> ] [ <globs> ]

DESCRIPTION

    s3up is a proof-of-concept for uploading files to s3, taking advantage
    of the features detailed on

    	Checking object integrity
    	https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html

    while also calculating full-body checksums to ease validation using
    local tools such as md5 or sha256sum.

    s3up takes one or more options, detailed in the OPTIONS section below.
    The only required options is -bucket.

    The -key option may be left unspecified, in which case the filepath
    name will be used, set to a prefix (ending in a slash ("/")) in which
    case the prefix will be prepended to filepath names, or set to
    non-prefix value in which case the filepath name will be replaced with
    the specified -key name.

    As its final arguments s3up takes one or more file glob patterns for
    files to upload.  A glob can be a full filename or valid glob pattern,
    e.g., '*.pdf', to match against a list of files.  Alternatively if no
    globs are provided then s3up will read from standard input, in which
    case a non-prefix -key name is required.

OPTIONS

    -h | -help | --help

    	Print out help and exit

    -bucket string

    	Required name of the bucket to upload objects to.

    -key string

    	If <globs> are specified then optionally set the name of the
    	object, or a prefix ending in '/' when uploading multiple
    	files.  If no <globs> are specified then a non-prefix -key is
    	required.

    -part-size value

    	Optionally specify the size of parts to upload.

    	(minimum: 5MiB, maximum: 5GiB, default: 5GiB)

    -recursive

    	Optionally recursively process directories listed in <globs>
    	for files to upload.

    -profile string

    	Optionally specify the AWS profile name to use.

    -concurrent-objects int

    	Optionally specify the number of concurrent objects to upload

    	(default: 1)

    -concurrent-parts int

    	Optionally specify the number of concurrent parts to upload per
    	object.

    	(default: 1)

    -manifest value

    	Optionally specify a manifest type to produce on standard
    	output.  Valid options are:

    	- json: produce full details about the object as JSON
    	- md5: MD5 checksum and <bucket>/<key>
    	- checksum: selected checksum and <bucket>/<key>
    	- aws: AWS hash-of-hashes checksum and <bucket>/<key>
    	- etag: AWS Object ETag and <bucket>/<key>

    	See MANIFESTS below for more details.

    -media-types string

    	Optionally specify a path to a tab-separated-value file with
    	each line listing an extension and a media-type to use when
    	setting the content-type of an upload, e.g.,

    	.pdf  application/pdf
    	.txt  text/plain

    	Comments may be added by starting the line with '#', and these
    	will be ignored.

    	Any mappings loaded will either override any existing mapping
    	or will be added to the mappings.

    -verbose

    	Optionally enable verbose logging to standard error.

    -checksum string

    	Optionally specify the checksum algorithm to use, one of
    	SHA256, SHA1, CRC32, or CRC32C.

    	(default: SHA256)

    -disable-path-style

    	Optionally disable use of older AWS S3 path-style requests (this
    	would be appropriate to set when copying to Amazon S3 instead of
    	to Elm).

    -disable-s3-pool

    	Optionally disable use of multiple s3 clients (this would be
    	appropriate to set when copying to Amazon S3 instead of to Elm).

    -max-part-id value

    	Optionally limit the number of parts to upload in a multi-part
    	object.

    	(default: 10000)

    -use-temp-dir string

    	Optionally specify a directory to use for temporary files
    	created when buffering a stream.

    -use-memory

    	Optionally specify that memory buffers should be used instead
    	of temporary files when buffering a stream.

    -copy-buf string

    	Optionally specify the buffer size used to copy chunks
    	in-between readers and writers during processing.

    	(default: 256KiB)

    -upload-part-timeout duration

    	Optionally set a timeout for any UploadPart requests, use
    	suffix "s" for seconds, "m" for minutes, "h" for hours, e.g.,
    	15m for 15 minutes.

    	(default: 0s, no timeout)

    -complete-multipart-timeout duration

    	Optionally set a timeout for any CompleteMultipartUpload
    	requests, use suffix "s" for seconds, "m" for minutes, "h" for
    	hours, e.g., 15m for 15 minutes.

    	(default: 0s, no timeout)

    -abort-multipart-timeout duration

    	Optionally set a timeout for any AbortMultipartUpload requests,
    	use suffix "s" for seconds, "m" for minutes, "h" for hours,
    	e.g., 15m for 15 minutes.

    	(default: 0s, no timeout)

    -leave-parts-on-error

    	Optionally do not abort failed uploads, leaving parts on the
    	server for manual recovery.

MANIFESTS

    Manifest types supported are:

    - json: produce full details about the object in JSON format
    - md5: MD5 checksum and <bucket>/<key>
    - checksum: selected checksum and <bucket>/<key
    - aws: AWS hash-of-hashes checksum and <bucket>/<key>
    - etag: AWS Object ETag and <bucket>/<key>

    With the exception of json the manifests take the form of

    	<value>  <bucket>/<key>

    Where <value> is a hex-encoded checksum (e.g., as produced by md5sum,
    sha1sum, sha256sum), an ETag as produced by AWS, or a base64 encoded
    hash-of-hashes as detailed in the AWS documentation section:

    	Using part-level checksums for multipart uploads
    	https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html#large-object-checksums

    The "md5" and "checksum" formats mimics the format used to check
    manifests produced by command line tools such as md5sum and sha1sum:

    	$ ./s3up --bucket test-jrobinso -manifest md5 *.dat
    	0386a9abe1d45fedae59fc3381506533  test-jrobinso/a-a-100MB.dat
    	10b41d719cc4a3e5e3228858ea84d533  test-jrobinso/a-a-200MB.dat
    	061fa237522bd2200ed14453ddfa6c86  test-jrobinso/a-a-500MB.dat

    	$ ./s3up --bucket test-jrobinso --checksum sha1 -manifest checksum *.dat
    	7bc7c147b691b55b4bc05ae3a40fa3bcc274e3fd  test-jrobinso/a-a-100MB.dat
    	481eb555e10d651a84abf64c76e558deab947fae  test-jrobinso/a-a-200MB.dat
    	5698313d0c7e27c16270c08fb250d544a14aa8b4  test-jrobinso/a-a-500MB.dat

    When a json manifest is requested s3up produces a JSON array.  Each
    record in the array corresponds to an uploaded object and contains
    metadata calculated by s3up followed by metadata fetched from the S3
    server (the latter is the ObjectAttributes object). A sample record:

    	{
    		"Bucket": "test-jrobinso",
    		"Key": "500GB-in-large-files/a/y/a-y-500MB.dat",
    		"Completed": true,
    		"Aborted": false,
    		"FullChecksums": {
    		"ChecksumMD5": {
    			"Hex": "77faeaf43e9e70ec067f7927d3e53424",
    			"Base64": "d/rq9D6ecOwGf3kn0+U0JA=="
    		},
    		"ChecksumSHA256": {
    				"Hex": "a8c8f8906df45d5311bdcb7168541f960d8c8fcda6c12e144e7d1240405dc9cb",
    				"Base64": "qMj4kG30XVMRvctxaFQflg2Mj82mwS4UTn0SQEBdycs="
    			}
    		},
    		"ObjectChecksum": {
    			"ChecksumSHA256": {
    				"Hex": "a8c8f8906df45d5311bdcb7168541f960d8c8fcda6c12e144e7d1240405dc9cb",
    				"Base64": "qMj4kG30XVMRvctxaFQflg2Mj82mwS4UTn0SQEBdycs="
    			}
    		},
    		"ObjectAttributes": {
    		"LastModified": "2024-08-28T19:12:51Z",
    		"ETag": "77faeaf43e9e70ec067f7927d3e53424",
    		"Checksum": {
    			"ChecksumSHA256": {
    				"Hex": "a8c8f8906df45d5311bdcb7168541f960d8c8fcda6c12e144e7d1240405dc9cb",
    				"Base64": "qMj4kG30XVMRvctxaFQflg2Mj82mwS4UTn0SQEBdycs="
    			}
    		},
    		"ObjectParts": {
    			"IsTruncated": false,
    			"TotalPartsCount": 1,
    			"Parts": [
    				{
    					"PartNumber": 1,
    					"Size": 500000000,
    					"ChecksumMD5": {
    						"Hex": "77faeaf43e9e70ec067f7927d3e53424",
    						"Base64": "d/rq9D6ecOwGf3kn0+U0JA=="
    					}
    				}
    			]
    		}
    		}
    	}

    If errors were encountered they will be listed in an additional Errors
    field.  The outline of an Errors field is:

    	"Errors": {
    		"PutObjectError": "<error>",
    		"UploadPartErrors": [
    			{
    				"PartNumber": 1,
    				"Error": "<error>"
    			}
    		],
    		"CompleteMultipartUploadError": "<error>",
    		"AbortMultipartUploadError": "<error>",
    		"GetObjectAttributesError": "<error>"
    	}
