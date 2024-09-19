package main

import (
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
)

// manifestType represents an identifier for a manifest output format.
type manifestType int

const (
	// No manifest output
	NoManifest manifestType = iota

	// Fully detailed manifest using JSON
	JsonManifest

	// MD5 checksum in hexadecimal and bucket/key path
	FullMD5Manifest

	// Configured checksum in hexadecimal and bucket/key path
	FullChecksumManifest

	// hash-of-hashes checksum in base64 and bucket/key path
	AWSChecksumManifest

	// AWS ETag and bucket/key path
	ETagManifest
)

// ManifestType represents a manifestType, with helper functions to parse and
// produce human readable and produce human readable representations of the
// identifier for use via the flag module.
type ManifestType manifestType

func (p ManifestType) String() string {
	switch manifestType(p) {
	case JsonManifest:
		return "json"
	case FullMD5Manifest:
		return "md5"
	case FullChecksumManifest:
		return "checksum"
	case AWSChecksumManifest:
		return "aws"
	case ETagManifest:
		return "etag"
	default:
		return "none"
	}
}

func (p *ManifestType) Set(s string) error {
	switch strings.ToLower(s) {
	case "json":
		*p = ManifestType(JsonManifest)
	case "md5":
		*p = ManifestType(FullMD5Manifest)
	case "checksum":
		*p = ManifestType(FullChecksumManifest)
	case "aws":
		*p = ManifestType(AWSChecksumManifest)
	case "etag":
		*p = ManifestType(ETagManifest)
	case "none":
		*p = ManifestType(NoManifest)
	default:
		return fmt.Errorf("valid manifest types: json, md5, checksum, aws, etag")
	}

	return nil
}

// Manifest returns a manifest generator for the specified manifestType,
// writing the results to the provided io.Writer.
func Manifest(t manifestType, w io.Writer) *manifestGenerator {
	return &manifestGenerator{
		w:    w,
		t:    t,
		nrec: 0,
	}
}

type manifestGenerator struct {
	w    io.Writer
	t    manifestType
	nrec int
}

// End writes trailing text to its io.Writer to indicate the end of the
// manifest, e.g., with JSON it writes the closing brace for a JSON array.
func (p *manifestGenerator) End() error {
	if p.t == NoManifest {
		return nil
	}
	if p.nrec == 0 {
		return nil
	}

	var s string
	if p.t == JsonManifest {
		s = "\n]\n"
	} else {
		s = "\n"
	}

	_, err := io.WriteString(p.w, s)

	return err
}

// Write writes another record for the manifest.
func (p *manifestGenerator) Write(obj *ObjectReporting) error {
	// increment record counter
	p.nrec += 1

	// write the formatted record to p.w
	switch p.t {
	case NoManifest:
		return nil
	case JsonManifest:
		buf, err := json.MarshalIndent(obj, "  ", "  ")
		if err != nil {
			return err
		}

		if p.nrec == 1 {
			// start of JSON array
			if _, err := io.WriteString(p.w, "[\n  "); err != nil {
				return err
			}
		} else {
			// end of prior record in JSON array
			if _, err := io.WriteString(p.w, ",\n  "); err != nil {
				return err
			}
		}

		// write current record in JSON array
		if _, err := p.w.Write(buf); err != nil {
			return err
		}
	default:
		var val string

		switch p.t {
		case FullMD5Manifest:
			val = obj.FullChecksums.ChecksumMD5.Hex
		case FullChecksumManifest:
			for _, c := range []*ObjectChecksum{
				obj.FullChecksums.ChecksumSHA256,
				obj.FullChecksums.ChecksumSHA1,
				obj.FullChecksums.ChecksumCRC32C,
				obj.FullChecksums.ChecksumCRC32,
			} {
				if c != nil {
					val = c.Hex
					break
				}
			}
		case AWSChecksumManifest:
			for _, c := range []*ObjectChecksum{
				obj.ObjectAttributes.Checksum.ChecksumSHA256,
				obj.ObjectAttributes.Checksum.ChecksumSHA1,
				obj.ObjectAttributes.Checksum.ChecksumCRC32C,
				obj.ObjectAttributes.Checksum.ChecksumCRC32,
			} {
				if c != nil {
					val = c.Base64
					break
				}
			}
		case ETagManifest:
			val = *obj.ObjectAttributes.ETag
		}

		if val == "" {
			return fmt.Errorf("error processing %v: unable to extract field value", p.t)
		}

		if p.nrec > 1 {
			// end of prior record in text manifest
			if _, err := io.WriteString(p.w, "\n"); err != nil {
				return err
			}
		}

		// current record in text manifest (note that there are two
		// spaces between the fields)c
		s := fmt.Sprintf("%s  %s", val, path.Join(obj.Bucket, obj.Key))
		if _, err := io.WriteString(p.w, s); err != nil {
			return err
		}
	}

	return nil
}
