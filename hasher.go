package main

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"
	"hash/crc32"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Hasher defines a generic function that returns hash.Hash, it is used to mask
// cases where the setup of a hash.Hash requires multiple steps (e.g., with
// CRC32 and CRC32C).
type Hasher func() hash.Hash

// ChecksumAlgorithm represents a named checksum algorithm and, if available,
// its AWS types.ChecksumAlgorithm counterpart.  In some cases an AWS
// counterpart is not available (e.g., with MD5)
type ChecksumAlgorithm struct {
	Name    string
	awsType types.ChecksumAlgorithm
}

// String returns the name of this algorithm.
func (p ChecksumAlgorithm) String() string {
	return p.Name
}

// HasType returns true if it has an associates AWS types.ChecksumAlgorithm.
func (p ChecksumAlgorithm) HasType() bool {
	return p.awsType != ""
}

// Type returns the counterpart AWS types.ChecksumAlgorithm if one was defined,
// otherwise it returns an empty string.
func (p ChecksumAlgorithm) Type() types.ChecksumAlgorithm {
	return p.awsType
}

// MD5 checksum algorithm.
var ChecksumAlgorithmMD5 = &ChecksumAlgorithm{
	Name: "MD5",
}

// CRC32 (IEEE 802.3) checksum algorithm.
var ChecksumAlgorithmCRC32 = &ChecksumAlgorithm{
	Name:    "CRC32",
	awsType: types.ChecksumAlgorithmCrc32,
}

// CRC32C (Castagnoli) checksum algorithm.
var ChecksumAlgorithmCRC32C = &ChecksumAlgorithm{
	Name:    "CRC32C",
	awsType: types.ChecksumAlgorithmCrc32c,
}

// SHA1 checksum algorithm.
var ChecksumAlgorithmSHA1 = &ChecksumAlgorithm{
	Name:    "SHA1",
	awsType: types.ChecksumAlgorithmSha1,
}

// SHA256 checksum algorithm.
var ChecksumAlgorithmSHA256 = &ChecksumAlgorithm{
	Name:    "SHA256",
	awsType: types.ChecksumAlgorithmSha256,
}

// NewHasher returns the Hasher generator for the specified ChecksumAlgorithm.
// It panics if the ChecksumAlgorithm is not recognized.
func NewHasher(checksumAlgorithm *ChecksumAlgorithm) Hasher {
	switch checksumAlgorithm {
	case ChecksumAlgorithmMD5:
		return md5.New
	case ChecksumAlgorithmCRC32:
		return func() hash.Hash {
			return crc32.New(crc32.MakeTable(crc32.IEEE)).(hash.Hash)
		}
	case ChecksumAlgorithmCRC32C:
		return func() hash.Hash {
			return crc32.New(crc32.MakeTable(crc32.Castagnoli)).(hash.Hash)
		}
	case ChecksumAlgorithmSHA1:
		return sha1.New
	case ChecksumAlgorithmSHA256:
		return sha256.New
	default:
		panic(fmt.Sprintf("unknown ChecksumAlgorithm: %v", checksumAlgorithm))
	}
}
