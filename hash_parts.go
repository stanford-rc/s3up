package main

import (
	"hash"
)

// HashPart represents the hash of a single part in a multi-part object.
type HashPart struct {
	// total number of bytes written to this HashPart
	n int64

	// hash algorithm implementation for this HashPart
	h hash.Hash
}

// Sum returns the full-body HashSum using the configure checksum algorithm.
func (hp *HashPart) Sum() HashSum {
	return HashSum(hp.h.Sum(nil))
}

// HashParts represents the parts in a multi-part object.
type HashParts struct {
	// maximum number of bytes per part
	partSize int64

	// checksum algorithm identifier
	checksumAlgorithm *ChecksumAlgorithm

	// hash.Hash generator factory
	hasher Hasher

	// current hash part
	p *HashPart

	// previous and current HashPart in the order that they were created
	h []*HashPart
}

// NewHashParts initializes a new HashParts using the specified checksum
// algorithm and a maximum part size in bytes.
func NewHashParts(checksumAlgorithm *ChecksumAlgorithm, partSize int64) *HashParts {
	return &HashParts{
		partSize:          partSize,
		checksumAlgorithm: checksumAlgorithm,
		hasher:            NewHasher(checksumAlgorithm),
	}
}

// ChecksumAlgorithm returns the checksum algorithm configured for this
// HashParts.
func (hp *HashParts) ChecksumAlgorithm() *ChecksumAlgorithm {
	return hp.checksumAlgorithm
}

// Count returns the number of parts hashed so far (including the current one).
func (hp *HashParts) Count() int {
	return len(hp.h)
}

// PartSize returns the number of bytes used for part partID.  Valid values for
// partID are 1 >= partID <= HashParts.Count().
func (hp *HashParts) PartSize(partID int32) int64 {
	return hp.h[int(partID)-1].n
}

// Sum returns the HashSum for partID using the configured checksum algorithm.
// Valid values for partID are 1 >= partID <= HashParts.Count().
func (hp *HashParts) Sum(partID int32) HashSum {
	return HashSum(hp.h[int(partID)-1].h.Sum(nil))
}

// SumOfSums returns the hash-of-hashes HashSum for the current parts using the
// configured checksum algorithm.
func (hp *HashParts) SumOfSums() HashSum {

	hoh := hp.hasher()

	for i := 0; i < len(hp.h); i++ {
		hoh.Write(hp.h[i].h.Sum(nil))
	}

	return HashSum(hoh.Sum(nil))
}

// Write adds more data to the running hashes, appending a new HashPart each
// time partSize bytes are written to the current part.  It never returns an
// error.
func (hp *HashParts) Write(buf []byte) (int, error) {
	// if hp.p is not set, allocate a new HashPart and add its hash to the
	// hp.h slice
	if hp.p == nil {
		hp.p = &HashPart{
			n: 0,
			h: hp.hasher(),
		}

		hp.h = append(hp.h, hp.p)
	}

	nbuf := len(buf)

	for len(buf) > 0 {
		// if hp.p was reset to nil after the last write, allocate a
		// new HashPart and add its hash to the hp.h slice
		if hp.p == nil {
			hp.p = &HashPart{
				n: 0,
				h: hp.hasher(),
			}

			hp.h = append(hp.h, hp.p)
		}

		// set n to the number of bytes from buf to write to the
		// current hash
		n := int64(len(buf))
		if hp.p.n+n > hp.partSize {
			// reduce n to the remaining bytes available to write
			// for this part
			n = hp.partSize - hp.p.n
		}

		hp.p.h.Write(buf[0:n])

		// record bytes written
		hp.p.n, buf = (hp.p.n + n), buf[n:]

		// if we've reached hp.partSize bytes written, reset hp.p for
		// the next iteration
		if hp.p.n == hp.partSize {
			hp.p = nil
		}
	}

	return nbuf, nil
}
