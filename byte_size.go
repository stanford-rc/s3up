package main

import (
	"strings"

	"kythe.io/kythe/go/util/datasize"
)

// ByteSize represents an int64 count of bytes, with helper functions to parse
// and produce human readable representations of the size for use via the flag
// module.
type ByteSize int64

// String returns a human readable representation of the number of bytes.
func (p ByteSize) String() string {
	return datasize.Size(p).String()
}

// Set parses a human readable representation of a number of bytes, e.g., 5MiB
// or 5GiB.  See kythe.io/kythe/go/util/datasize for the recognized formats.
func (p *ByteSize) Set(s string) error {
	size, err := datasize.Parse(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		return err
	}

	*p = ByteSize(size)

	return nil
}
