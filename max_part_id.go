package main

import (
	"fmt"
	"strconv"
)

// MaxPartID represents an int32 upper limit on the number of parts to be
// created for a multi-part object, with helper functions to parse and produce
// string representations of the number for use via the flag module.
type MaxPartID int32

// String returns a human readable representation
func (p MaxPartID) String() string {
	return fmt.Sprintf("%d", int32(p))
}

// Set parses a string representation
func (p *MaxPartID) Set(s string) error {
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return err
	}

	if n < 1 || n > int64(DefaultMaxPartID) {
		return fmt.Errorf(
			"MaxPartID must be >= 1 and <= %d: %s", DefaultMaxPartID, s)
	}

	*p = MaxPartID(n)

	return nil
}
