package main

import (
	"context"
	"errors"
	"testing"
)

func TestProcessFlags(t *testing.T) {

	required_ok := []string{
		"-bucket", "bucket",
		"glob1", "glob2", "glob3",
	}

	tests := []struct {
		optional []string
		required []string
		expect   func(opts *Options, err error)
	}{
		/*
			{
				required:
				expect: func(opts *Options, err error) {

				},
			},
		*/
		{
			optional: []string{"-checksum", "MD5"},
			required: required_ok,
			expect: func(opts *Options, err error) {
				if !errors.Is(err, errBadChecksum) {
					t.Errorf("expected errBadChecksum, got %v", err)
				}
			},
		},
		{
			optional: []string{"-part-size", "1MiB"},
			required: required_ok,
			expect: func(opts *Options, err error) {
				if !errors.Is(err, errBadPartSize) {
					t.Errorf("expected errBadPartSize, got %v", err)
				}
			},
		},
		{
			required: required_ok,
			expect: func(opts *Options, err error) {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if opts == nil {
					t.Errorf("nil options returned")
				} else {
					if opts.MaxPartID != DefaultMaxPartID {
						t.Errorf("expected default MaxPartID %d, got %d",
							DefaultMaxPartID, opts.MaxPartID)
					}
					if opts.bucket != "bucket" {
						t.Errorf("expected -bucket bucket: %s", opts.bucket)
					}
					if len(opts.globs) != 3 {
						t.Errorf("expected 3 globs: %s", opts.globs)
					}
				}
			},
		},
	}

	for _, tst := range tests {
		opts, err := processFlags(
			context.Background(), append(tst.optional, tst.required...))

		tst.expect(opts, err)
	}
}
