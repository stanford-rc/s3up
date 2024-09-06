package main

import (
	"fmt"
	"testing"
)

func TestByteSizeSet(t *testing.T) {
	conversions := []struct {
		s string
		p int64
	}{
		{
			s: "B",
			p: 1e0,
		},
		{
			s: "kB",
			p: 1e3,
		},
		{
			s: "MB",
			p: 1e6,
		},
		{
			s: "GB",
			p: 1e9,
		},
		{
			s: "TB",
			p: 1e12,
		},
		{
			s: "PB",
			p: 1e15,
		},
		{
			s: "KiB",
			p: 1024,
		},
		{
			s: "MiB",
			p: 1024 * 1024,
		},
		{
			s: "GiB",
			p: 1024 * 1024 * 1024,
		},
		{
			s: "TiB",
			p: 1024 * 1024 * 1024 * 1024,
		},
		{
			s: "PiB",
			p: 1024 * 1024 * 1024 * 1024 * 1024,
		},
	}

	for i, c := range conversions {
		for _, f := range []float64{1, 2, 3, 4, 5, 6} {
			input := fmt.Sprintf("%.0f%s", f, c.s)

			expect := f * float64(c.p)

			var actual ByteSize
			actual.Set(input)

			if int64(expect) != int64(actual) {
				t.Errorf("%d test parsing [%s] expected [%.6f] got [%s]",
					i, input, expect, actual)
			}

			if input != actual.String() {
				t.Errorf("%d test serializing [%s] expected [%s] got [%s]",
					i, input, input, actual)
			}
		}
	}
}
