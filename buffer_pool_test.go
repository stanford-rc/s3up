package main

import (
	"fmt"
	"testing"
)

// TestBufferPool validates that BufferPool returns []byte slice of the
// expected capacity and that the buffers are re-used on subsequent calls
func TestBufferPool(t *testing.T) {
	sizes := []int64{
		8,
		16,
		32,
		64,
		128,
		256,
		512,
		1024,
		2048,
		4096,
		8192,
	}

	for _, size := range sizes {
		seen := make(map[string]bool)

		pool := NewBufferPool(size)

		nitr := 10

		fill := func() {
			for i := 0; i < nitr; i++ {
				buf := pool.Get(size)

				if size != int64(cap(buf)) {
					t.Errorf("BufferPool returned []byte of cap %d, expected %d",
						cap(buf), size)
				}

				seen[fmt.Sprintf("%p", buf)] = true

				defer pool.Put(buf)
			}
		}

		fill()

		for i := 0; i < nitr; i++ {
			buf := pool.Get(size)

			p := fmt.Sprintf("%p", buf)

			if !seen[p] {
				t.Errorf("expected %s in seen: %#v", p, seen)
			}
		}
	}
}
