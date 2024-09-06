package main

import (
	"sync"
)

// BufferPool specifies an interface to fetch and return resources from a cache
// pool.
type BufferPool interface {
	Get(int64) []byte
	Put([]byte)
}

// bufferPool implements a simple unbounded cache for reusing []byte of a
// specified size.
type bufferPool struct {
	size int64
	pool *sync.Pool
}

// NewBufferPool initializes a new BufferPool which will return []byte slice of
// the specified size.
func NewBufferPool(size int64) BufferPool {
	return &bufferPool{
		size: size,
		pool: &sync.Pool{
			New: func() any {
				return make([]byte, size)
			},
		},
	}
}

// Get returns a []byte slice of the specified length, resizing it (shrinking
// or reallocating) if necessary.  The slice should be returned via Put when
// the caller has finished with it.
func (p *bufferPool) Get(size int64) []byte {

	buf := p.pool.Get().([]byte)
	if int64(len(buf)) < size {
		if n := size - int64(cap(buf)); n > 0 {
			// old buf capacity was too small, discard the old buf
			// and create a new one to eventually return to the
			// pool
			buf = make([]byte, size)
		} else {
			// buf had capacity, resize in place
			buf = buf[0:size]
		}
	} else if int64(len(buf)) > size {
		// buf was too large, shrink it to the desired size
		buf = buf[0:size]
	}

	return buf
}

// Put returns a []byte slice to be added back to the cache pool to become
// available from another call to Get.
func (p *bufferPool) Put(b []byte) {
	p.pool.Put(b)
}
