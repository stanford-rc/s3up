package main

import (
	"bytes"
	"errors"
	"io"
	"os"
)

// copyBufSize sets the size of the buffer used to copy between an underlying
// io.Reader and a temp file or memory buffer
var copyBufSize int64 = DefaultCopyBufSize

// copyBuf is used to cache []byte used to copy between an underlying io.Reader
// and a temp file or memory buffer
var copyBuf BufferPool = NewBufferPool(copyBufSize)

// Source defines an interface for a generator of SourceReader from an
// underlying io.ReaderAt or io.Reader.
type Source interface {
	Next() (*SourceReader, error)
}

// SourceReader extends io.SectionReader with a Close method compatible with
// io.Closer.
type SourceReader struct {
	*io.SectionReader
	closer func() error
}

func (p *SourceReader) Close() error {
	return p.closer()
}

// seekLimit returns the length of an io.Seeker
func seekLimit(seeker io.Seeker) (int64, error) {
	pos, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}

	limit, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		return -1, err
	}

	_, err = seeker.Seek(pos, io.SeekStart)
	if err != nil {
		return -1, err
	}

	return limit, nil
}

// TempfileSource returns a Source that will generate SourceReader backed by
// temporary files when r does not implement io.ReaderAt and io.Seeker.  If r
// does implement io.ReaderAt and io.Seeker then direct access to r will be
// used instead.
//
// When temporary files are used they will be created under the specified
// tempDir.  If tempDir is the empty string then the Operating System default
// will be used.
//
// Disk consumption will be at least the partSize multiplied by the number of
// concurrent parts being uploaded at any given point in time.
func TempfileSource(r io.Reader, partSize int64, tempDir string) (Source, error) {
	var src Source

	if readerAt, ok := r.(io.ReaderAt); ok {
		if seeker, ok := r.(io.Seeker); ok {
			limit, err := seekLimit(seeker)
			if err != nil {
				return nil, err
			}

			src = &readerAtSource{
				r:        readerAt,
				limit:    limit,
				offset:   0,
				partSize: partSize,
			}

			return src, nil
		}
	}

	src = &tempfSource{
		r:        r,
		tempDir:  tempDir,
		partSize: partSize,
	}

	return src, nil
}

// MemorySource returns a Source that will generate SourceReader backed by
// memory buffers when r does not implement io.ReaderAt and io.Seeker.  If r
// does implement io.ReaderAt and io.Seeker then direct access to r will be
// used instead.
//
// When memory buffers are used they will be created and returned via bp, which
// should be configured to return []byte of partSize length, otherwise buffers
// will have to be reallocated to that size.
//
// Memory consumption will be at least the partSize multiplied by the number of
// concurrent parts being uploaded at any given point in time.
func MemorySource(r io.Reader, partSize int64, bp BufferPool) (Source, error) {
	var src Source

	if readerAt, ok := r.(io.ReaderAt); ok {
		if seeker, ok := r.(io.Seeker); ok {
			limit, err := seekLimit(seeker)
			if err != nil {
				return nil, err
			}

			src = &readerAtSource{
				r:        readerAt,
				limit:    limit,
				offset:   0,
				partSize: partSize,
			}

			return src, nil
		}
	}

	src = &memSource{
		bp:       bp,
		r:        r,
		partSize: partSize,
	}

	return src, nil
}

// readerAtSource uses the underlying io.ReaderAt to directly read from the
// underlying source
type readerAtSource struct {
	r        io.ReaderAt
	limit    int64
	offset   int64
	partSize int64
}

func (p *readerAtSource) Next() (*SourceReader, error) {
	if p.offset >= p.limit {
		return nil, io.EOF
	}

	size := p.partSize
	if p.offset+size > p.limit {
		size = p.limit - p.offset
	}

	sr := &SourceReader{
		SectionReader: io.NewSectionReader(p.r, p.offset, size),
		closer:        func() error { return nil },
	}

	p.offset += size

	return sr, nil
}

// tempfSource uses a temporary file
type tempfSource struct {
	r        io.Reader
	tempDir  string
	partSize int64
}

func (p *tempfSource) Next() (*SourceReader, error) {
	fh, err := os.CreateTemp(p.tempDir, "*.s3up")
	if err != nil {
		return nil, err
	}

	cleanup := func() {
		fh.Close()
		os.Remove(fh.Name())
	}

	lr := io.LimitReader(p.r, p.partSize)

	chunk := copyBuf.Get(copyBufSize)
	defer copyBuf.Put(chunk)

	var size int64
	for {
		n, err := lr.Read(chunk)

		if n > 0 {
			var nw int
			nw, err = fh.Write(chunk[0:n])
			size += int64(nw)
		}

		if err != nil {
			if !errors.Is(err, io.EOF) {
				defer cleanup()
				return nil, err
			}
			break
		}
	}

	if size == 0 {
		defer cleanup()
		return nil, io.EOF
	}

	_, err = fh.Seek(0, io.SeekStart)
	if err != nil {
		defer cleanup()
		return nil, err
	}

	rc := &tempfBuffer{
		fh: fh,
	}

	sr := &SourceReader{
		SectionReader: io.NewSectionReader(rc, 0, size),
		closer:        rc.Close,
	}

	return sr, nil
}

// tempBuffer is backed by a temporary file, closing the buffer deletes the
// temporary file
type tempfBuffer struct {
	fh *os.File
}

func (p *tempfBuffer) ReadAt(b []byte, off int64) (n int, err error) {
	return p.fh.ReadAt(b, off)
}

func (p *tempfBuffer) Close() error {
	defer os.Remove(p.fh.Name())
	return p.fh.Close()
}

// memSource uses a bytes.Reader backed by a []byte slice allocated from a
// BufferPool
type memSource struct {
	r        io.Reader
	partSize int64
	bp       BufferPool
}

func (p *memSource) Next() (*SourceReader, error) {
	// lr limits the number of bytes read from p.r so that we only read up
	// to the maximum part size
	lr := io.LimitReader(p.r, p.partSize)

	// chunk will be used to copy from lr in stages
	chunk := copyBuf.Get(copyBufSize)
	defer copyBuf.Put(chunk)

	// buf will hold the in-memory copy of the part
	buf := p.bp.Get(p.partSize)
	buf = buf[0:0]

	var size int64
	for {
		n, err := lr.Read(chunk)

		if n > 0 {
			buf = append(buf, chunk[0:n]...)
			size += int64(n)
		}

		if err != nil {
			break
		}
	}

	if size == 0 {
		p.bp.Put(buf)
		return nil, io.EOF
	}

	rc := &memBuffer{
		bp: p.bp,
		b:  buf,
		r:  bytes.NewReader(buf),
	}

	sr := &SourceReader{
		SectionReader: io.NewSectionReader(rc, 0, size),
		closer:        rc.Close,
	}

	return sr, nil
}

// memBuffer is backed by a []byte slice allocated from a BufferPool
type memBuffer struct {
	bp BufferPool
	b  []byte
	r  io.ReaderAt
}

func (p *memBuffer) ReadAt(b []byte, off int64) (n int, err error) {
	return p.r.ReadAt(b, off)
}

func (p *memBuffer) Close() error {
	if p.b != nil {
		p.bp.Put(p.b)
		p.b = nil
	}
	return nil
}
