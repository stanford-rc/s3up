package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

var st_debug bool = false

// pseudo-random seed (re-using the same seed generates the same pseudo-random
// data)
var st_seed int64 = 69

var st_benchmark_size int64 = 4 * 1024 * 1024

// st_ReaderType represents an identifier for an input to Source
type st_ReaderType string

const (
	// st_ReaderAt doesn't require a backing store in Source
	st_ReaderAt st_ReaderType = "io.ReaderAt"
	// st_Reader does require a backing store in Source
	st_Reader st_ReaderType = "io.Reader"
)

// st_SourceType represents an identifier for a backing store for Source
type st_SourceType string

const (
	// st_TempfileSource uses temporary files on the filesystem to back
	// Source data
	st_TempfileSource st_SourceType = "TempfileReader"
	// st_MemorySource uses memory to back Source data
	st_MemorySource st_SourceType = "MemoryReader"
)

// Validate that Source produces the expected results for the supported reader
// inputs and backing stores
func TestSourceBasics(t *testing.T) {

	// test combinations of input reader types and source backing types
	tests := []struct {
		readerType st_ReaderType
		sourceType st_SourceType
		totalSize  int64
		partSize   int64
	}{
		{
			readerType: st_ReaderAt,
			sourceType: st_TempfileSource,
			totalSize:  0,
			partSize:   10,
		},
		{
			readerType: st_ReaderAt,
			sourceType: st_MemorySource,
			totalSize:  0,
			partSize:   10,
		},
		{
			readerType: st_Reader,
			sourceType: st_TempfileSource,
			totalSize:  0,
			partSize:   10,
		},
		{
			readerType: st_Reader,
			sourceType: st_MemorySource,
			totalSize:  0,
			partSize:   10,
		},
		{
			readerType: st_ReaderAt,
			sourceType: st_TempfileSource,
			totalSize:  8193,
			partSize:   10,
		},
		{
			readerType: st_ReaderAt,
			sourceType: st_MemorySource,
			totalSize:  8193,
			partSize:   10,
		},
		{
			readerType: st_Reader,
			sourceType: st_TempfileSource,
			totalSize:  8193,
			partSize:   10,
		},
		{
			readerType: st_Reader,
			sourceType: st_MemorySource,
			totalSize:  8193,
			partSize:   10,
		},
	}

	for _, tst := range tests {
		// bp will provide a buffer pool for MemorySource
		bp := NewBufferPool(tst.partSize)

		// tstDir will holds temporary files we create when
		// testing the TempfileSource
		tstDir, err := os.MkdirTemp("", "")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(tstDir)

		// pr will provide the test data to iterate over
		pr := st_random_r(st_seed, tst.totalSize)

		// generate the Source to test and the expected output
		src, expect, err := st_test_source(
			pr, tst.partSize, tst.readerType, tst.sourceType, tstDir, bp)
		if err != nil {
			t.Fatal(err)
		}

		// confirm we got the expected underlying Source implementation
		switch tst.readerType {
		case st_ReaderAt:
			if _, ok := src.(*readerAtSource); !ok {
				t.Errorf("readerType %s: not a readerAtSource: %v",
					tst.readerType, src)
			}
		case st_Reader:
			switch tst.sourceType {
			case st_TempfileSource:
				if _, ok := src.(*tempfSource); !ok {
					t.Errorf(
						"readerType %s and sourceType %s: not a tempfSource: %v",
						tst.readerType, tst.sourceType, src)
				}
			case st_MemorySource:
				if _, ok := src.(*memSource); !ok {
					t.Errorf(
						"readerType %s and sourceType %s: not a memSource: %v",
						tst.readerType, tst.sourceType, src)

				}
			default:
				t.Fatalf("unhandled sourceType: %s", tst.sourceType)
			}
		default:
			t.Fatalf("unhandled readerType: %s", tst.readerType)
		}

		// set up map of *SourceReader
		sreaders, err := st_sreaders_map(src)
		if err != nil {
			t.Fatal(err)
		}

		if tst.totalSize == 0 {

		} else {
			n := int(tst.totalSize / tst.partSize)
			if (tst.totalSize % tst.partSize) > 0 {
				n += 1
			}
			if n != len(sreaders) {
				t.Errorf("expected %d entries, got %d", n, len(sreaders))
			}
		}

		// randomly shuffle the sreader keys
		sr_actual, err := st_shuffle_read(st_seed, sreaders)
		if err != nil {
			t.Fatal(err)
		}

		// check that we got the expected data
		actual := st_ordered_bytes(sr_actual)
		if bytes.Compare(expect.Bytes(), actual.Bytes()) != 0 {
			t.Errorf("mismatch:\nexpect: %x\nactual: %x",
				expect.Bytes(), actual.Bytes())
		} else if st_debug {
			log.Printf("%s %s expected bytes matched actual bytes",
				tst.readerType, tst.sourceType)
		}
	}
}

// Benchmark iterating through an io.ReaderAt of st_benchmark_size in 4 parts
// using Source
func BenchmarkSourceReaderAt(b *testing.B) {
	b.StopTimer()

	partSize := st_benchmark_size / 4

	tstDir, err := os.MkdirTemp("", "")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tstDir)

	for i := 0; i < b.N; i++ {
		pr := st_random_r(st_seed, st_benchmark_size)

		fh, cleanup, err := st_input_file(pr, tstDir)
		if err != nil {
			b.Fatal(err)
		}
		defer cleanup()

		// if our testing is correct it should not matter whether we
		// test using a TempfileSource or a MemorySource when passing
		// an io.ReaderAt
		src, err := TempfileSource(fh, partSize, tstDir)
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()
		benchmarkSource(b, src)
		b.StopTimer()
	}
}

// Benchmark iterating through an io.Reader of st_benchmark_size in 4 parts
// using Source backed by temporary files
func BenchmarkTempfileSource(b *testing.B) {
	b.StopTimer()

	partSize := st_benchmark_size / 4

	tstDir, err := os.MkdirTemp("", "")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tstDir)

	for i := 0; i < b.N; i++ {
		pr := st_random_r(st_seed, st_benchmark_size)

		src, err := TempfileSource(pr, partSize, tstDir)
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()
		benchmarkSource(b, src)
		b.StopTimer()
	}
}

// Benchmark iterating through an io.Reader of st_benchmark_size in 4 parts
// using Source backed by memory buffers
func BenchmarkMemorySource(b *testing.B) {
	b.StopTimer()

	partSize := st_benchmark_size / 4

	bp := NewBufferPool(partSize)

	for i := 0; i < b.N; i++ {
		pr := st_random_r(st_seed, st_benchmark_size)

		src, err := MemorySource(pr, partSize, bp)
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()
		benchmarkSource(b, src)
		b.StopTimer()
	}
}

// utility function to iterate over a Source and copy each SourceReader's
// contentsx
func benchmarkSource(b *testing.B, src Source) {
	for {
		sr, err := src.Next()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				b.Fatal(err)
			}
			break
		}

		defer sr.Close()

		if _, err = io.Copy(io.Discard, sr); err != nil {
			b.Error(err)
		}

		if err = sr.Close(); err != nil {
			b.Error(err)
		}
	}
}

// st_test_source creates a Source to read pr using a specified st_ReaderType
// and backed by a specified st_SourceType.
//
// It returns a Source to read from and a *bytes.Buffer holding a copy of the
// original data read from pr.
func st_test_source(
	pr io.Reader,
	partSize int64,
	readerType st_ReaderType,
	sourceType st_SourceType,
	tstDir string,
	bp BufferPool) (Source, *bytes.Buffer, error) {

	var src Source
	var expect *bytes.Buffer
	var err error

	switch readerType {
	case st_ReaderAt:
		// copy everything from pr to data
		data := &bytes.Buffer{}
		io.Copy(data, pr)

		// create io.ReaderAt r_at from data
		r_at := bytes.NewReader(data.Bytes())

		// record data in expect
		expect = &bytes.Buffer{}
		expect.Write(data.Bytes())

		// setup Source with io.ReaderAt r_at
		switch sourceType {
		case st_TempfileSource:
			src, err = TempfileSource(r_at, partSize, tstDir)
		case st_MemorySource:
			src, err = MemorySource(r_at, partSize, bp)
		}
	case st_Reader:
		// tr passes through data from pr and makes a copy into expect
		expect = &bytes.Buffer{}
		tr := io.TeeReader(pr, expect)

		// setup Source with io.Reader tr
		switch sourceType {
		case st_TempfileSource:
			src, err = TempfileSource(tr, partSize, tstDir)
		case st_MemorySource:
			src, err = MemorySource(tr, partSize, bp)
		}
	}

	return src, expect, err
}

// st_provided_r returns an io.Reader that will provide a copy of the original
// buf input.
func st_provided_r(buf []byte) io.Reader {

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		io.Copy(pw, bytes.NewReader(buf))
	}()

	return pr
}

// st_random_r returns an io.Reader that will provide pseudo-random data using
// the provided seed.
func st_random_r(seed, size int64) io.Reader {
	rnd := rand.New(rand.NewSource(seed))
	lr := io.LimitReader(rnd, size)

	pr, pw := io.Pipe()
	go func(r io.Reader) {
		defer pw.Close()
		io.Copy(pw, lr)
	}(lr)

	return pr
}

// st_shuffle_keys returns the keys in m in a pseudo-random order using the
// provided seed.
func st_shuffle_keys(seed int64, m map[int]*SourceReader) []int {
	rnd := rand.New(rand.NewSource(seed))

	keys := make([]int, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	rnd.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys
}

// st_shuffle_read will run goroutines to randonmly read from the SourceReader
// it returns the read data in a map that shares the same keys as m.
func st_shuffle_read(seed int64, m map[int]*SourceReader) (map[int]*bytes.Buffer, error) {
	sr_actual := map[int]*bytes.Buffer{}
	sr_errors := map[int]chan error{}

	wg := &sync.WaitGroup{}
	for _, k := range st_shuffle_keys(seed, m) {
		wg.Add(1)

		sr_actual[k] = &bytes.Buffer{}
		sr_errors[k] = make(chan error)

		go func(k int,
			sr *SourceReader,
			buf *bytes.Buffer,
			ch chan error,
			wg *sync.WaitGroup,
		) {
			defer sr.Close()
			defer wg.Done()

			n, err := io.Copy(buf, sr)

			if st_debug {
				log.Printf("%s: read %d: %d bytes, error %s, bytes %x",
					time.Now(), k, n, err, buf.Bytes())
			}

			ch <- err
		}(k, m[k], sr_actual[k], sr_errors[k], wg)
	}

	for k, ch := range sr_errors {
		if err := <-ch; err != nil {
			err = fmt.Errorf("error on read of id %d: %v", k, err)
			return nil, err
		}
	}

	return sr_actual, nil
}

// st_ordered_bytes reads the map m in sorted key order and returns a
// *bytes.Buffer of the ordered concatenated values.
func st_ordered_bytes(m map[int]*bytes.Buffer) *bytes.Buffer {
	buf := &bytes.Buffer{}

	for i := 0; i < len(m); i++ {
		buf.Write(m[i].Bytes())
	}

	return buf
}

// st_sreaders_map iterates over src serially and returns a map of the
// resulting *SourceReader
func st_sreaders_map(src Source) (map[int]*SourceReader, error) {
	m := map[int]*SourceReader{}

	for {
		sr, err := src.Next()
		if sr != nil {
			m[len(m)] = sr
		}

		if err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			break
		}
	}

	return m, nil
}

func st_input_file(r io.Reader, tstDir string) (*os.File, func(), error) {
	fh, err := os.CreateTemp(tstDir, "*.in")
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		os.Remove(fh.Name())
		fh.Close()
	}

	_, err = io.Copy(fh, r)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	_, err = fh.Seek(0, io.SeekStart)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	return fh, cleanup, err
}
