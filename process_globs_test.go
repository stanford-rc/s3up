package main

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func test_globs_gather(ch chan *uploadObject) []*uploadObject {
	a := []*uploadObject{}

	for v := range ch {
		a = append(a, v)
	}

	return a
}

func test_globs_close(t *testing.T, x []*uploadObject) {
	for _, v := range x {
		if err := v.rc.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func test_globs_keys(t *testing.T, prefix string, x []*uploadObject) []string {
	var keys []string

	for _, v := range x {
		keys = append(keys, v.key)

		buf, err := io.ReadAll(v.rc)
		if err != nil {
			t.Error(err)
		}

		// check that the file contents are what we expect
		if !strings.HasSuffix(filepath.Base(v.key), filepath.Join("", filepath.Base(string(buf)))) {
			t.Errorf("expected %s contents to have substring %s: got %s",
				v.key, filepath.Join("", filepath.Base(v.key)), string(buf))
		}
	}

	return keys
}

func test_globs_expect(t *testing.T, prefix string, x []*uploadObject, bucket string, keys []string) {

	for i, v := range x {
		if v.bucket != bucket {
			t.Errorf("expected item #%d to have bucket %s, got bucket %s",
				i, bucket, v.bucket)
		}
	}

	actual := test_globs_keys(t, prefix, x)

	if len(keys) != len(actual) {
		t.Errorf("expected %d items, got %d:\nkeys: %#v\nactual: %#v",
			len(keys), len(actual), keys, actual)
	}

	sort.Strings(actual)
	sort.Strings(keys)
	for i := 0; i < len(keys); i++ {
		if actual[i] != keys[i] {
			t.Errorf("expected item #%d to be %s, got %s:\nkeys: %#v\nactual: %#v",
				i, keys[i], actual[i], keys, actual)
		}
	}
}

func TestProcessGlobs(t *testing.T) {
	tests := []struct {
		desc      string
		recursive bool
		bucket    string
		key       string
		fs        []string
		globs     []string
		expect    func(string, chan *uploadObject, error)
	}{
		{
			desc:      "processing a directly supplied directory path w/o recursive uploads just the top-level files",
			bucket:    "bucket",
			key:       "",
			recursive: false,
			fs: []string{
				"a",
				"b",
				"c",
				"d/e",
				"d/f",
				"d/g",
			},
			globs: []string{
				"./",
			},
			expect: func(tstDir string, ch chan *uploadObject, err error) {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				if ch == nil {
					t.Error("unexpected nil ch")
				} else {
					x := test_globs_gather(ch)

					defer test_globs_close(t, x)

					test_globs_expect(t, tstDir, x, "bucket", []string{
						"a", "b", "c"})
				}
			},
		},
		{
			desc:      "processing a directly supplied directory path w/ recursive uploads uploads all files and preserves directory paths",
			bucket:    "bucket",
			key:       "",
			recursive: true,
			fs: []string{
				"a",
				"b",
				"c",
				"d/e",
				"d/f",
				"d/g",
			},
			globs: []string{
				"./",
			},
			expect: func(tstDir string, ch chan *uploadObject, err error) {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				if ch == nil {
					t.Error("unexpected nil ch")
				} else {
					x := test_globs_gather(ch)

					defer test_globs_close(t, x)

					test_globs_expect(t, tstDir, x, "bucket", []string{
						"a", "b", "c", "d/e", "d/f", "d/g"})
				}
			},
		},
		{
			desc:      "processing a glob containing a directory path uploads all the files and the directory including its own name",
			bucket:    "bucket",
			key:       "z/",
			recursive: false,
			fs: []string{
				"a",
				"b",
				"c",
				"d/e",
				"d/f",
				"d/g",
			},
			globs: []string{
				"*",
			},
			expect: func(tstDir string, ch chan *uploadObject, err error) {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				if ch == nil {
					t.Error("unexpected nil ch")
				} else {
					x := test_globs_gather(ch)

					defer test_globs_close(t, x)

					test_globs_expect(t, tstDir, x, "bucket", []string{
						"z/a", "z/b", "z/c", "z/d/e", "z/d/f", "z/d/g"})
				}
			},
		},
		{
			desc:      "processing a directory path with a trailing slash uploads the files w/o the directory name",
			bucket:    "bucket",
			key:       "z/",
			recursive: false,
			fs: []string{
				"a",
				"b",
				"c",
				"d/e",
				"d/f",
				"d/g",
			},
			globs: []string{
				"d/",
			},
			expect: func(tstDir string, ch chan *uploadObject, err error) {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				if ch == nil {
					t.Error("unexpected nil ch")
				} else {
					x := test_globs_gather(ch)

					defer test_globs_close(t, x)

					test_globs_expect(t, tstDir, x, "bucket", []string{
						"z/e", "z/f", "z/g"})
				}
			},
		},
		{
			desc:      "processing a directory path w/o a trailing slash preserves the directory name",
			bucket:    "bucket",
			key:       "z/",
			recursive: false,
			fs: []string{
				"a",
				"b",
				"c",
				"d/e",
				"d/f",
				"d/g",
			},
			globs: []string{
				"d",
			},
			expect: func(tstDir string, ch chan *uploadObject, err error) {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				if ch == nil {
					t.Error("unexpected nil ch")
				} else {
					x := test_globs_gather(ch)

					defer test_globs_close(t, x)

					test_globs_expect(t, tstDir, x, "bucket", []string{
						"z/d/e", "z/d/f", "z/d/g"})
				}
			},
		},
	}

	for _, tst := range tests {
		tstDir, err := os.MkdirTemp("", "")
		if err != nil {
			t.Fatal(err)
		}

		defer os.RemoveAll(tstDir)

		if err = os.Chdir(tstDir); err != nil {
			t.Fatal(err)
		}

		for _, name := range tst.fs {
			fpath := filepath.FromSlash(filepath.Join(tstDir, name))

			err = os.MkdirAll(path.Dir(fpath), 0755)
			if err != nil {
				t.Fatal(err)
			}

			fh, err := os.Create(fpath)
			if err != nil {
				t.Fatal(err)
			}

			if _, err = fh.Write([]byte(name)); err != nil {
				t.Fatal(err)
			}

			if err = fh.Close(); err != nil {
				t.Fatal(err)
			}
		}

		ch, err := processGlobs(tst.globs, tst.bucket, tst.key, tst.recursive, false)
		tst.expect(tstDir, ch, err)
	}
}
