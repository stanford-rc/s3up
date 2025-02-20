package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var ErrMultiUploadKey = errors.New(
	"to upload multiple files, specify a blank -key or a -key ending in slash ('/')")

// processGlobs processes Options.globs, returning each source file via the
// returned channel.
func processGlobs(globs []string, Bucket, Key string, recursive, verbose, encodeKey bool) (chan *uploadObject, error) {
	ch := make(chan *uploadObject)

	// if globs is empty then assume we want to read from standard input
	if len(globs) == 0 {
		if Key == "" {
			close(ch)
			return nil, fmt.Errorf(
				"uploading from standard input requires a -key name")
		} else if strings.HasSuffix(Key, "/") {
			close(ch)
			return nil, fmt.Errorf(
				"uploading from standard input requires a -key name, not a prefix: %s", Key)
		}

		if k, err := S3Key(Key, encodeKey); err != nil {
			return nil, err
		} else {
			Key = k
		}

		if verbose {
			log.Printf("reading from standard input")
		}

		go func(ch chan *uploadObject) {
			defer close(ch)

			ch <- &uploadObject{
				bucket: Bucket,
				key:    Key,
				rc:     io.NopCloser(os.Stdin),
			}
		}(ch)

		return ch, nil
	}

	// otherwise iterate over globs and process each entry as a filepath
	// pattern
	go func(ch chan *uploadObject, globs []string) {
		defer close(ch)

		// nqueued tracks how many uploadObject have been returned, and
		// is used to return an error if we encounter multiple upload
		// targets while the key is set to a non-prefix (ending in /)
		// value.
		nqueued := 0

		for _, pattern := range globs {
			// check for one or more filesystem matches for this
			// glob pattern
			matches, err := filepath.Glob(pattern)
			if err != nil {
				log.Printf("error processing glob: %s: %s", pattern, err)
				continue
			}

			// if no matches were found log an error and continue
			if len(matches) == 0 {
				log.Printf("no matches for glob: %s", pattern)
				continue
			}

			// process each matched file or directory
			for _, match := range matches {
				// if a key value was specified and isn't a
				// prefix then we need to log an error if we
				// encounter more than one upload,  to prevent
				// uploading multiple sources to the same
				// target.
				if nqueued > 1 && Key != "" && !strings.HasSuffix(Key, "/") {
					log.Println(ErrMultiUploadKey)
					return
				}

				// stat the source to see what it is, if we
				// encounter an error just log the issue and
				// continue
				fi, err := os.Stat(match)
				if err != nil {
					log.Printf("cannot stat path: %s: %s", match, err)
					continue
				}

				if fi.Mode().IsRegular() {
					// open the file and calculate the
					// bucket / key target name
					fh, err := os.Open(match)
					if err != nil {
						log.Printf("cannot open path: %s: %s", match, err)
						continue
					}

					var currentKey string
					if Key != "" && !strings.HasSuffix(Key, "/") {
						currentKey = Key
					} else {
						currentKey = filepath.ToSlash(filepath.Base(match))
						currentKey = path.Join(Key, currentKey)
					}

					nqueued += 1

					if k, err := S3Key(currentKey, encodeKey); err != nil {
						log.Printf("cannot process path %s: %s", match, err)
						continue
					} else {
						currentKey = k
					}

					ch <- &uploadObject{
						bucket: Bucket,
						key:    currentKey,
						rc:     fh,
					}
				} else if fi.Mode().IsDir() {
					// directories specified in the globs
					// will be walked to find files to
					// upload
					err = filepath.WalkDir(match, func(name string, d fs.DirEntry, err error) error {
						if err != nil {
							return err
						}

						// process top-level directories; process
						// sub-directories if recursive was set.
						if d.IsDir() {
							if recursive || name == match {
								return nil
							}
							return filepath.SkipDir
						}

						// stat the source to determine what it is
						dFi, dErr := d.Info()
						if dErr != nil {
							if errors.Is(dErr, fs.ErrNotExist) {
								return nil
							}
							return dErr
						}

						// if the source wasn't a directory and isn't
						// a regular file, skip processing it
						if !dFi.Mode().IsRegular() {
							return nil
						}

						// submit sub-directory file for upload
						fh, err := os.Open(name)
						if err != nil {
							log.Printf("cannot open path: %s: %s", name, err)
							return nil
						}

						// strip directory prefixes when a trailing slash
						// was specified in the glob, similar to how rsync
						// operates on directory paths
						currentKey := name
						if strings.HasSuffix(match, "/") {
							currentKey, err = filepath.Rel(match, name)
							if err != nil {
								log.Printf("error processing currentKey: %s, %s: %s",
									match, name, err)
								return nil
							}
						}
						if err != nil {
							return err
						}

						// prepend specified Key prefix to currentKey
						currentKey = path.Join(Key, filepath.ToSlash(currentKey))

						// prior to submission increment nqueued and confirm
						// that Key was either blank or was a prefix if
						// multiple files have been queued
						nqueued += 1

						if nqueued > 1 && Key != "" && !strings.HasSuffix(Key, "/") {
							return ErrMultiUploadKey
						}

						if k, err := S3Key(currentKey, encodeKey); err != nil {
							return err
						} else {
							currentKey = k
						}

						// submit upload source
						ch <- &uploadObject{
							bucket: Bucket,
							key:    currentKey,
							rc:     fh,
						}

						return nil
					})

					// log any errors encountered walking the directory
					if err != nil {
						if errors.Is(err, ErrMultiUploadKey) {
							log.Println(err)
							return
						}
						log.Printf("error processing directory: %s: %s", match, err)
					}
				}
			}
		}

	}(ch, globs)

	return ch, nil
}
