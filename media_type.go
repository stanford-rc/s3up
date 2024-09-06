package main

import (
	"bufio"
	"io"
	"log"
	"mime"
	"path/filepath"
	"strings"
)

// MediaType evaluates a file path for recognized extensions and returns the
// first IANA Media Type it recognizes, otherwise it returns the default value
// "application/octet-stream".
//
// MediaType is backed by the golang mime module's TypeByExtension function,
// which documents that it depends on a small built-in table of extensions but
// that:
//
// On Unix it is augmented by the local system's MIME-info database or
// mime.types file(s) if available under one or more of these names:
//
//	/usr/local/share/mime/globs2
//	/usr/share/mime/globs2
//	/etc/mime.types
//	/etc/apache2/mime.types
//	/etc/apache/mime.types
//
// On Windows, MIME types are extracted from the registry.
//
// The ExtendMediaTypes function can be used to add a custom set of mappings to
// the running process.
func MediaType(name string) string {
	for {
		ext := filepath.Ext(name)
		if ext == "" {
			return "application/octet-stream"
		} else {
			name = name[0 : len(name)-len(ext)]
		}

		if typ := mime.TypeByExtension(ext); typ != "" {
			return typ
		}
	}
}

// ExtendMediaTypes extends or replacies entries in the table used by MediaType
// The provided io.Reader r should return lines with two tab-separated fields:
//
// field 1: the extension (including a leading period)
// field 2: a valid IANA Media Type
//
// As an example:
//
//	.pdf	application/pdf
//	.txt	text/plain
//
// The TSV data may optionally contain lines starting with '#' which will be
// treated as comments and ignored.
func ExtendMediaTypes(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	lineno := 0
	for scanner.Scan() {
		lineno += 1

		tsv := scanner.Text()
		if strings.HasPrefix(tsv, "#") {
			// skipping comments
			continue
		}

		fields := strings.Split(tsv, "\t")
		if len(fields) != 2 {
			log.Printf("skipping line %d, invalid number of fields; %d: %s", lineno, len(fields), tsv)
			continue
		}

		ext := fields[0]
		typ := fields[1]

		if err := mime.AddExtensionType(ext, typ); err != nil {
			log.Printf("skipping line %d, format; %s: %s", lineno, err, tsv)
		}
	}

	return nil
}
