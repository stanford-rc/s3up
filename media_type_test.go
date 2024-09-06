package main

import (
	"bytes"
	"fmt"
	"testing"
)

var mediaTypesBuiltin = map[string]string{
	".avif": "image/avif",
	".css":  "text/css; charset=utf-8",
	".gif":  "image/gif",
	".htm":  "text/html; charset=utf-8",
	".html": "text/html; charset=utf-8",
	".jpeg": "image/jpeg",
	".jpg":  "image/jpeg",
	".js":   "text/javascript; charset=utf-8",
	".json": "application/json",
	".mjs":  "text/javascript; charset=utf-8",
	".pdf":  "application/pdf",
	".png":  "image/png",
	".svg":  "image/svg+xml",
	".wasm": "application/wasm",
	".webp": "image/webp",
	".xml":  "text/xml; charset=utf-8",
}

var mediaTypesExtensions = map[string]string{
	".xorby": "application/xorby",
	".xml":   "application/xml",
}

func TestMediaTypeBuiltins(t *testing.T) {
	for ext, expect := range mediaTypesBuiltin {
		fpath := fmt.Sprintf("/some/file/path%s", ext)
		actual := MediaType(fpath)
		if expect != actual {
			t.Errorf("expected [%s] to map to [%s] got [%s]",
				fpath, expect, actual)
		}
	}
}

func TestExtendMediaTypesValidTSV(t *testing.T) {
	buf := &bytes.Buffer{}

	for ext, media_type := range mediaTypesExtensions {
		if buf.Len() == 0 {
			buf.WriteString("# this is a comment\n")
		} else {
			buf.WriteString("\n")
		}
		buf.WriteString(ext)
		buf.WriteString("\t")
		buf.WriteString(media_type)
	}

	err := ExtendMediaTypes(buf)
	if err != nil {
		t.Error("unable to extend media types: ", err)
	}

	for ext, expect := range mediaTypesExtensions {
		fpath := fmt.Sprintf("/some/file/path%s", ext)
		actual := MediaType(fpath)
		if expect != actual {
			t.Errorf("expected [%s] to map to [%s] got [%s]",
				fpath, expect, actual)
		}
	}
}
