package main

import (
	"fmt"
	"net/url"
	"unicode"
	"unicode/utf8"
)

// upper limit on the length of key
var MAX_KEY_BYTES = 1024

// S3Key checks key to ensure it is valid UTF-8 without control characters.  If
// encode is set to true then any invalid UTF-8 bytes or control characters
// will be percent-encoded.  If encode is set to false an either invalid UTF-8
// or control characters are present then the original key and an error will be
// returned.
func S3Key(key string, encode bool) (string, error) {
	if !encode && !utf8.ValidString(key) {
		return key, fmt.Errorf(
			"key is not valid UTF-8 and percent-encoding was not requested: %s", key)
	}

	// iterate over key p to build x, checking for invalid UTF-8 bytes or
	// for control characters and percent-encoding them into x
	p := []byte(key)
	x := make([]byte, 0, len(p)*4)
	for {
		if len(p) == 0 {
			break
		}

		r, w := utf8.DecodeRune(p)
		if r != utf8.RuneError && w != 0 && !unicode.IsControl(r) {
			x = utf8.AppendRune(x, r)
			p = p[w:]
			continue
		}

		// record current length of x to check whether or not we've
		// added to x when we search for the next valid UTF-8 character
		xn := len(x)

		// search p for the next valid rune, if any
		for i := 0; i < len(p); i++ {
			r, w = utf8.DecodeRune(p[i:])
			if r != utf8.RuneError && w != 0 && !unicode.IsControl(r) {
				pct := url.PathEscape(string(p[0:i]))
				x = append(x, []byte(pct)...)
				x = utf8.AppendRune(x, r)
				p = p[i+w:]
				break
			}
		}

		// if no more valid runes were found, encode the rest of the
		// key
		if xn == len(x) {
			pct := url.PathEscape(string(p))
			x = append(x, []byte(pct)...)
			p = p[len(p):]
		}
	}

	if !encode && string(x) != key {
		return key, fmt.Errorf(
			"key contained control characters and percent-encoding was not requested: %s",
			string(x))
	}

	// convert k to a string
	xk := string(x)

	// check for valid key length
	if len(x) > MAX_KEY_BYTES {
		if xk != key {
			return key, fmt.Errorf(
				"encoded key is %d bytes which exceeds the maximum of %d: %s",
				len(x), MAX_KEY_BYTES, xk)
		} else {
			return key, fmt.Errorf(
				"key is %d bytes which exceeds the maximum of %d: %s",
				len(x), MAX_KEY_BYTES, xk)
		}
	}

	return xk, nil
}
