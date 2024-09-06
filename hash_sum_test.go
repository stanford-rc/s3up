package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
)

// input for hash or base64 encoding
var test_hash_raw string = "Hello, World!"

// base64 encoded value of test_hash_raw
var test_hash_base64 string = base64.StdEncoding.EncodeToString([]byte(test_hash_raw))

// hex encoded value of test_hash_raw
var test_hash_hex string = hex.EncodeToString([]byte(test_hash_raw))

// TestHashToHex validates that the raw input can be converted into hex
func TestHashToHex(t *testing.T) {
	input := test_hash_raw

	expect := test_hash_hex

	h := HashSum([]byte(input))
	actual := h.Hex()

	if expect != actual {
		t.Errorf("expected [%s] got [%s]", expect, actual)
	}
}

// TestHashSumHexMarshal validates that the raw input can be converted into json
func TestHashSumHexMarshal(t *testing.T) {
	data := struct {
		Hash *HashSumHex `json:"hash,omitempty"`
	}{
		Hash: &HashSumHex{[]byte(test_hash_raw)},
	}

	buf, err := json.Marshal(data)
	if err != nil {
		t.Error("marshal error: ", err)
	}

	expect := fmt.Sprintf(`{"hash":"%s"}`, test_hash_hex)

	actual := string(buf)

	if expect != actual {
		t.Errorf("expected [%s], got [%s]", expect, actual)
	}
}

// TestHashSumHexUnmarshal confirms that encoded json can be turned into the expected raw vaue
func TestHashSumHexUnmarshal(t *testing.T) {
	input := []byte(fmt.Sprintf(`{"hash": "%s"}`, test_hash_hex))

	expect := test_hash_raw

	target := map[string]*HashSumHex{}

	err := json.Unmarshal(input, &target)
	if err != nil {
		t.Error("unable to unmarshal input: ", err)
	}

	if v, ok := target["hash"]; !ok {
		t.Errorf("target missing expected 'hash' key: %#v", target)
	} else {
		actual := string(v.HashSum)

		if expect != actual {
			t.Errorf("expected [%s], got [%s]", expect, actual)
		}
	}
}

// TestHashToBase64 validates that the raw input can be converted into base64
func TestHashToBase64(t *testing.T) {
	input := test_hash_raw

	expect := test_hash_base64

	h := HashSum([]byte(input))
	actual := h.Base64()

	if expect != actual {
		t.Errorf("expected [%s] got [%s]", expect, actual)
	}
}

// TestHashSumBase64Marshal validates that the raw input can be converted into json
func TestHashSumBase64Marshal(t *testing.T) {
	data := struct {
		Hash *HashSumBase64 `json:"hash,omitempty"`
	}{
		Hash: &HashSumBase64{[]byte(test_hash_raw)},
	}

	buf, err := json.Marshal(data)
	if err != nil {
		t.Error("marshal error: ", err)
	}

	expect := fmt.Sprintf(`{"hash":"%s"}`, test_hash_base64)

	actual := string(buf)

	if expect != actual {
		t.Errorf("expected [%s], got [%s]", expect, actual)
	}
}

// TestHashSumHexUnmarshal confirms that encoded json can be turned into the expected raw vaue
func TestHashSumBase64Unmarshal(t *testing.T) {
	input := []byte(fmt.Sprintf(`{"hash": "%s"}`, test_hash_base64))

	expect := test_hash_raw

	target := map[string]*HashSumBase64{}

	err := json.Unmarshal(input, &target)
	if err != nil {
		t.Error("unable to unmarshal input: ", err)
	}

	if v, ok := target["hash"]; !ok {
		t.Errorf("target missing expected 'hash' key: %#v", target)
	} else {
		actual := string(v.HashSum)

		if expect != actual {
			t.Errorf("expected [%s], got [%s]", expect, actual)
		}
	}
}
