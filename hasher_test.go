package main

import (
	"fmt"
	"testing"
)

// TestHasher validates that a given input and checksum algorithm produces the
// expected hashed value
func TestHasher(t *testing.T) {
	testAlgos := []struct {
		Name string
		ID   *ChecksumAlgorithm
		Data string
		Hex  string
	}{
		{
			Name: "MD5",
			ID:   ChecksumAlgorithmMD5,
			Data: "Hello, World!",
			Hex:  "65a8e27d8879283831b664bd8b7f0ad4",
		},
		{
			Name: "CRC32",
			ID:   ChecksumAlgorithmCRC32,
			Data: "Hello, World!",
			Hex:  "ec4ac3d0",
		},
		{
			Name: "CRC32C",
			ID:   ChecksumAlgorithmCRC32C,
			Data: "Hello, World!",
			Hex:  "4d551068",
		},
		{
			Name: "SHA1",
			ID:   ChecksumAlgorithmSHA1,
			Data: "Hello, World!",
			Hex:  "0a0a9f2a6772942557ab5355d76af442f8f65e01",
		},
		{
			Name: "SHA256",
			ID:   ChecksumAlgorithmSHA256,
			Data: "Hello, World!",
			Hex:  "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f",
		},
	}

	for i := 0; i < len(testAlgos); i++ {
		ta := testAlgos[i]

		hasher := NewHasher(ta.ID)

		hash := hasher()
		hash.Write([]byte(ta.Data))
		sum := hash.Sum(nil)
		hex := fmt.Sprintf("%x", sum)

		if hex != ta.Hex {
			t.Errorf("expected %s of [%s] to produce %s, got %s: %v",
				ta.Name, ta.Data, ta.Hex, hex, sum)
		}
	}
}
