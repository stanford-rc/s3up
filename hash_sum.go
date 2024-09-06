package main

import (
	"encoding/base64"
	"encoding/hex"
)

// HashSum represents a []byte returned by a call to the hash.Hash interface's
// Sum([]byte) method.
type HashSum []byte

// Bytes returns the []byte representation of the checksum.
func (p HashSum) Bytes() []byte {
	return []byte(p)
}

// Hex returns the hex-encoded representation of the checksum.
func (p HashSum) Hex() string {
	return hex.EncodeToString(p)
}

// Hex returns the base64-encoded representation of the checksum.
func (p HashSum) Base64() string {
	return base64.StdEncoding.EncodeToString(p)
}

// String returns the hex-encoded representation of the checksum.
func (p HashSum) String() string {
	return p.Hex()
}

// HashSumHex represents a HashSum that can be marshalled or unmarshalelled
// from its hexadecimal representation using the encoding module's
// TextMarshaller and TextUnmarshaller interfaces.
type HashSumHex struct{ HashSum }

func (p HashSumHex) MarshalText() ([]byte, error) {
	return []byte(p.Hex()), nil
}

func (p *HashSumHex) UnmarshalText(t []byte) error {
	buf, err := hex.DecodeString(string(t))
	if err != nil {
		return err
	}

	sum := HashSum(buf)

	*p = HashSumHex{sum}

	return nil
}

// HashSumBase64 represents a HashSum that can be marshalled or unmarshalelled
// from its base64 representation using the encoding module's TextMarshaller
// and TextUnmarshaller interfaces.
type HashSumBase64 struct{ HashSum }

func (p HashSumBase64) MarshalText() ([]byte, error) {
	return []byte(p.Base64()), nil
}

func (p *HashSumBase64) UnmarshalText(t []byte) error {
	buf, err := base64.StdEncoding.DecodeString(string(t))
	if err != nil {
		return err
	}

	sum := HashSum(buf)

	*p = HashSumBase64{sum}

	return nil
}
