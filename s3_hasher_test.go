package main

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

// Validate that S3Hasher produce the correct hash values
func TestS3Hasher(t *testing.T) {
	// aws s3api --checksum-algorithm algorithm choices
	testAlgos := []*ChecksumAlgorithm{
		ChecksumAlgorithmCRC32,
		ChecksumAlgorithmCRC32C,
		ChecksumAlgorithmSHA1,
		ChecksumAlgorithmSHA256,
	}

	for i, algo := range testAlgos {
		algoHasher := NewHasher(algo)
		md5Hasher := NewHasher(ChecksumAlgorithmMD5)

		for partSize := 1; partSize < len(lorum); partSize++ {
			// lorum sliced into parts of partSize
			parts := [][]byte{}

			// individual checksums calculated using <algo>
			algoPartChecksum := [][]byte{}

			// individual checksums calculated using MD5
			md5PartChecksum := [][]byte{}

			// read lorum in chunks of up to partSize and add it to
			// the parts slice
			r := strings.NewReader(lorum)
			for {
				// byte buffer of up to partSize bytes
				buf := make([]byte, partSize)

				// read next block of data
				n, err := r.Read(buf)

				if err == io.EOF {
					break
				}

				if err != nil {
					t.Fatalf(
						"error on test %d for %s with buf[%d]: %s",
						i, algo, partSize, err)
				}

				parts = append(parts, buf[0:n])
			}

			// number of parts we expect HashTracker to produce when
			// told to read lorum using the current partSize
			numParts := len(parts)

			// calculating part checksums using <algo>
			for j := 0; j < numParts; j++ {
				h := algoHasher()
				h.Write(parts[j])
				algoPartChecksum = append(algoPartChecksum, h.Sum(nil))
			}

			// calculating part checksums using MD5
			for j := 0; j < numParts; j++ {
				h := md5Hasher()
				h.Write(parts[j])
				md5PartChecksum = append(md5PartChecksum, h.Sum(nil))
			}

			// calculate the expected Checksum hash-of-hashes for
			// these parts
			h := algoHasher()
			for j := 0; j < numParts; j++ {
				h.Write(algoPartChecksum[j])
			}

			sum := HashSum(h.Sum(nil))
			expectChecksum := sum.Base64()

			// calculate the expected MD5 ETag hash-of-hashes for
			// these parts
			h = md5Hasher()
			for j := 0; j < numParts; j++ {
				h.Write(md5PartChecksum[j])
			}
			expectETag := fmt.Sprintf("%x-%d", h.Sum(nil), numParts)

			// we'll use S3HashReader to fully consume lorum and
			// write to an S3HashWriter
			lorum_r := strings.NewReader(lorum)
			s3hr := NewS3HashReader(lorum_r, algo, int64(partSize))
			s3hw := NewS3HashWriter(algo, int64(partSize))
			data := &bytes.Buffer{}

			// read lorum_r via s3hr and write to s3hw and data
			n, err := io.Copy(io.MultiWriter(s3hw, data), s3hr)
			if err != nil {
				t.Fatalf(
					"error on test %d for %s with buf[%d]: %s",
					i, algo, partSize, err)
			}
			if n != int64(len(lorum)) {
				err = fmt.Errorf("read %d of %d bytes", n, len(lorum))
				t.Fatalf(
					"error on test %d for %s with buf[%d]: %s",
					i, algo, partSize, err)
			}

			// bytes read through s3hr should not be altered in any
			// way
			if !bytes.Equal([]byte(lorum), data.Bytes()) {
				err = fmt.Errorf("passthrough bytes did not match original")
				t.Fatalf(
					"error on test %d for %s with buf[%d]: %s",
					i, algo, partSize, err)
			}

			// validate that S3HashReader and S3HashWriter produce
			// the expected values
			for j, s3hash := range []*S3Hasher{
				s3hr.S3Hasher,
				s3hw.S3Hasher,
			} {
				var id string
				switch j {
				case 0:
					id = "S3HashReader"
				case 1:
					id = "S3HashWriter"
				default:
					t.Fatalf("unhandled j iteration: %d", j)
				}

				// ChecksumAlgorithm
				if algo != s3hash.ChecksumAlgorithm() {
					t.Fatalf(
						"%s error on test %d for %s, part-size %d, checksumAlgorithm mismatch: %s vs %s",
						id, i, algo, partSize, algo, s3hash.ChecksumAlgorithm())
				}

				// Count
				if len(parts) != s3hash.Count() {
					t.Fatalf(
						"%s error on test %d for %s, part-size %d, parts count mismatch: %d vs %d",
						id, i, algo, partSize, len(parts), s3hash.Count())
				}

				// PartSize
				for j := 0; j < len(parts); j++ {
					partID := int32(j) + 1
					if int64(len(parts[j])) != s3hash.PartSize(partID) {
						t.Fatalf(
							"%s error on test %d for %s, part-size %d, part-size mismatch: %d vs %d",
							id, i, algo, partSize, len(parts[j]), s3hash.PartSize(partID))
					}
				}

				// SumPart
				for j := 0; j < len(algoPartChecksum); j++ {
					partID := int32(j) + 1
					if !bytes.Equal(algoPartChecksum[j], s3hash.SumPart(partID)) {
						t.Fatalf(
							"%s error on test %d for %s, part-size %d, partID %d checksum mismatch: %s vs %s",
							id, i, algo, partSize, partID,
							HashSum(algoPartChecksum[j]).Base64(),
							HashSum(s3hash.SumPart(partID)).Base64())
					}
				}

				// SumOfSums
				if expectChecksum != HashSum(s3hash.SumOfSums()).Base64() {
					t.Fatalf(
						"%s error on test %d for %s, part-size %d, hash-of-hashes mismatch: %s vs %s",
						id, i, algo, partSize,
						expectChecksum,
						HashSum(s3hash.SumOfSums()).Base64())
				}

				// Sum
				h = algoHasher()
				h.Write(data.Bytes())
				if !bytes.Equal(h.Sum(nil), s3hash.Sum()) {
					t.Fatalf(
						"%s error on test %d for %s, part-size %d, whole-body checksum mismatch: %x vs %x",
						id, i, algo, partSize, h.Sum(nil), s3hash.Sum())

				}

				// MD5Part
				for j := 0; j < len(md5PartChecksum); j++ {
					partID := int32(j) + 1
					if !bytes.Equal(md5PartChecksum[j], s3hash.MD5SumPart(partID)) {
						t.Fatalf(
							"%s error on test %d for %s, part-size %d, partID %d md5 checksum mismatch: %s vs %s",
							id, i, algo, partSize, partID,
							HashSum(md5PartChecksum[j]).Base64(),
							HashSum(s3hash.MD5SumPart(partID)).Base64())
					}
				}

				// MD5
				h = md5Hasher()
				h.Write(data.Bytes())
				if !bytes.Equal(h.Sum(nil), s3hash.MD5Sum()) {
					t.Fatalf(
						"%s error on test %d for %s, part-size %d, whole-body md5 checksum mismatch: %x vs %s",
						id, i, algo, partSize, h.Sum(nil), s3hash.MD5Sum())
				}

				// ETag
				if expectETag != s3hash.ETag() {
					t.Fatalf(
						"%s error on test %d for %s, part-size %d, ETag mismatch: %s vs %s",
						id, i, algo, partSize, expectETag, s3hash.ETag())
				}
			}
		}
	}
}

const lorum string = string(
	`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc gravida leo lacus, ac interdum ipsum imperdiet vitae. In lorem diam, ornare vel ullamcorper suscipit, pulvinar vel urna. Donec nec lectus tellus. Donec non orci in leo sollicitudin ullamcorper eu eu dolor. Sed nibh velit, volutpat a justo vitae, lobortis placerat elit. Maecenas finibus urna id velit ullamcorper pellentesque. Nam posuere ullamcorper porttitor.

Ut elementum luctus mi ut tempor. Sed et leo euismod, faucibus felis vel, rutrum risus. Duis imperdiet et metus at egestas. Nulla ultrices viverra est. Mauris lacus dui, auctor at tortor quis, accumsan tincidunt leo. Proin sagittis commodo arcu at ultricies. Nunc varius est eu odio interdum consequat vehicula tincidunt neque. Ut tempus dictum lacus, nec pretium nisl tempor quis. In at porttitor nunc. Pellentesque mollis tincidunt ultrices. Nam id volutpat ante. Vestibulum orci ligula, lobortis in purus id, posuere condimentum tortor. Nullam nibh ex, feugiat at dui vitae, accumsan auctor purus. Suspendisse bibendum lectus at maximus aliquet. Duis velit elit, vestibulum sit amet est ac, lobortis maximus quam.

Proin vitae tempus massa, ac fermentum neque. Vivamus rhoncus, felis sed feugiat euismod, erat eros euismod tortor, sit amet tincidunt urna nunc eu nisl. Phasellus massa augue, faucibus vel aliquet vel, varius et tortor. Quisque magna felis, placerat scelerisque mi sit amet, tincidunt sollicitudin metus. Cras in eros sed orci porttitor scelerisque. Aliquam eget sapien vel ligula sagittis egestas nec ut enim. In vitae arcu ut elit pulvinar vehicula quis at libero. Cras metus libero, dapibus et nisl vitae, volutpat mattis nibh. Vestibulum et risus purus. Curabitur fermentum leo lorem, eget suscipit lorem mattis in. Cras commodo condimentum ante. Donec sagittis vitae mi id efficitur. Morbi tempus odio at leo lacinia bibendum.

Pellentesque at viverra justo, a pharetra nibh. Sed egestas felis ut nunc feugiat commodo. Phasellus eu nisl a risus auctor lobortis. Pellentesque placerat tempus cursus. Nulla convallis tortor augue, eu rutrum erat blandit eu. Fusce dui dui, elementum pellentesque dictum at, semper at turpis. Phasellus et felis at felis pharetra iaculis vel sed tellus. Nunc id iaculis ligula. Morbi tortor neque, egestas sit amet pellentesque ut, pharetra et lacus. Maecenas ipsum dolor, feugiat dapibus placerat a, vehicula vel neque. Etiam mollis facilisis vestibulum.

Duis eu aliquet risus. Sed vehicula libero eu neque ultrices, eu elementum leo sodales. Duis in varius dolor, id aliquet eros. Sed porttitor orci eu nunc ultricies, quis efficitur odio volutpat. Etiam ut malesuada tellus. Pellentesque non molestie sapien, eu tincidunt enim. Donec vel magna at nulla dapibus volutpat a vel augue. Donec rhoncus nisl non fringilla bibendum. Sed blandit sem lacus, sed posuere nibh tincidunt eu. Duis sagittis dui nunc, pulvinar porta velit placerat eu.`)
