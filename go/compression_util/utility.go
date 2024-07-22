package compression_util

import (
	"bytes"
	"io"

	"github.com/pierrec/lz4"
)

func Compress(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
	if _, err := writer.Write(raw); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decompress(compressedBytes []byte) ([]byte, error) {
	compressedReader := bytes.NewReader(compressedBytes)
	reader := lz4.NewReader(compressedReader)
	bytes, error := io.ReadAll(reader)
	return bytes, error
}
