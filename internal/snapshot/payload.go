package snapshot

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
)

const (
	// CompressionGzip stores snapshot payloads with gzip compression.
	CompressionGzip = "gzip"
)

// CreateInput configures one manual snapshot package creation.
type CreateInput struct {
	Store    Store
	Manifest Manifest
	Data     []byte
}

// Create compresses a manual snapshot payload, records its checksum, and stores it.
func Create(ctx context.Context, input CreateInput) (Manifest, error) {
	if input.Store == nil {
		return Manifest{}, fmt.Errorf("snapshot store is required")
	}
	data, err := CompressGzip(input.Data)
	if err != nil {
		return Manifest{}, err
	}

	manifest := input.Manifest
	manifest.Compression = CompressionGzip
	manifest.ChecksumSHA256 = SHA256Hex(data)
	if err := input.Store.Put(ctx, manifest, data); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// CompressGzip returns a gzip-compressed snapshot payload.
func CompressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		_ = zw.Close()
		return nil, fmt.Errorf("compress snapshot data: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("finish snapshot compression: %w", err)
	}
	return buf.Bytes(), nil
}

// Decompress returns the raw snapshot payload represented by manifest.
func Decompress(manifest Manifest, data []byte) ([]byte, error) {
	switch manifest.Compression {
	case "":
		return data, nil
	case CompressionGzip:
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("open snapshot gzip payload: %w", err)
		}
		defer func() {
			_ = zr.Close()
		}()
		raw, err := io.ReadAll(zr)
		if err != nil {
			return nil, fmt.Errorf("read snapshot gzip payload: %w", err)
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("unsupported snapshot compression %q", manifest.Compression)
	}
}
