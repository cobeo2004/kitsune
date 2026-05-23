package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinIOConfig configures an S3-compatible snapshot store.
type MinIOConfig struct {
	Endpoint     string
	Bucket       string
	AccessKeyID  string
	SecretAccess string
	SessionToken string
	Region       string
	Secure       bool
}

// MinIOStore stores snapshots in MinIO or another S3-compatible object store.
type MinIOStore struct {
	client objectClient
	bucket string
}

type objectClient interface {
	PutObject(ctx context.Context, bucketName string, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) error
	GetObject(ctx context.Context, bucketName string, objectName string) ([]byte, error)
	StatObject(ctx context.Context, bucketName string, objectName string) error
}

type minioObjectClient struct {
	client *minio.Client
}

// NewMinIOStore creates a MinIO-backed snapshot store.
func NewMinIOStore(cfg MinIOConfig) (*MinIOStore, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("minio endpoint is required")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("minio bucket is required")
	}
	if cfg.AccessKeyID == "" {
		return nil, fmt.Errorf("minio access key ID is required")
	}
	if cfg.SecretAccess == "" {
		return nil, fmt.Errorf("minio secret access key is required")
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccess, cfg.SessionToken),
		Secure: cfg.Secure,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}
	return &MinIOStore{client: minioObjectClient{client: client}, bucket: cfg.Bucket}, nil
}

// Put uploads a verified snapshot payload and manifest.
func (s *MinIOStore) Put(ctx context.Context, manifest Manifest, data []byte) error {
	if err := VerifySnapshot(manifest, data); err != nil {
		return err
	}

	manifestObject, dataObject := s.objectNames(manifest.IndexName, manifest.ShardID, manifest.ReplicaSourceNode, manifest.SnapshotGeneration)
	if err := s.client.PutObject(ctx, s.bucket, dataObject, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "application/octet-stream"}); err != nil {
		return fmt.Errorf("put snapshot data object: %w", err)
	}

	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal snapshot manifest: %w", err)
	}
	if err := s.client.PutObject(ctx, s.bucket, manifestObject, bytes.NewReader(manifestData), int64(len(manifestData)), minio.PutObjectOptions{ContentType: "application/json"}); err != nil {
		return fmt.Errorf("put snapshot manifest object: %w", err)
	}
	return nil
}

// Get downloads and verifies a snapshot payload and manifest.
func (s *MinIOStore) Get(ctx context.Context, indexName string, shardID int, replicaSourceNode string, generation int64) (Manifest, []byte, error) {
	manifestObject, dataObject := s.objectNames(indexName, shardID, replicaSourceNode, generation)

	manifestData, err := s.getObject(ctx, manifestObject)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return Manifest{}, nil, err
		}
		return Manifest{}, nil, fmt.Errorf("get snapshot manifest object: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return Manifest{}, nil, fmt.Errorf("decode snapshot manifest: %w", err)
	}

	if err := s.client.StatObject(ctx, s.bucket, dataObject); err != nil {
		if errors.Is(err, ErrSnapshotNotFound) || minioErrorCode(err) == minio.NoSuchKey {
			return Manifest{}, nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, dataObject)
		}
		return Manifest{}, nil, fmt.Errorf("stat snapshot data object: %w", err)
	}
	data, err := s.getObject(ctx, dataObject)
	if err != nil {
		return Manifest{}, nil, fmt.Errorf("get snapshot data object: %w", err)
	}
	if err := VerifySnapshot(manifest, data); err != nil {
		return Manifest{}, nil, err
	}
	return manifest, data, nil
}

func (s *MinIOStore) getObject(ctx context.Context, objectName string) ([]byte, error) {
	data, err := s.client.GetObject(ctx, s.bucket, objectName)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) || minioErrorCode(err) == minio.NoSuchKey {
			return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, objectName)
		}
		return nil, err
	}
	return data, nil
}

func minioErrorCode(err error) string {
	return minio.ToErrorResponse(err).Code
}

func (c minioObjectClient) PutObject(ctx context.Context, bucketName string, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) error {
	_, err := c.client.PutObject(ctx, bucketName, objectName, reader, objectSize, opts)
	return err
}

func (c minioObjectClient) GetObject(ctx context.Context, bucketName string, objectName string) ([]byte, error) {
	object, err := c.client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = object.Close()
	}()
	data, err := io.ReadAll(object)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c minioObjectClient) StatObject(ctx context.Context, bucketName string, objectName string) error {
	_, err := c.client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
	return err
}

func (s *MinIOStore) objectNames(indexName string, shardID int, replicaSourceNode string, generation int64) (manifestObject string, dataObject string) {
	base := fmt.Sprintf("snapshots/%s/shard-%d/%s/generation-%06d", indexName, shardID, replicaSourceNode, generation)
	return base + "/" + manifestFile, base + "/" + snapshotFile
}
