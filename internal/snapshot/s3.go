package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3Config configures an S3-compatible snapshot store.
type S3Config struct {
	Endpoint     string
	Bucket       string
	AccessKeyID  string
	SecretAccess string
	SessionToken string
	Region       string
	Secure       bool
}

// S3Store stores snapshots in an S3-compatible object store.
type S3Store struct {
	client objectClient
	bucket string
}

type objectClient interface {
	PutObject(ctx context.Context, bucketName string, objectName string, reader io.Reader, objectSize int64, contentType string) error
	GetObject(ctx context.Context, bucketName string, objectName string) ([]byte, error)
	StatObject(ctx context.Context, bucketName string, objectName string) error
}

type s3ObjectClient struct {
	client *s3.Client
}

// NewS3Store creates an S3-compatible snapshot store.
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}
	hasAccessKey := cfg.AccessKeyID != ""
	hasSecretAccess := cfg.SecretAccess != ""
	if hasAccessKey != hasSecretAccess {
		return nil, fmt.Errorf("s3 static credentials require both access key ID and secret access key")
	}

	loadOptions := []func(*config.LoadOptions) error{
		config.WithRegion(s3Region(cfg.Region)),
	}
	if hasAccessKey {
		loadOptions = append(loadOptions, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccess, cfg.SessionToken)))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load s3 config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(opts *s3.Options) {
		if cfg.Endpoint != "" {
			opts.BaseEndpoint = aws.String(s3Endpoint(cfg))
			opts.UsePathStyle = true
		}
	})
	return &S3Store{client: s3ObjectClient{client: client}, bucket: cfg.Bucket}, nil
}

// Put uploads a verified snapshot payload and manifest.
func (s *S3Store) Put(ctx context.Context, manifest Manifest, data []byte) error {
	if err := VerifySnapshot(manifest, data); err != nil {
		return err
	}

	manifestObject, dataObject := s.objectNames(manifest.IndexName, manifest.ShardID, manifest.ReplicaSourceNode, manifest.SnapshotGeneration)
	if err := s.client.PutObject(ctx, s.bucket, dataObject, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		return fmt.Errorf("put snapshot data object: %w", err)
	}

	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("marshal snapshot manifest: %w", err)
	}
	if err := s.client.PutObject(ctx, s.bucket, manifestObject, bytes.NewReader(manifestData), int64(len(manifestData)), "application/json"); err != nil {
		return fmt.Errorf("put snapshot manifest object: %w", err)
	}
	return nil
}

// Get downloads and verifies a snapshot payload and manifest.
func (s *S3Store) Get(ctx context.Context, indexName string, shardID int, replicaSourceNode string, generation int64) (Manifest, []byte, error) {
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
		if errors.Is(err, ErrSnapshotNotFound) || isObjectNotFound(err) {
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

func (s *S3Store) getObject(ctx context.Context, objectName string) ([]byte, error) {
	data, err := s.client.GetObject(ctx, s.bucket, objectName)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) || isObjectNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, objectName)
		}
		return nil, err
	}
	return data, nil
}

func isObjectNotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound"
}

func (c s3ObjectClient) PutObject(ctx context.Context, bucketName string, objectName string, reader io.Reader, objectSize int64, contentType string) error {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(objectName),
		Body:          reader,
		ContentLength: aws.Int64(objectSize),
		ContentType:   aws.String(contentType),
	})
	return err
}

func (c s3ObjectClient) GetObject(ctx context.Context, bucketName string, objectName string) ([]byte, error) {
	output, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = output.Body.Close()
	}()
	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c s3ObjectClient) StatObject(ctx context.Context, bucketName string, objectName string) error {
	_, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	})
	return err
}

func s3Region(region string) string {
	if region != "" {
		return region
	}
	return "us-east-1"
}

func s3Endpoint(cfg S3Config) string {
	if strings.HasPrefix(cfg.Endpoint, "http://") || strings.HasPrefix(cfg.Endpoint, "https://") {
		return cfg.Endpoint
	}
	scheme := "http"
	if cfg.Secure {
		scheme = "https"
	}
	return scheme + "://" + cfg.Endpoint
}

func (s *S3Store) objectNames(indexName string, shardID int, replicaSourceNode string, generation int64) (manifestObject string, dataObject string) {
	base := fmt.Sprintf("snapshots/%s/shard-%d/%s/generation-%06d", indexName, shardID, replicaSourceNode, generation)
	return base + "/" + manifestFile, base + "/" + snapshotFile
}
