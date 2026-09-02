package presigner

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config holds the credentials and settings needed to generate pre-signed URLs.
// In production (GKE deployment) these values come from GCP Secret Manager.
// Locally they are loaded from the .env file via godotenv.
type Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Bucket          string
	TTL             time.Duration // how long the pre-signed URL remains valid
}

// S3Presigner generates time-limited pre-signed PUT URLs for S3 objects.
// The pre-signed URL is injected into the job response so the GKE worker pod
// can write output directly to S3 without holding any long-lived AWS credentials.
type S3Presigner struct {
	client *s3.PresignClient
	cfg    Config
}

// New constructs an S3Presigner using explicit credentials from cfg.
// It does NOT fall back to the ambient AWS credential chain — the presigner
// identity must always be the dedicated cross-cloud-presigner IAM user.
func New(cfg Config) (*S3Presigner, error) {
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("presigner: AWS_PRESIGNER_ACCESS_KEY_ID and AWS_PRESIGNER_SECRET_ACCESS_KEY must be set")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("presigner: AWS_PRESIGNER_REGION must be set")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("presigner: AWS_OUTPUT_BUCKET must be set")
	}
	if cfg.TTL == 0 {
		cfg.TTL = 15 * time.Minute
	}

	awsCfg := aws.Config{
		Region: cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"", // session token — not needed for IAM user keys
		),
	}

	s3Client := s3.NewFromConfig(awsCfg)
	presignClient := s3.NewPresignClient(s3Client)

	return &S3Presigner{client: presignClient, cfg: cfg}, nil
}

// PresignPutResult holds the generated URL and its expiry time.
type PresignPutResult struct {
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at"`
	ObjectKey string    `json:"object_key"`
	Bucket    string    `json:"bucket"`
	Region    string    `json:"region"`
}

// PresignPut generates a pre-signed HTTP PUT URL for the given object key.
// The caller (job worker) uses this URL to upload output data directly to S3
// without needing any AWS credentials of its own.
func (p *S3Presigner) PresignPut(ctx context.Context, objectKey string) (*PresignPutResult, error) {
	req, err := p.client.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(p.cfg.Bucket),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(p.cfg.TTL))
	if err != nil {
		return nil, fmt.Errorf("presigner: failed to generate pre-signed URL for key %q: %w", objectKey, err)
	}

	return &PresignPutResult{
		URL:       req.URL,
		ExpiresAt: time.Now().UTC().Add(p.cfg.TTL),
		ObjectKey: objectKey,
		Bucket:    p.cfg.Bucket,
		Region:    p.cfg.Region,
	}, nil
}
