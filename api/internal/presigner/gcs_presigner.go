package presigner

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/option"
)

// GCSConfig holds settings for generating GCS signed URLs.
// Locally: uses ADC (gcloud auth application-default login) with SA impersonation.
// On GKE:  uses Workload Identity — no key file needed.
type GCSConfig struct {
	// ServiceAccountEmail is the SA whose identity signs URLs via IAM SignBlob.
	// The ADC principal must have roles/iam.serviceAccountTokenCreator on this SA.
	ServiceAccountEmail string
	Bucket              string
	TTL                 time.Duration
}

// GCSPresigner generates time-limited V4 signed PUT URLs for GCS objects.
// Signing is done via the IAM Credentials API (SignBlob) using ADC — no
// service account key file is ever downloaded or stored.
type GCSPresigner struct {
	client *storage.Client
	iamSvc *iamcredentials.Service
	cfg    GCSConfig
}

// NewGCSPresigner constructs a GCSPresigner using Application Default Credentials.
// Locally: set up via `gcloud auth application-default login`.
// On GKE:  automatically provided by Workload Identity.
func NewGCSPresigner(ctx context.Context, cfg GCSConfig) (*GCSPresigner, error) {
	if cfg.ServiceAccountEmail == "" {
		return nil, fmt.Errorf("gcs presigner: GCS_PRESIGNER_SA must be set")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("gcs presigner: GCS_OUTPUT_BUCKET must be set")
	}
	if cfg.TTL == 0 {
		cfg.TTL = 15 * time.Minute
	}

	// Load ADC — works locally (gcloud auth application-default login)
	// and on GKE (Workload Identity). No key file involved.
	creds, err := google.FindDefaultCredentials(ctx,
		"https://www.googleapis.com/auth/devstorage.read_write",
		"https://www.googleapis.com/auth/iam",
	)
	if err != nil {
		return nil, fmt.Errorf("gcs presigner: failed to load ADC credentials: %w", err)
	}

	// GCS client — used for bucket/object operations and for computing the
	// signed URL structure; actual signing is delegated to iamSvc.SignBlob.
	storageClient, err := storage.NewClient(ctx, option.WithTokenSource(creds.TokenSource))
	if err != nil {
		return nil, fmt.Errorf("gcs presigner: failed to create storage client: %w", err)
	}

	// IAM Credentials client — calls projects.serviceAccounts.signBlob on
	// behalf of ServiceAccountEmail using the ADC token.
	iamSvc, err := iamcredentials.NewService(ctx, option.WithTokenSource(creds.TokenSource))
	if err != nil {
		storageClient.Close()
		return nil, fmt.Errorf("gcs presigner: failed to create IAM credentials client: %w", err)
	}

	return &GCSPresigner{client: storageClient, iamSvc: iamSvc, cfg: cfg}, nil
}

// GCSPresignResult holds the generated signed URL and metadata.
type GCSPresignResult struct {
	URL       string
	ExpiresAt time.Time
	ObjectKey string
	Bucket    string
}

// PresignPut generates a V4 signed HTTP PUT URL for the given object key.
// The URL is signed via IAM SignBlob — the worker pod needs no credentials.
func (p *GCSPresigner) PresignPut(ctx context.Context, objectKey string) (*GCSPresignResult, error) {
	saResource := "projects/-/serviceAccounts/" + p.cfg.ServiceAccountEmail

	opts := &storage.SignedURLOptions{
		Scheme:         storage.SigningSchemeV4,
		Method:         "PUT",
		Expires:        time.Now().Add(p.cfg.TTL),
		GoogleAccessID: p.cfg.ServiceAccountEmail,
		// SignBytes delegates signing to IAM SignBlob API using ADC.
		// The response is base64-encoded; storage SDK expects raw bytes.
		SignBytes: func(b []byte) ([]byte, error) {
			req := &iamcredentials.SignBlobRequest{Payload: base64.StdEncoding.EncodeToString(b)}
			resp, err := p.iamSvc.Projects.ServiceAccounts.
				SignBlob(saResource, req).Context(ctx).Do()
			if err != nil {
				return nil, fmt.Errorf("IAM SignBlob failed: %w", err)
			}
			decoded, err := base64.StdEncoding.DecodeString(resp.SignedBlob)
			if err != nil {
				return nil, fmt.Errorf("failed to decode SignBlob response: %w", err)
			}
			return decoded, nil
		},
	}

	url, err := p.client.Bucket(p.cfg.Bucket).SignedURL(objectKey, opts)
	if err != nil {
		return nil, fmt.Errorf("gcs presigner: failed to generate signed URL for key %q: %w", objectKey, err)
	}

	return &GCSPresignResult{
		URL:       url,
		ExpiresAt: time.Now().UTC().Add(p.cfg.TTL),
		ObjectKey: objectKey,
		Bucket:    p.cfg.Bucket,
	}, nil
}

// Close releases the underlying GCS client resources.
func (p *GCSPresigner) Close() error {
	return p.client.Close()
}
