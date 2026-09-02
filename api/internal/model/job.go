package model

// JobSubmission represents the payload accepted by POST /api/v1/jobs.
// Fields map directly to the LLD schema defined in the HLD spec.
type JobSubmission struct {
	JobName     string   `json:"job_name"`
	Engine      string   `json:"engine"`       // "spark" | "flink" | "trino" | "jupyter" | "druid"
	TargetCloud string   `json:"target_cloud"` // "aws" | "gcp"
	Region      string   `json:"region"`       // e.g. "us-west-2", "us-central1"
	ArtifactURI string   `json:"artifact_uri"` // JAR URI (s3://, gs://, local:///) or container image for non-JVM engines
	MainClass   string   `json:"main_class,omitempty"`  // Spark: fully-qualified entry class; Flink: job entry class (optional)
	JobArgs     []string `json:"job_args,omitempty"`    // Optional positional arguments passed to the job
	Parallelism int      `json:"parallelism,omitempty"` // Flink job parallelism (default: 2)
	InputPath   string   `json:"input_path"`   // public S3 or GCS dataset URI
	OutputPath  string   `json:"output_path"`  // region-locked destination bucket
	Priority    string   `json:"priority"`     // "batch-low" | "interactive-high"
}

// JobResponse is returned after successful job acceptance.
type JobResponse struct {
	JobID            string            `json:"job_id"`
	Status           string            `json:"status"`
	Message          string            `json:"message"`
	OutputCred       *OutputCredential `json:"output_credential,omitempty"`
	SparkApplication string            `json:"spark_application,omitempty"` // name of submitted SparkApplication CRD
}

// OutputCredential carries the pre-signed URL the job worker uses to write
// output data to S3 without holding any long-lived AWS credentials.
// Only populated for AWS target_cloud jobs.
type OutputCredential struct {
	Type      string `json:"type"`       // always "presigned_put_url"
	URL       string `json:"url"`        // HTTP PUT to this URL to upload the output object
	ObjectKey string `json:"object_key"` // S3 key the URL targets
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	ExpiresAt string `json:"expires_at"` // RFC3339 UTC
}

// ErrorResponse is the standard error envelope.
type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
