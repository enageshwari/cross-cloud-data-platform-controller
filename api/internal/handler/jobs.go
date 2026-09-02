package handler

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/enageshwari/cross-cloud-data-platform-controller/internal/flink"
	"github.com/enageshwari/cross-cloud-data-platform-controller/internal/model"
	"github.com/enageshwari/cross-cloud-data-platform-controller/internal/presigner"
	"github.com/enageshwari/cross-cloud-data-platform-controller/internal/spark"
	"github.com/enageshwari/cross-cloud-data-platform-controller/internal/validator"
)

// JobsHandler handles job submission requests.
type JobsHandler struct {
	logger      *slog.Logger
	s3Presign   *presigner.S3Presigner
	gcsPresign  *presigner.GCSPresigner
	sparkSubAWS *spark.Submitter // EKS Spark; nil when not configured
	sparkSubGCP *spark.Submitter // GKE Spark; nil when not configured
	flinkSubAWS *flink.Submitter // EKS Flink; nil when not configured
	flinkSubGCP *flink.Submitter // GKE Flink; nil when not configured
}

// NewJobsHandler constructs a handler with structured JSON logging.
func NewJobsHandler(
	logger *slog.Logger,
	s3p *presigner.S3Presigner,
	gcsp *presigner.GCSPresigner,
	awsSpark *spark.Submitter,
	gcpSpark *spark.Submitter,
	awsFlink *flink.Submitter,
	gcpFlink *flink.Submitter,
) *JobsHandler {
	return &JobsHandler{
		logger:      logger,
		s3Presign:   s3p,
		gcsPresign:  gcsp,
		sparkSubAWS: awsSpark,
		sparkSubGCP: gcpSpark,
		flinkSubAWS: awsFlink,
		flinkSubGCP: gcpFlink,
	}
}

// Submit handles POST /api/v1/jobs.
//   - AWS Spark jobs: generates S3 presigned URL + dispatches SparkApplication CRD to EKS
//   - GCP jobs:       generates GCS signed URL
func (h *JobsHandler) Submit(w http.ResponseWriter, r *http.Request) {
	var job model.JobSubmission

	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON payload: "+err.Error())
		return
	}

	if err := validator.ValidateJobSubmission(&job); err != nil {
		h.writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())

	h.logger.Info("job accepted",
		slog.String("job_id", jobID),
		slog.String("job_name", job.JobName),
		slog.String("engine", job.Engine),
		slog.String("target_cloud", job.TargetCloud),
		slog.String("region", job.Region),
		slog.String("priority", job.Priority),
	)

	resp := model.JobResponse{
		JobID:   jobID,
		Status:  "accepted",
		Message: "Job queued for scheduling. OPA admission and Kueue placement will proceed asynchronously.",
	}

	objectKey := fmt.Sprintf("jobs/%s/%s/output", jobID, job.JobName)

	switch strings.ToLower(job.TargetCloud) {
	case "aws":
		// Step 1: generate pre-signed S3 PUT URL for output.
		if h.s3Presign == nil {
			h.writeError(w, http.StatusInternalServerError, "S3 presigner not configured")
			return
		}
		result, err := h.s3Presign.PresignPut(r.Context(), objectKey)
		if err != nil {
			h.logger.Error("S3 presign failed", slog.String("job_id", jobID), slog.String("error", err.Error()))
			h.writeError(w, http.StatusInternalServerError, "failed to generate S3 output credential: "+err.Error())
			return
		}
		resp.OutputCred = &model.OutputCredential{
			Type:      "presigned_put_url",
			URL:       result.URL,
			ObjectKey: result.ObjectKey,
			Bucket:    result.Bucket,
			Region:    result.Region,
			ExpiresAt: result.ExpiresAt.Format(time.RFC3339),
		}
		h.logger.Info("S3 presigned URL generated",
			slog.String("job_id", jobID),
			slog.String("object_key", objectKey),
		)

		// Step 2: dispatch SparkApplication CRD to EKS (Spark engine only).
		if strings.ToLower(job.Engine) == "spark" && h.sparkSubAWS != nil {
			appName, err := h.sparkSubAWS.Submit(r.Context(), spark.JobSpec{
				JobID:       jobID,
				JobName:     job.JobName,
				ArtifactURI: job.ArtifactURI,
				MainClass:   job.MainClass,
				JobArgs:     job.JobArgs,
				InputPath:   job.InputPath,
				OutputPath:  job.OutputPath,
				Priority:    job.Priority,
			})
			if err != nil {
				h.logger.Error("SparkApplication dispatch failed (EKS)",
					slog.String("job_id", jobID),
					slog.String("error", err.Error()),
				)
				resp.Message = "Job accepted. SparkApplication dispatch failed: " + err.Error()
			} else {
				resp.SparkApplication = appName
				h.logger.Info("SparkApplication dispatched to EKS",
					slog.String("job_id", jobID),
					slog.String("spark_app", appName),
				)
			}
		}

		// Step 3: dispatch Flink AppWrapper to EKS (Flink engine only).
		if strings.ToLower(job.Engine) == "flink" && h.flinkSubAWS != nil {
			awName, err := h.flinkSubAWS.Submit(r.Context(), flink.JobSpec{
				JobID:       jobID,
				JobName:     job.JobName,
				JarURI:      job.ArtifactURI,
				EntryClass:  job.MainClass,
				JobArgs:     job.JobArgs,
				Parallelism: job.Parallelism,
				Priority:    job.Priority,
			})
			if err != nil {
				h.logger.Error("Flink AppWrapper dispatch failed (EKS)",
					slog.String("job_id", jobID),
					slog.String("error", err.Error()),
				)
				resp.Message = "Job accepted. Flink dispatch failed: " + err.Error()
			} else {
				resp.SparkApplication = awName // reuse field — represents the dispatched workload name
				h.logger.Info("Flink AppWrapper dispatched to EKS",
					slog.String("job_id", jobID),
					slog.String("appwrapper", awName),
				)
			}
		}

	case "gcp":
		if h.gcsPresign == nil {
			h.writeError(w, http.StatusInternalServerError, "GCS presigner not configured")
			return
		}
		result, err := h.gcsPresign.PresignPut(r.Context(), objectKey)
		if err != nil {
			h.logger.Error("GCS presign failed", slog.String("job_id", jobID), slog.String("error", err.Error()))
			h.writeError(w, http.StatusInternalServerError, "failed to generate GCS output credential: "+err.Error())
			return
		}
		resp.OutputCred = &model.OutputCredential{
			Type:      "signed_put_url",
			URL:       result.URL,
			ObjectKey: result.ObjectKey,
			Bucket:    result.Bucket,
			Region:    "us-west2",
			ExpiresAt: result.ExpiresAt.Format(time.RFC3339),
		}
		h.logger.Info("GCS signed URL generated",
			slog.String("job_id", jobID),
			slog.String("object_key", objectKey),
		)

		// Dispatch SparkApplication CRD to GKE (Spark engine only).
		if strings.ToLower(job.Engine) == "spark" && h.sparkSubGCP != nil {
			appName, err := h.sparkSubGCP.Submit(r.Context(), spark.JobSpec{
				JobID:       jobID,
				JobName:     job.JobName,
				ArtifactURI: job.ArtifactURI,
				MainClass:   job.MainClass,
				JobArgs:     job.JobArgs,
				InputPath:   job.InputPath,
				OutputPath:  job.OutputPath,
				Priority:    job.Priority,
			})
			if err != nil {
				h.logger.Error("SparkApplication dispatch failed (GKE)",
					slog.String("job_id", jobID),
					slog.String("error", err.Error()),
				)
				resp.Message = "Job accepted. SparkApplication dispatch failed: " + err.Error()
			} else {
				resp.SparkApplication = appName
				h.logger.Info("SparkApplication dispatched to GKE",
					slog.String("job_id", jobID),
					slog.String("spark_app", appName),
				)
			}
		}

		// Dispatch Flink AppWrapper to GKE (Flink engine only).
		if strings.ToLower(job.Engine) == "flink" && h.flinkSubGCP != nil {
			awName, err := h.flinkSubGCP.Submit(r.Context(), flink.JobSpec{
				JobID:       jobID,
				JobName:     job.JobName,
				JarURI:      job.ArtifactURI,
				EntryClass:  job.MainClass,
				JobArgs:     job.JobArgs,
				Parallelism: job.Parallelism,
				Priority:    job.Priority,
			})
			if err != nil {
				h.logger.Error("Flink AppWrapper dispatch failed (GKE)",
					slog.String("job_id", jobID),
					slog.String("error", err.Error()),
				)
				resp.Message = "Job accepted. Flink dispatch failed: " + err.Error()
			} else {
				resp.SparkApplication = awName
				h.logger.Info("Flink AppWrapper dispatched to GKE",
					slog.String("job_id", jobID),
					slog.String("appwrapper", awName),
				)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *JobsHandler) writeError(w http.ResponseWriter, code int, msg string) {
	h.logger.Warn("job rejected", slog.Int("status", code), slog.String("reason", msg))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(model.ErrorResponse{Code: code, Message: msg})
}
