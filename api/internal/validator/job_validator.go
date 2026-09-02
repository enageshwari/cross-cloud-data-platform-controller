package validator

import (
	"fmt"
	"strings"

	"github.com/enageshwari/cross-cloud-data-platform-controller/internal/model"
)

var (
	validEngines  = map[string]bool{"spark": true, "flink": true, "trino": true, "jupyter": true, "druid": true}
	validClouds   = map[string]bool{"aws": true, "gcp": true}
	validPriority = map[string]bool{"batch-low": true, "interactive-high": true}
)

// ValidateJobSubmission checks required fields and enumerated values.
// It does NOT perform OPA residency checks — those happen at admission time.
func ValidateJobSubmission(job *model.JobSubmission) error {
	if strings.TrimSpace(job.JobName) == "" {
		return fmt.Errorf("job_name is required")
	}
	engine := strings.ToLower(job.Engine)
	if !validEngines[engine] {
		return fmt.Errorf("engine %q is not supported; valid values: spark, flink, trino, jupyter, druid", job.Engine)
	}
	if !validClouds[strings.ToLower(job.TargetCloud)] {
		return fmt.Errorf("target_cloud %q is not supported; valid values: aws, gcp", job.TargetCloud)
	}
	if strings.TrimSpace(job.Region) == "" {
		return fmt.Errorf("region is required")
	}
	if strings.TrimSpace(job.ArtifactURI) == "" {
		return fmt.Errorf("artifact_uri is required")
	}
	// main_class is required for Spark custom JARs (not needed when using bundled demo jars,
	// but enforced here so users don't accidentally submit without it).
	if engine == "spark" && strings.TrimSpace(job.MainClass) == "" {
		return fmt.Errorf("main_class is required for spark engine (e.g. \"com.example.MyJob\")")
	}
	if strings.TrimSpace(job.InputPath) == "" {
		return fmt.Errorf("input_path is required")
	}
	if strings.TrimSpace(job.OutputPath) == "" {
		return fmt.Errorf("output_path is required")
	}
	if !validPriority[strings.ToLower(job.Priority)] {
		return fmt.Errorf("priority %q is not supported; valid values: batch-low, interactive-high", job.Priority)
	}
	return nil
}
