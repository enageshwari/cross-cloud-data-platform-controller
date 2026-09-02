package spark

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

// SparkApplicationGVR is the GroupVersionResource for Spark Operator's SparkApplication CRD.
var SparkApplicationGVR = schema.GroupVersionResource{
	Group:    "sparkoperator.k8s.io",
	Version:  "v1beta2",
	Resource: "sparkapplications",
}

// Config holds the settings for submitting SparkApplication CRDs to a cluster.
type Config struct {
	// KubeconfigPath is the path to the kubeconfig file.
	// Defaults to ~/.kube/config when empty.
	KubeconfigPath string
	// KubeContext is the kubeconfig context name for the target cluster.
	KubeContext string
	// TargetCloud identifies the cloud the cluster runs on: "aws" or "gcp".
	// Controls which sparkConf credentials block is injected.
	TargetCloud string
	// Namespace where SparkApplication CRDs are submitted.
	Namespace string
	// SparkImage is the Spark container image URI used for driver/executors.
	SparkImage string
	// DriverServiceAccount is the K8s SA used by the driver pod.
	DriverServiceAccount string
	// ExecutorServiceAccount is the K8s SA used by executor pods.
	ExecutorServiceAccount string
	// CoreRequest is the CPU request for driver and executor pods (e.g. "500m", "1").
	// Defaults to "1" when empty.
	CoreRequest string
}

// Submitter submits SparkApplication CRDs to a Kubernetes cluster.
type Submitter struct {
	client dynamic.Interface
	cfg    Config
}

// New constructs a Submitter connected to the target cluster.
func New(cfg Config) (*Submitter, error) {
	if cfg.KubeContext == "" {
		return nil, fmt.Errorf("spark submitter: KubeContext must be set")
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if cfg.KubeconfigPath != "" {
		loadingRules.ExplicitPath = cfg.KubeconfigPath
	}
	restCfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{CurrentContext: cfg.KubeContext},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("spark submitter: failed to build kubeconfig: %w", err)
	}

	dynClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("spark submitter: failed to create dynamic client: %w", err)
	}

	return &Submitter{client: dynClient, cfg: cfg}, nil
}

// JobSpec holds the per-job parameters for a SparkApplication.
type JobSpec struct {
	JobID       string
	JobName     string
	ArtifactURI string   // JAR URI (s3://, gs://, local:///) — used as mainApplicationFile
	MainClass   string   // Fully-qualified Spark entry class
	JobArgs     []string // Optional positional args passed to the Spark job
	InputPath   string
	OutputPath  string
	Priority    string // "batch-low" | "interactive-high"
}

// Submit creates a SparkApplication CRD on the target cluster.
// Returns the name of the created SparkApplication resource.
func (s *Submitter) Submit(ctx context.Context, spec JobSpec) (string, error) {
	// Map priority to Kueue local queue name and priority class.
	queue := "batch-data-queue"
	priorityClass := "batch-low"
	if spec.Priority == "interactive-high" {
		queue = "interactive-data-queue"
		priorityClass = "interactive-high"
	}

	coreRequest := s.cfg.CoreRequest
	if coreRequest == "" {
		coreRequest = "1"
	}

	appName := spec.JobName + "-" + spec.JobID[4:18]

	// Resolve JAR URI and main class — use user-provided values with fallback to SparkPi demo.
	mainAppFile := spec.ArtifactURI
	if mainAppFile == "" {
		mainAppFile = "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar"
	}
	mainClass := spec.MainClass
	if mainClass == "" {
		mainClass = "org.apache.spark.examples.SparkPi"
	}

	// Build job arguments — convert []string to []interface{} for unstructured.
	var jobArgs []interface{}
	if len(spec.JobArgs) > 0 {
		for _, a := range spec.JobArgs {
			jobArgs = append(jobArgs, a)
		}
	} else if mainClass == "org.apache.spark.examples.SparkPi" {
		jobArgs = []interface{}{"100"} // default iterations for demo
	}

	// Cloud-specific sparkConf: S3A + IRSA for AWS, GCS connector + Workload Identity for GCP.
	sparkConf := map[string]interface{}{
		"spark.app.input.path":  spec.InputPath,
		"spark.app.output.path": spec.OutputPath,
	}
	targetCloud := s.cfg.TargetCloud
	if targetCloud == "" {
		targetCloud = "aws"
	}
	switch targetCloud {
	case "aws":
		sparkConf["spark.hadoop.fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"
		sparkConf["spark.hadoop.fs.s3a.aws.credentials.provider"] = "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
	case "gcp":
		// GCS connector is bundled in apache/spark:3.5.3 via hadoop-gcs shaded jar.
		// Workload Identity provides credentials automatically via the annotated K8s SA.
		sparkConf["spark.hadoop.fs.gs.impl"] = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
		sparkConf["spark.hadoop.fs.AbstractFileSystem.gs.impl"] = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
		sparkConf["spark.hadoop.google.cloud.auth.type"] = "APPLICATION_DEFAULT"
	}

	app := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "sparkoperator.k8s.io/v1beta2",
			"kind":       "SparkApplication",
			"metadata": map[string]interface{}{
				"name":      appName,
				"namespace": s.cfg.Namespace,
				"labels": map[string]interface{}{
					"job-id":                              spec.JobID,
					"job-name":                            spec.JobName,
					"target-cloud":                        targetCloud,
					"region":                              extractRegion(spec.OutputPath),
					"kueue.x-k8s.io/queue-name":          queue,
					"kueue.x-k8s.io/priority-class":      priorityClass,
				},
			},
			"spec": map[string]interface{}{
				"type":                "Scala",
				"mode":                "cluster",
				"image":               s.cfg.SparkImage,
				"imagePullPolicy":     "IfNotPresent",
				"mainClass":           mainClass,
				"mainApplicationFile": mainAppFile,
				"arguments":           jobArgs,
				"sparkVersion":        "3.5.3",
				"sparkConf":           sparkConf,
				"driver": map[string]interface{}{
					"cores":          int64(1),
					"coreRequest":    coreRequest,
					"memory":         "512m",
					"serviceAccount": s.cfg.DriverServiceAccount,
					"labels": map[string]interface{}{
						"version": "3.5.3",
					},
				},
				"executor": map[string]interface{}{
					"cores":          int64(1),
					"coreRequest":    coreRequest,
					"instances":      int64(1),
					"memory":         "512m",
					"serviceAccount": s.cfg.ExecutorServiceAccount,
					"labels": map[string]interface{}{
						"version": "3.5.3",
					},
				},
				"restartPolicy": map[string]interface{}{
					"type": "Never",
				},
			},
		},
	}

	_, err := s.client.Resource(SparkApplicationGVR).
		Namespace(s.cfg.Namespace).
		Create(ctx, app, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("spark submitter: failed to create SparkApplication %q: %w", appName, err)
	}
	return appName, nil
}

// extractRegion infers the cloud region from the output path by matching known region tokens.
func extractRegion(outputPath string) string {
	regions := []string{
		"us-west-1", "us-west-2", "us-east-1", "us-east-2", "eu-west-1",
		"us-west2", "us-central1", "europe-west1",
	}
	for _, r := range regions {
		for i := 0; i <= len(outputPath)-len(r); i++ {
			if outputPath[i:i+len(r)] == r {
				return r
			}
		}
	}
	return "us-west-1"
}

// WaitForCompletion polls the SparkApplication status until terminal state or timeout.
// Used in tests — production jobs are fire-and-forget from the API perspective.
func (s *Submitter) WaitForCompletion(ctx context.Context, name string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		obj, err := s.client.Resource(SparkApplicationGVR).
			Namespace(s.cfg.Namespace).
			Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		state, _, _ := unstructured.NestedString(obj.Object, "status", "applicationState", "state")
		if state == "COMPLETED" || state == "FAILED" || state == "SUBMISSION_FAILED" {
			return state, nil
		}
		time.Sleep(10 * time.Second)
	}
	return "TIMEOUT", nil
}
