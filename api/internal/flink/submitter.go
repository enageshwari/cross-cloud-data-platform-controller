// Package flink submits Flink workloads to a Kubernetes cluster via AppWrapper CRDs.
// AppWrapper provides Kueue-native admission control without requiring Kueue-specific
// support inside the Flink Kubernetes Operator itself.
package flink

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

// AppWrapperGVR is the GroupVersionResource for the AppWrapper CRD.
var AppWrapperGVR = schema.GroupVersionResource{
	Group:    "workload.codeflare.dev",
	Version:  "v1beta2",
	Resource: "appwrappers",
}

// Config holds the settings for submitting Flink jobs wrapped in AppWrappers.
type Config struct {
	// KubeconfigPath is the path to the kubeconfig file. Defaults to ~/.kube/config when empty.
	KubeconfigPath string
	// KubeContext is the kubeconfig context name for the target cluster.
	KubeContext string
	// TargetCloud identifies the cloud: "aws" or "gcp".
	TargetCloud string
	// Namespace where AppWrapper and FlinkDeployment CRDs are submitted.
	Namespace string
	// FlinkImage is the Flink container image. Defaults to "apache/flink:1.20".
	FlinkImage string
	// ServiceAccount is the K8s SA used by Flink jobManager and taskManager pods.
	ServiceAccount string
	// CPURequest is the CPU request for jobManager and taskManager pods (e.g. "500m", "1").
	// Defaults to "1" when empty.
	CPURequest string
	// MemoryRequest is the memory for jobManager and taskManager pods (e.g. "1024m").
	// Defaults to "1024m" when empty.
	MemoryRequest string
}

// Submitter submits Flink jobs as AppWrapper-wrapped FlinkDeployment CRDs.
type Submitter struct {
	client dynamic.Interface
	cfg    Config
}

// New constructs a Submitter connected to the target cluster.
func New(cfg Config) (*Submitter, error) {
	if cfg.KubeContext == "" {
		return nil, fmt.Errorf("flink submitter: KubeContext must be set")
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
		return nil, fmt.Errorf("flink submitter: failed to build kubeconfig: %w", err)
	}
	dynClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("flink submitter: failed to create dynamic client: %w", err)
	}
	return &Submitter{client: dynClient, cfg: cfg}, nil
}

// JobSpec holds the per-job parameters for a Flink AppWrapper.
type JobSpec struct {
	JobID       string
	JobName     string
	JarURI      string   // JAR URI (s3://, gs://, local:///) — used as FlinkDeployment job.jarURI
	EntryClass  string   // Optional Flink entry class (leave empty for default in JAR manifest)
	JobArgs     []string // Optional positional args passed to the Flink job
	Parallelism int      // Flink job parallelism (default: 2)
	Priority    string   // "batch-low" | "interactive-high"
}

// Submit creates an AppWrapper wrapping a FlinkDeployment on the target cluster.
// Returns the name of the created AppWrapper resource.
func (s *Submitter) Submit(ctx context.Context, spec JobSpec) (string, error) {
	// Map priority to Kueue local queue name and priority class.
	queue := "batch-data-queue"
	priorityClass := "batch-low"
	if spec.Priority == "interactive-high" {
		queue = "interactive-data-queue"
		priorityClass = "interactive-high"
	}

	flinkImage := s.cfg.FlinkImage
	if flinkImage == "" {
		flinkImage = "apache/flink:1.20"
	}
	cpuRequest := s.cfg.CPURequest
	if cpuRequest == "" {
		cpuRequest = "1"
	}
	memRequest := s.cfg.MemoryRequest
	if memRequest == "" {
		memRequest = "1024m"
	}
	memRequestMi := memRequest
	// Convert "1024m" → "1024Mi" for K8s resource requests (different units).
	if len(memRequest) > 1 && memRequest[len(memRequest)-1] == 'm' {
		memRequestMi = memRequest[:len(memRequest)-1] + "Mi"
	}

	sa := s.cfg.ServiceAccount
	if sa == "" {
		sa = "flink"
	}

	// Resolve JAR URI, entry class, args, and parallelism — user values with demo fallback.
	jarURI := spec.JarURI
	if jarURI == "" {
		jarURI = "local:///opt/flink/examples/streaming/WordCount.jar"
	}
	parallelism := spec.Parallelism
	if parallelism <= 0 {
		parallelism = 2
	}

	// Build job spec — only include entryClass and args if provided.
	jobSpec := map[string]interface{}{
		"jarURI":      jarURI,
		"parallelism": int64(parallelism),
		"upgradeMode": "stateless",
		"state":       "running",
	}
	if spec.EntryClass != "" {
		jobSpec["entryClass"] = spec.EntryClass
	}
	if len(spec.JobArgs) > 0 {
		args := make([]interface{}, len(spec.JobArgs))
		for i, a := range spec.JobArgs {
			args[i] = a
		}
		jobSpec["args"] = args
	}

	// AppWrapper name: truncate to ensure it's a valid K8s name (<= 63 chars).
	awName := spec.JobName + "-" + spec.JobID[4:18]

	podTemplate := map[string]interface{}{
		"spec": map[string]interface{}{
			"containers": []interface{}{
				map[string]interface{}{
					"name": "flink-main-container",
					"resources": map[string]interface{}{
						"requests": map[string]interface{}{
							"cpu":    cpuRequest,
							"memory": memRequestMi,
						},
					},
				},
			},
		},
	}

	flinkDeployment := map[string]interface{}{
		"apiVersion": "flink.apache.org/v1beta1",
		"kind":       "FlinkDeployment",
		"metadata": map[string]interface{}{
			"name":      awName,
			"namespace": s.cfg.Namespace,
		},
		"spec": map[string]interface{}{
			"image":        flinkImage,
			"flinkVersion": "v1_20",
			"flinkConfiguration": map[string]interface{}{
				"taskmanager.numberOfTaskSlots": "2",
			},
			"serviceAccount": sa,
			"jobManager": map[string]interface{}{
				"resource": map[string]interface{}{
					"memory": memRequest,
					"cpu":    0.5,
				},
				"podTemplate": podTemplate,
			},
			"taskManager": map[string]interface{}{
				"resource": map[string]interface{}{
					"memory": memRequest,
					"cpu":    0.5,
				},
				"podTemplate": podTemplate,
			},
			"job": jobSpec,
		},
	}

	appWrapper := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "workload.codeflare.dev/v1beta2",
			"kind":       "AppWrapper",
			"metadata": map[string]interface{}{
				"name":      awName,
				"namespace": s.cfg.Namespace,
				"labels": map[string]interface{}{
					"job-id":                         spec.JobID,
					"job-name":                       spec.JobName,
					"target-cloud":                   s.cfg.TargetCloud,
					"kueue.x-k8s.io/queue-name":     queue,
					"kueue.x-k8s.io/priority-class": priorityClass,
				},
			},
			"spec": map[string]interface{}{
				"components": []interface{}{
					map[string]interface{}{
						"podSets": []interface{}{
							map[string]interface{}{
								"path":     "template.spec.jobManager.podTemplate",
								"replicas": int64(1),
							},
							map[string]interface{}{
								"path":     "template.spec.taskManager.podTemplate",
								"replicas": int64(1),
							},
						},
						"template": flinkDeployment,
					},
				},
			},
		},
	}

	_, err := s.client.Resource(AppWrapperGVR).
		Namespace(s.cfg.Namespace).
		Create(ctx, appWrapper, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("flink submitter: failed to create AppWrapper %q: %w", awName, err)
	}
	return awName, nil
}

// WaitForCompletion polls the FlinkDeployment job status until terminal state or timeout.
func (s *Submitter) WaitForCompletion(ctx context.Context, name string, timeout time.Duration) (string, error) {
	flinkGVR := schema.GroupVersionResource{
		Group: "flink.apache.org", Version: "v1beta1", Resource: "flinkdeployments",
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		obj, err := s.client.Resource(flinkGVR).
			Namespace(s.cfg.Namespace).
			Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		state, _, _ := unstructured.NestedString(obj.Object, "status", "jobStatus", "state")
		if state == "FINISHED" || state == "FAILED" || state == "CANCELED" {
			return state, nil
		}
		time.Sleep(15 * time.Second)
	}
	return "TIMEOUT", nil
}
