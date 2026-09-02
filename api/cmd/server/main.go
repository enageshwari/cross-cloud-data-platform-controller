package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/joho/godotenv"

	"github.com/enageshwari/cross-cloud-data-platform-controller/internal/flink"
	"github.com/enageshwari/cross-cloud-data-platform-controller/internal/handler"
	"github.com/enageshwari/cross-cloud-data-platform-controller/internal/presigner"
	"github.com/enageshwari/cross-cloud-data-platform-controller/internal/spark"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Load .env when running locally. In GKE the vars come from Secret Manager
	// mounted as environment variables — godotenv is a no-op if .env is absent.
	if err := godotenv.Load(); err != nil {
		logger.Info(".env not found, relying on environment variables")
	}

	// ---- AWS S3 presigner ----
	ttlMinutes, _ := strconv.Atoi(os.Getenv("PRESIGN_TTL_MINUTES"))
	if ttlMinutes <= 0 {
		ttlMinutes = 15
	}

	s3p, err := presigner.New(presigner.Config{
		AccessKeyID:     os.Getenv("AWS_PRESIGNER_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_PRESIGNER_SECRET_ACCESS_KEY"),
		Region:          os.Getenv("AWS_PRESIGNER_REGION"),
		Bucket:          os.Getenv("AWS_OUTPUT_BUCKET"),
		TTL:             time.Duration(ttlMinutes) * time.Minute,
	})
	if err != nil {
		logger.Warn("S3 presigner not available — AWS jobs will fail at URL generation",
			slog.String("reason", err.Error()),
		)
		s3p = nil
	} else {
		logger.Info("S3 presigner ready",
			slog.String("bucket", os.Getenv("AWS_OUTPUT_BUCKET")),
			slog.String("region", os.Getenv("AWS_PRESIGNER_REGION")),
			slog.Int("ttl_minutes", ttlMinutes),
		)
	}

	// ---- GCS presigner ----
	gcsTTLMinutes, _ := strconv.Atoi(os.Getenv("GCS_PRESIGN_TTL_MINUTES"))
	if gcsTTLMinutes <= 0 {
		gcsTTLMinutes = 15
	}

	ctx := context.Background()
	gcsp, err := presigner.NewGCSPresigner(ctx, presigner.GCSConfig{
		ServiceAccountEmail: os.Getenv("GCS_PRESIGNER_SA"),
		Bucket:              os.Getenv("GCS_OUTPUT_BUCKET"),
		TTL:                 time.Duration(gcsTTLMinutes) * time.Minute,
	})
	if err != nil {
		logger.Warn("GCS presigner not available — GCP jobs will fail at URL generation",
			slog.String("reason", err.Error()),
		)
		gcsp = nil
	} else {
		logger.Info("GCS presigner ready",
			slog.String("bucket", os.Getenv("GCS_OUTPUT_BUCKET")),
			slog.Int("ttl_minutes", gcsTTLMinutes),
		)
	}

	// ---- Spark submitter (EKS / AWS) ----
	sparkSubAWS, err := spark.New(spark.Config{
		KubeconfigPath:         os.Getenv("KUBECONFIG"),
		KubeContext:            os.Getenv("EKS_CONTEXT"),
		TargetCloud:            "aws",
		Namespace:              "data-workloads",
		SparkImage:             os.Getenv("SPARK_IMAGE"),
		DriverServiceAccount:   "spark-operator-spark",
		ExecutorServiceAccount: "spark-operator-spark",
	})
	if err != nil {
		logger.Warn("AWS Spark submitter not available — AWS Spark jobs won't be dispatched to EKS",
			slog.String("reason", err.Error()),
		)
		sparkSubAWS = nil
	} else {
		logger.Info("AWS Spark submitter ready",
			slog.String("context", os.Getenv("EKS_CONTEXT")),
		)
	}

	// ---- Spark submitter (GKE / GCP) ----
	sparkSubGCP, err := spark.New(spark.Config{
		KubeconfigPath:         os.Getenv("KUBECONFIG"),
		KubeContext:            os.Getenv("GKE_CONTEXT"),
		TargetCloud:            "gcp",
		Namespace:              "data-workloads",
		SparkImage:             os.Getenv("SPARK_IMAGE"),
		DriverServiceAccount:   "spark-operator-spark",
		ExecutorServiceAccount: "spark-operator-spark",
		CoreRequest:            "500m", // e2-standard-2 nodes are CPU-constrained
	})
	if err != nil {
		logger.Warn("GCP Spark submitter not available — GCP Spark jobs won't be dispatched to GKE",
			slog.String("reason", err.Error()),
		)
		sparkSubGCP = nil
	} else {
		logger.Info("GCP Spark submitter ready",
			slog.String("context", os.Getenv("GKE_CONTEXT")),
		)
	}

	// ---- Flink submitter (EKS / AWS) ----
	flinkSubAWS, err := flink.New(flink.Config{
		KubeconfigPath: os.Getenv("KUBECONFIG"),
		KubeContext:    os.Getenv("EKS_CONTEXT"),
		TargetCloud:    "aws",
		Namespace:      "data-workloads",
		FlinkImage:     "apache/flink:1.20",
		ServiceAccount: "flink",
		CPURequest:     "500m",
		MemoryRequest:  "1024m",
	})
	if err != nil {
		logger.Warn("AWS Flink submitter not available — AWS Flink jobs won't be dispatched to EKS",
			slog.String("reason", err.Error()),
		)
		flinkSubAWS = nil
	} else {
		logger.Info("AWS Flink submitter ready",
			slog.String("context", os.Getenv("EKS_CONTEXT")),
		)
	}

	// ---- Flink submitter (GKE / GCP) ----
	flinkSubGCP, err := flink.New(flink.Config{
		KubeconfigPath: os.Getenv("KUBECONFIG"),
		KubeContext:    os.Getenv("GKE_CONTEXT"),
		TargetCloud:    "gcp",
		Namespace:      "data-workloads",
		FlinkImage:     "apache/flink:1.20",
		ServiceAccount: "flink",
		CPURequest:     "500m",
		MemoryRequest:  "1024m",
	})
	if err != nil {
		logger.Warn("GCP Flink submitter not available — GCP Flink jobs won't be dispatched to GKE",
			slog.String("reason", err.Error()),
		)
		flinkSubGCP = nil
	} else {
		logger.Info("GCP Flink submitter ready",
			slog.String("context", os.Getenv("GKE_CONTEXT")),
		)
	}

	jobsHandler := handler.NewJobsHandler(logger, s3p, gcsp, sparkSubAWS, sparkSubGCP, flinkSubAWS, flinkSubGCP)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	// Structured JSON request logging via slog
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			logger.Info("request",
				slog.String("method", req.Method),
				slog.String("path", req.URL.Path),
				slog.String("remote_addr", req.RemoteAddr),
			)
			next.ServeHTTP(w, req)
		})
	})

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/jobs", jobsHandler.Submit)
	})

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	logger.Info("control plane API starting", slog.String("port", port))
	if err := http.ListenAndServe(":"+port, r); err != nil {
		logger.Error("server exited", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
