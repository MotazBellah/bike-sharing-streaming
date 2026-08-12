variable "kubeconfig_path" {
  description = "Path to the kubeconfig file."
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "Kubeconfig context to target."
  type        = string
  default     = "minikube"
}

variable "namespace" {
  description = "Namespace holding the whole pipeline."
  type        = string
  default     = "citibike"
}

# The localhost/ prefix is how podman namespaces locally built images, and it
# survives into minikube's CRI-O store. Without it CRI-O expands a bare name to
# docker.io/library/... and fails to pull. Override both to the unprefixed names
# when running minikube on the Docker driver.
variable "flink_image" {
  description = "PyFlink job image. Preloaded by 'make images'."
  type        = string
  default     = "localhost/citibike-flink:local"
}

variable "api_image" {
  description = "FastAPI image. Preloaded by 'make images'."
  type        = string
  default     = "localhost/citibike-api:local"
}

variable "redpanda_image" {
  description = "Redpanda broker image. Preloaded by 'make images' to avoid Docker Hub rate limits."
  type        = string
  default     = "docker.io/redpandadata/redpanda:v24.2.7"
}

variable "flink_parallelism" {
  description = "Job parallelism. Keep at or below total task slots."
  type        = number
  default     = 2
}

variable "kafka_topic" {
  type    = string
  default = "citibike-events"
}
