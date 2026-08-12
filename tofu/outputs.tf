output "api_url" {
  description = "Reachable once minikube exposes the NodePort."
  value       = "http://$(minikube ip):30080"
}

output "flink_ui_port_forward" {
  description = "Flink dashboard is not exposed; forward it when needed."
  value       = "kubectl -n ${var.namespace} port-forward svc/citibike-aggregator-rest 8081:8081"
}

output "kafka_bootstrap" {
  description = "In-cluster Redpanda bootstrap address."
  value       = "${kubernetes_service.redpanda.metadata[0].name}.${var.namespace}.svc.cluster.local:9093"
}
