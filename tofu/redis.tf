# Deployed as plain manifests rather than a chart: Redis needs no configuration
# here, and this avoids taking a dependency on the Bitnami chart catalog.
#
# No persistence is configured, and that is deliberate. Redis holds a derived
# materialized view, not source-of-truth data. If it is lost, the Flink job is
# restarted from an empty state and replays the Redpanda topic from offset 0 to
# rebuild it. The event log is the durable store.
resource "kubernetes_deployment" "redis" {
  metadata {
    name      = "redis"
    namespace = kubernetes_namespace.citibike.metadata[0].name
    labels    = { app = "redis" }
  }

  spec {
    replicas = 1

    selector {
      match_labels = { app = "redis" }
    }

    template {
      metadata {
        labels = { app = "redis" }
      }

      spec {
        container {
          name  = "redis"
          image = "redis:7-alpine"

          port {
            container_port = 6379
          }

          resources {
            requests = { cpu = "100m", memory = "128Mi" }
            limits   = { cpu = "500m", memory = "512Mi" }
          }

          readiness_probe {
            exec {
              command = ["redis-cli", "ping"]
            }
            initial_delay_seconds = 5
            period_seconds        = 5
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "redis" {
  metadata {
    name      = "redis"
    namespace = kubernetes_namespace.citibike.metadata[0].name
  }

  spec {
    selector = { app = "redis" }

    port {
      port        = 6379
      target_port = 6379
    }
  }
}
