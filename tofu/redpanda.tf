# Deployed as plain manifests rather than the official Helm chart.
#
# The chart is the better choice for a real cluster, but for a single-node
# minikube it brings a StatefulSet, PVCs, a console Deployment and a tuning
# init container — most of which either cannot be satisfied on one node or pull
# images that are subject to Docker Hub's anonymous rate limit. A single-replica
# dev-mode broker is what this environment actually needs, and it is the same
# shape as the Redis deployment next door.
#
# Redpanda is still the broker; only the packaging differs. Helm is used where
# it earns its keep: cert-manager, the Flink operator, and the FlinkDeployment.
resource "kubernetes_deployment" "redpanda" {
  metadata {
    name      = "redpanda"
    namespace = kubernetes_namespace.citibike.metadata[0].name
    labels    = { app = "redpanda" }
  }

  spec {
    replicas = 1

    selector {
      match_labels = { app = "redpanda" }
    }

    template {
      metadata {
        labels = { app = "redpanda" }
      }

      spec {
        container {
          name              = "redpanda"
          image             = var.redpanda_image
          image_pull_policy = "IfNotPresent"

          # `rpk redpanda start`, not `redpanda start`: a Kubernetes `command`
          # replaces the image entrypoint outright, and the bare redpanda binary
          # does not accept these flags — the rpk wrapper translates them.
          command = [
            "rpk", "redpanda", "start",
            "--smp=1",
            "--overprovisioned",
            "--node-id=0",
            "--kafka-addr=INTERNAL://0.0.0.0:9093",
            "--advertise-kafka-addr=INTERNAL://redpanda.${var.namespace}.svc.cluster.local:9093",
            "--check=false",
            # No TLS and no SASL: local development cluster only.
            "--set", "redpanda.auto_create_topics_enabled=true",
          ]

          port {
            name           = "kafka"
            container_port = 9093
          }

          port {
            name           = "admin"
            container_port = 9644
          }

          resources {
            requests = { cpu = "300m", memory = "512Mi" }
            limits   = { cpu = "1", memory = "2Gi" }
          }

          readiness_probe {
            tcp_socket {
              port = 9093
            }
            initial_delay_seconds = 10
            period_seconds        = 5
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "redpanda" {
  metadata {
    name      = "redpanda"
    namespace = kubernetes_namespace.citibike.metadata[0].name
  }

  spec {
    selector = { app = "redpanda" }

    port {
      name        = "kafka"
      port        = 9093
      target_port = 9093
    }

    port {
      name        = "admin"
      port        = 9644
      target_port = 9644
    }
  }
}
