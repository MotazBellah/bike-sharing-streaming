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

# `auto_create_topics_enabled` only fires on a *produce* request. Flink's
# KafkaSource is a consumer, and consumer-side metadata requests don't trigger
# auto-creation, so the source enumerator fails with UnknownTopicOrPartition
# if it comes up before anything has ever produced to the topic. Locally
# `make topic` covers this; in the cluster nothing produces until the
# event_generator is run by hand, so the topic has to be created explicitly
# before the Flink job starts.
resource "kubernetes_job" "create_topic" {
  metadata {
    name      = "create-topic"
    namespace = kubernetes_namespace.citibike.metadata[0].name
  }

  spec {
    backoff_limit = 6

    template {
      metadata {
        labels = { app = "create-topic" }
      }

      spec {
        restart_policy = "OnFailure"

        container {
          name  = "create-topic"
          image = var.redpanda_image

          # Idempotent: reapplying this config re-runs the job (its pod
          # already completed and is immutable), and `rpk topic create`
          # errors out on a topic that's already there.
          command = ["sh", "-c", <<-EOT
            rpk topic describe "$TOPIC" -X brokers="$BROKERS" >/dev/null 2>&1 \
              || rpk topic create "$TOPIC" --partitions 3 --replicas 1 -X brokers="$BROKERS"
          EOT
          ]

          env {
            name  = "TOPIC"
            value = var.kafka_topic
          }

          env {
            name  = "BROKERS"
            value = "${kubernetes_service.redpanda.metadata[0].name}.${kubernetes_namespace.citibike.metadata[0].name}.svc.cluster.local:9093"
          }
        }
      }
    }
  }

  wait_for_completion = true

  timeouts {
    create = "2m"
  }

  depends_on = [kubernetes_service.redpanda]
}
