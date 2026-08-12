resource "kubernetes_deployment" "api" {
  metadata {
    name      = "citibike-api"
    namespace = kubernetes_namespace.citibike.metadata[0].name
    labels    = { app = "citibike-api" }
  }

  spec {
    replicas = 1

    selector {
      match_labels = { app = "citibike-api" }
    }

    template {
      metadata {
        labels = { app = "citibike-api" }
      }

      spec {
        container {
          name  = "api"
          image = var.api_image
          # Image is side-loaded with 'minikube image load', so never try a registry.
          image_pull_policy = "Never"

          port {
            container_port = 8000
          }

          env {
            name  = "REDIS_HOST"
            value = kubernetes_service.redis.metadata[0].name
          }

          # Must be set explicitly. Kubernetes injects Docker-link-style
          # variables for every Service in the namespace, so a Service named
          # "redis" produces REDIS_PORT=tcp://10.x.x.x:6379 — which shadows the
          # application default and makes int() blow up at startup. Setting it
          # here wins over the injected value.
          env {
            name  = "REDIS_PORT"
            value = "6379"
          }

          resources {
            requests = { cpu = "100m", memory = "128Mi" }
            limits   = { cpu = "500m", memory = "512Mi" }
          }

          readiness_probe {
            http_get {
              path = "/health"
              port = 8000
            }
            initial_delay_seconds = 5
            period_seconds        = 10
          }
        }
      }
    }
  }

  depends_on = [kubernetes_service.redis]
}

resource "kubernetes_service" "api" {
  metadata {
    name      = "citibike-api"
    namespace = kubernetes_namespace.citibike.metadata[0].name
  }

  spec {
    selector = { app = "citibike-api" }
    type     = "NodePort"

    port {
      port        = 8000
      target_port = 8000
      node_port   = 30080
    }
  }
}
