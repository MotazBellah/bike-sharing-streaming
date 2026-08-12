# The Flink Operator's admission webhook is served over TLS, so cert-manager
# has to exist and be ready before the operator installs.
resource "helm_release" "cert_manager" {
  name             = "cert-manager"
  repository       = "https://charts.jetstack.io"
  chart            = "cert-manager"
  version          = "v1.15.3"
  namespace        = "cert-manager"
  create_namespace = true
  wait             = true

  set {
    name  = "crds.enabled"
    value = "true"
  }

  # CRI-O does not resolve the images' symbolic USER to a uid, so the default
  # runAsNonRoot check rejects them with "image will run as root". Pinning an
  # explicit uid satisfies the check on any runtime.
  values = [yamlencode({
    securityContext = { runAsNonRoot = true, runAsUser = 1000 }
    cainjector      = { securityContext = { runAsNonRoot = true, runAsUser = 1000 } }
    webhook         = { securityContext = { runAsNonRoot = true, runAsUser = 1000 } }
    startupapicheck = { securityContext = { runAsNonRoot = true, runAsUser = 1000 } }
  })]
}

# Served from archive.apache.org, not downloads.apache.org: Apache keeps only
# the current release on the download mirrors, so every pinned operator version
# 404s there sooner or later. The archive is stable.
resource "helm_release" "flink_operator" {
  name       = "flink-kubernetes-operator"
  repository = "https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.15.0/"
  chart      = "flink-kubernetes-operator"
  version    = "1.15.0"
  namespace  = kubernetes_namespace.citibike.metadata[0].name
  wait       = true

  # Scoping the operator to our namespace also makes the chart provision the
  # 'flink' ServiceAccount and RBAC that FlinkDeployment pods run under.
  set {
    name  = "watchNamespaces[0]"
    value = kubernetes_namespace.citibike.metadata[0].name
  }

  depends_on = [helm_release.cert_manager]
}

# The FlinkDeployment is a custom resource, so it cannot be created with
# `kubernetes_manifest`: that resource validates against the cluster's schema
# at *plan* time, and the CRD does not exist until the operator above has been
# applied. `depends_on` cannot fix it, because plan runs before any apply.
#
# Wrapping the CR in a local Helm chart sidesteps the ordering problem
# entirely, since helm_release only contacts the cluster during apply.
resource "helm_release" "citibike_job" {
  name      = "citibike-job"
  chart     = "${path.module}/charts/citibike-job"
  namespace = kubernetes_namespace.citibike.metadata[0].name
  wait      = false

  set {
    name  = "image"
    value = var.flink_image
  }

  set {
    name  = "parallelism"
    value = var.flink_parallelism
  }

  set {
    name  = "kafka.brokers"
    value = "${kubernetes_service.redpanda.metadata[0].name}.${kubernetes_namespace.citibike.metadata[0].name}.svc.cluster.local:9093"
  }

  set {
    name  = "kafka.topic"
    value = var.kafka_topic
  }

  set {
    name  = "redis.host"
    value = "${kubernetes_service.redis.metadata[0].name}.${kubernetes_namespace.citibike.metadata[0].name}.svc.cluster.local"
  }

  depends_on = [
    helm_release.flink_operator,
    kubernetes_service.redpanda,
    kubernetes_service.redis,
  ]
}
