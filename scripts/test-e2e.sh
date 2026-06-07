#!/usr/bin/env bash

set -euo pipefail

CLUSTER_NAME="surreal-dbops-tests"
NAMESPACE="reliquo-system"

echo "=== 1. Creating KIND Cluster ==="
if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
  echo "Cluster ${CLUSTER_NAME} already exists. Deleting it..."
  kind delete cluster --name "${CLUSTER_NAME}"
fi
kind create cluster --name "${CLUSTER_NAME}"

# Setup cleanup trap
function cleanup {
  echo "=== Cleanup: Killing Port-Forward and Deleting KIND Cluster ==="
  if [ -n "${PF_PID:-}" ]; then
    kill "$PF_PID" || true
  fi
  kind delete cluster --name "${CLUSTER_NAME}"
}
trap cleanup EXIT

echo "=== 2. Installing cert-manager ==="
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.12.0/cert-manager.yaml
echo "Waiting for cert-manager to be ready..."
kubectl wait --for=condition=Available deployment/cert-manager-webhook -n cert-manager --timeout=120s

echo "Creating selfsigned-issuer ClusterIssuer..."
cat <<EOF | kubectl apply -f -
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned-issuer
spec:
  selfSigned: {}
EOF

echo "=== 3. Deploying SurrealDB ==="
helm repo add surrealdb https://helm.surrealdb.com
helm repo update
helm install surrealdb surrealdb/surrealdb \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --set surrealdb.initial_user=root \
  --set surrealdb.initial_pass=rootpassword \
  --set image.tag=v1.3.0

echo "Waiting for SurrealDB pod to be ready..."
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=surrealdb -n "${NAMESPACE}" --timeout=120s

# Create root credentials secret for the Instance CRD to reference
kubectl create secret generic surrealdb-root \
  -n "${NAMESPACE}" \
  --from-literal=password=rootpassword

echo "=== 4. Building and Loading Operator Image ==="
docker build -t ghcr.io/reliquo/surreal-dbops:latest .
kind load docker-image ghcr.io/reliquo/surreal-dbops:latest --name "${CLUSTER_NAME}"

echo "=== 5. Installing surreal-dbops Operator ==="
helm install surreal-dbops ./charts/surreal-dbops \
  --namespace "${NAMESPACE}" \
  --set image.repository=ghcr.io/reliquo/surreal-dbops \
  --set image.tag=latest \
  --set webhook.enabled=true \
  --set webhook.certManager.enabled=true \
  --set webhook.certManager.generateCert=true

echo "Waiting for Operator deployment to be ready..."
if ! kubectl wait --for=condition=Available deployment/surreal-dbops -n "${NAMESPACE}" --timeout=120s; then
  echo "=== Operator deployment failed to become ready ==="
  echo "=== Pod Status ==="
  kubectl get pods -n "${NAMESPACE}"
  echo "=== Pod Describe ==="
  kubectl describe pods -l app.kubernetes.io/name=surreal-dbops -n "${NAMESPACE}"
  echo "=== Operator Logs ==="
  kubectl logs -l app.kubernetes.io/name=surreal-dbops -n "${NAMESPACE}" --tail=100 || true
  exit 1
fi

echo "=== 6. Port-forwarding SurrealDB for Host Test Verification ==="
kubectl port-forward svc/surrealdb -n "${NAMESPACE}" 8000:8000 > /dev/null 2>&1 &
PF_PID=$!
sleep 3 # Give port-forward some time to establish

echo "=== 7. Running Cargo Integration Tests ==="
# Run only integration tests
if ! cargo test --test integration_test -- --nocapture; then
  echo "=== Integration Tests Failed ==="
  echo "=== Pod Status ==="
  kubectl get pods -n "${NAMESPACE}"
  echo "=== Instance Status ==="
  kubectl get instances -n test-ns-dbops -o yaml || true
  echo "=== Namespace Status ==="
  kubectl get namespaces.surrealdb.reliquo.io -n test-ns-dbops -o yaml || true
  echo "=== Operator Logs ==="
  kubectl logs -l app.kubernetes.io/name=surreal-dbops -n "${NAMESPACE}" --tail=100 || true
  exit 1
fi

echo "=== E2E Tests Succeeded ==="
