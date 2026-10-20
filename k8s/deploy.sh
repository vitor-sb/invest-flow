#!/usr/bin/env bash
# Build da imagem, import no k3d, Secret, manifests (warehouse + PVC) e helm install do Airflow.
set -euo pipefail
export PATH=$HOME/.local/bin:$PATH
cd "$(dirname "$0")"
APP=..

docker build -t airflow-invest-flow:k8s "$APP"
k3d image import airflow-invest-flow:k8s -c dev

kubectl apply -f namespace.yaml
WH_PASS="${WAREHOUSE_PASSWORD:-airflow}"
kubectl -n invest-flow create secret generic invest-flow-env \
  --from-literal=WAREHOUSE_PASSWORD="$WH_PASS" \
  --from-literal=WAREHOUSE_CONN="postgresql://airflow:$WH_PASS@warehouse:5432/data_warehouse" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -k .

helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
# --timeout longo: as migrations rodam como Job do chart antes dos pods subirem
helm upgrade --install airflow apache-airflow/airflow --version 1.19.0 \
  -n invest-flow -f values.yaml --timeout 10m
