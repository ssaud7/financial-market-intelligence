#!/usr/bin/env bash
# Provisions the two isolated virtualenvs this project needs.
#
# Airflow pins a large, opinionated dependency set through its official
# constraints file. PySpark, Great Expectations, ChromaDB and
# sentence-transformers each pin overlapping libraries of their own. Resolving
# them into a single environment is fragile, so the pipeline code and the
# orchestrator get one environment each and the DAG shells out to the app
# interpreter. This is the same separation you would use in production.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PY=python3.11
APP_VENV="$REPO_ROOT/.venv-app"
AIRFLOW_VENV="$REPO_ROOT/.venv-airflow"
AIRFLOW_VERSION=2.10.5
PYTHON_TAG=3.11

echo "==> Creating application virtualenv at $APP_VENV"
$PY -m venv "$APP_VENV"
"$APP_VENV/bin/pip" install --upgrade pip setuptools wheel
# CPU-only torch first: the default Linux wheel pulls ~2.5 GB of CUDA runtime
# that a Codespace will never use, and sentence-transformers only needs CPU.
"$APP_VENV/bin/pip" install --index-url https://download.pytorch.org/whl/cpu torch
"$APP_VENV/bin/pip" install -r requirements/app.txt
"$APP_VENV/bin/pip" install -e .

echo "==> Creating Airflow virtualenv at $AIRFLOW_VENV"
$PY -m venv "$AIRFLOW_VENV"
"$AIRFLOW_VENV/bin/pip" install --upgrade pip setuptools wheel
"$AIRFLOW_VENV/bin/pip" install \
  "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_TAG}.txt"
"$AIRFLOW_VENV/bin/pip" install -r requirements/airflow.txt

echo "==> Creating runtime directories"
mkdir -p data/raw data/landing data/quarantine data/lakehouse data/vectorstore \
         evidence/lineage evidence/logs evidence/runs airflow/logs

if [ ! -f .env ]; then
  echo "==> Seeding .env from .env.example (remember to add ANTHROPIC_API_KEY)"
  cp .env.example .env
fi

echo ""
echo "Setup complete."
echo "  App interpreter     : $APP_VENV/bin/python"
echo "  Airflow interpreter : $AIRFLOW_VENV/bin/python"
echo "  Next: make kafka-up && make demo"
