.DEFAULT_GOAL := help
SHELL := /bin/bash

REPO_ROOT    := $(shell pwd)
APP_VENV     := $(REPO_ROOT)/.venv-app
AIRFLOW_VENV := $(REPO_ROOT)/.venv-airflow
FMIS         := $(APP_VENV)/bin/fmis
OLLAMA_MODEL ?= qwen2.5:7b-instruct-q4_K_M

export AIRFLOW_HOME := $(REPO_ROOT)/airflow
export FMIS_REPO_ROOT := $(REPO_ROOT)

.PHONY: help
help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---- Setup ---------------------------------------------------------------

.PHONY: setup
setup:  ## Create both virtualenvs and install dependencies
	bash .devcontainer/post-create.sh

.PHONY: infra-up
infra-up:  ## Start Kafka and Ollama
	docker compose up -d kafka ollama
	@echo "Waiting for Kafka to report healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' fmis-kafka 2>/dev/null)" = "healthy" ]; do \
		sleep 3; echo "  ...still starting"; \
	done
	@echo "Kafka is ready."

.PHONY: infra-down
infra-down:  ## Stop all containers (volumes preserved)
	docker compose --profile lineage --profile tools down

.PHONY: infra-nuke
infra-nuke:  ## Stop containers AND delete their volumes
	docker compose --profile lineage --profile tools down -v

.PHONY: ollama-pull
ollama-pull:  ## Pull the generation model (~4.7 GB, one time)
	docker compose exec ollama ollama pull $(OLLAMA_MODEL)
	docker compose exec ollama ollama list

.PHONY: lineage-up
lineage-up:  ## Start Marquez, the OpenLineage backend (UI on :3000)
	docker compose --profile lineage up -d
	@echo "Set OPENLINEAGE_TRANSPORT=http in .env to ship events to Marquez."

# ---- Pipeline stages -----------------------------------------------------

.PHONY: ingest
ingest:  ## Stage 1: produce to Kafka, then validate and route through the contract
	$(FMIS) kafka-setup
	$(FMIS) produce
	$(FMIS) consume
	$(FMIS) contract-report

.PHONY: lakehouse
lakehouse:  ## Stage 2: Bronze -> Silver -> (gate) -> Gold, plus the enforcement proof
	$(FMIS) bronze
	$(FMIS) silver
	$(FMIS) enforce-schema
	$(FMIS) quality-gate
	$(FMIS) gold

.PHONY: rag
rag:  ## Stage 3: build the hybrid index and answer the demo questions
	$(FMIS) rag-index
	$(FMIS) rag-demo

.PHONY: gate-fails
gate-fails:  ## Prove the quality gate halts the pipeline (expected to exit non-zero)
	-$(FMIS) quality-gate --taint
	@echo "^ a non-zero exit above is the expected, demonstrated behaviour."

.PHONY: pipeline
pipeline: ingest lakehouse rag  ## Run every stage end to end without Airflow
	$(FMIS) lineage-summary

# ---- Airflow -------------------------------------------------------------

.PHONY: airflow-init
airflow-init:  ## Initialise the Airflow metadata database and admin user
	AIRFLOW__CORE__DAGS_FOLDER=$(REPO_ROOT)/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	$(AIRFLOW_VENV)/bin/airflow db migrate
	AIRFLOW__CORE__DAGS_FOLDER=$(REPO_ROOT)/dags \
	$(AIRFLOW_VENV)/bin/airflow users create \
		--username admin --password admin \
		--firstname Capstone --lastname Admin \
		--role Admin --email admin@example.com || true

.PHONY: airflow-up
airflow-up:  ## Start the Airflow scheduler and web UI on :8080 (admin/admin)
	AIRFLOW__CORE__DAGS_FOLDER=$(REPO_ROOT)/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	FMIS_CLI=$(FMIS) \
	$(AIRFLOW_VENV)/bin/airflow standalone

.PHONY: airflow-trigger
airflow-trigger:  ## Trigger the production DAG from the command line
	AIRFLOW__CORE__DAGS_FOLDER=$(REPO_ROOT)/dags \
	$(AIRFLOW_VENV)/bin/airflow dags trigger financial_market_intelligence

.PHONY: airflow-trigger-failure
airflow-trigger-failure:  ## Trigger the gate-failure demonstration DAG
	AIRFLOW__CORE__DAGS_FOLDER=$(REPO_ROOT)/dags \
	$(AIRFLOW_VENV)/bin/airflow dags trigger financial_market_intelligence_gate_failure_demo

# ---- Quality -------------------------------------------------------------

.PHONY: test
test:  ## Run the unit tests (no Kafka or Spark required)
	$(APP_VENV)/bin/pytest

.PHONY: clean
clean:  ## Delete generated data, keeping raw inputs and evidence
	rm -rf data/landing data/quarantine data/lakehouse data/vectorstore data/spark-warehouse
	rm -rf metastore_db derby.log
	@echo "Generated layers removed. data/raw and evidence/ are untouched."
