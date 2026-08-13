.PHONY: help sync test lint up down logs topic generate images deploy destroy

CSV    ?= citibike_data.csv
BROKER ?= localhost:19092
TOPIC  ?= citibike-events

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

sync: ## Install all dependency groups (uv fetches Python 3.11 if needed)
	uv sync --all-groups

test: ## Run unit tests (no infrastructure required)
	uv run pytest -q

lint: ## Lint with ruff
	uv run ruff check .

up: ## Bring up the full stack with docker compose
	docker compose up -d --build

down: ## Tear the stack down, including volumes
	docker compose down -v

logs: ## Follow the Flink TaskManager logs
	docker compose logs -f taskmanager

topic: ## Create the topic with 3 partitions (auto-create would give 1)
	docker compose exec redpanda rpk topic create $(TOPIC) --partitions 3 --replicas 1

generate: ## Publish events (CSV=path/to/file.csv)
	uv run --group generator python event_generator.py --file $(CSV) --topic $(TOPIC) --broker $(BROKER)

images: ## Build images and side-load them into minikube
	podman build -t citibike-flink:local -f flink_job/Dockerfile .
	podman build -t citibike-api:local -f api/Dockerfile .
# Loading via a tar rather than `minikube image load <name>`: with the podman
# driver minikube cannot see the local image store directly, and podman
# namespaces local builds under localhost/, so a bare name is never found.
	podman save --format docker-archive -o /tmp/citibike-flink.tar citibike-flink:local
	podman save --format docker-archive -o /tmp/citibike-api.tar citibike-api:local
	minikube image load /tmp/citibike-flink.tar
	minikube image load /tmp/citibike-api.tar
	rm -f /tmp/citibike-flink.tar /tmp/citibike-api.tar
	minikube image ls | grep citibike

deploy: images ## Deploy to minikube via OpenTofu
	cd tofu && tofu init && tofu apply -auto-approve

destroy: ## Remove everything from minikube
	cd tofu && tofu destroy -auto-approve
