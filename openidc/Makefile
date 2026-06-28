# OpenIDC developer commands. Run `make help` for the list.
.DEFAULT_GOAL := help
COMPOSE ?= $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; elif command -v docker-compose >/dev/null 2>&1; then echo "docker-compose"; else echo "docker compose"; fi)
PYTHON ?= $(shell if command -v python3.11 >/dev/null 2>&1; then echo "python3.11"; else echo "python3"; fi)

.PHONY: help up down logs ps test psql topics clean rebuild inject quiet boom watch dashboard events rabbitmq min-stations station-add grafana prom css-export

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up: ## Build and start the whole pipeline
	$(COMPOSE) up --build -d
	@echo "Pipeline starting. Run 'make logs' to watch detections."

down: ## Stop all services
	$(COMPOSE) down

logs: ## Tail logs from the processing services
	$(COMPOSE) logs -f ingestion station-processing

ps: ## Show running services
	$(COMPOSE) ps

test: ## Run unit tests for the detector (no containers needed)
	cd services/station-processing && $(PYTHON) -m pytest ../../tests -v

psql: ## Open a SQL shell against the arrivals database
	$(COMPOSE) exec postgres psql -U idc -d openidc

topics: ## List Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

rebuild: ## Rebuild service images without cache
	$(COMPOSE) build --no-cache ingestion station-processing dashboard

dashboard: ## Print the web control-panel URL (Level B)
	@echo "Dashboard: http://localhost:8000"

inject: ## Inject one event on demand. Vars: STA AMP FREQ (e.g. make inject STA=FINES AMP=20)
	$(COMPOSE) exec ingestion python inject_event.py --sta $(or $(STA),ARCES) --amp $(or $(AMP),18) --freq $(or $(FREQ),4)

quiet: ## Inject a noise-only frame (should NOT trigger a detection). Var: STA
	$(COMPOSE) exec ingestion python inject_event.py --quiet --sta $(or $(STA),ARCES)

boom: ## Inject a high-amplitude event (likely screens as EXPLOSION-LIKE). Var: STA
	$(COMPOSE) exec ingestion python inject_event.py --sta $(or $(STA),ARCES) --amp 35 --freq 6

watch: ## Live-refreshing table of recent detections (Ctrl-C to stop)
	bash scripts/watch.sh

events: ## Show recently located + screened events (Phase 2)
	$(COMPOSE) exec postgres psql -U idc -d openidc -c "SELECT orid, round(lat::numeric,2) lat, round(lon::numeric,2) lon, round(depth::numeric,1) depth, round(mb::numeric,2) mb, nass, etype FROM origin ORDER BY lddate DESC LIMIT 15;"

rabbitmq: ## Print the RabbitMQ management UI URL (guest/guest)
	@echo "RabbitMQ UI: http://localhost:15672  (user: guest / pass: guest)"

grafana: ## Print the Grafana State-of-Health dashboard URL
	@echo "Grafana: http://localhost:3000  (anonymous viewer; admin login admin/admin)"

prom: ## Print the Prometheus URL
	@echo "Prometheus: http://localhost:9090"

css-export: ## Export arrivals to a CSS 3.0 flat file via the Perl tool
	$(COMPOSE) run --rm perl-tools

min-stations: ## Set associator min stations via RabbitMQ. Var: N (e.g. make min-stations N=3)
	curl -s -X POST localhost:8000/api/command -H 'Content-Type: application/json' -d '{"cmd":"set_min_stations","value":$(or $(N),2)}'; echo

station-add: ## Configure a new station via RabbitMQ. Vars: NAME LAT LON
	curl -s -X POST localhost:8000/api/command -H 'Content-Type: application/json' -d '{"cmd":"add_station","sta":"$(NAME)","lat":$(LAT),"lon":$(LON)}'; echo

clean: ## Stop and remove volumes (wipes the database)
	$(COMPOSE) down -v
