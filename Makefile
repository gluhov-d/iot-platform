SHELL         := /bin/bash
ROOT_DIR      := $(shell cd $(dir $(lastword $(MAKEFILE_LIST))) && pwd)
INFRA_DIR     := $(ROOT_DIR)/infrastructure
ENV_FILE      := $(ROOT_DIR)/.env
COMPOSE_FILE  := $(INFRA_DIR)/docker-compose.yaml
COMPOSE       := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

include $(ENV_FILE)
export

# Сервисы, у которых есть healthcheck — их ждём при `make up`
HEALTH_SERVICES := postgres redis clickhouse kafka schema-registry minio keycloak camunda prometheus loki tempo grafana

.DEFAULT_GOAL := help
.PHONY: help up down stop start restart ps logs health urls pull build clean reset \
        psql redis-cli clickhouse-cli topics diagrams check

## ----------------------------------------------------------------- Справка
help: ## Показать список команд
	@echo "IoT Platform — доступные команды:"
	@echo ""
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""

## ------------------------------------------------------------ Жизненный цикл
up: ## Поднять всю инфраструктуру и дождаться готовности
	@echo "==> Запуск инфраструктуры IoT Platform"
	@$(COMPOSE) up -d --remove-orphans
	@$(MAKE) --no-print-directory wait
	@$(MAKE) --no-print-directory urls

down: ## Остановить и удалить контейнеры (тома сохраняются)
	@echo "==> Остановка инфраструктуры"
	@$(COMPOSE) down --remove-orphans

stop: ## Приостановить контейнеры без удаления
	@$(COMPOSE) stop

start: ## Запустить ранее остановленные контейнеры
	@$(COMPOSE) start

restart: ## Перезапустить всё окружение
	@$(MAKE) --no-print-directory down
	@$(MAKE) --no-print-directory up

clean: ## Удалить контейнеры вместе с томами (данные будут потеряны)
	@echo "==> Удаление контейнеров и томов"
	@$(COMPOSE) down -v --remove-orphans

reset: clean up ## Полный пересоздание окружения с нуля

pull: ## Скачать/обновить образы
	@$(COMPOSE) pull

## ---------------------------------------------------------------- Состояние
ps: ## Статус контейнеров
	@$(COMPOSE) ps

logs: ## Логи всех сервисов (make logs S=kafka — по одному)
	@$(COMPOSE) logs -f --tail=100 $(S)

health: ## Показать healthcheck-статус всех сервисов
	@printf "%-20s %-12s %s\n" "SERVICE" "STATE" "HEALTH"; \
	$(COMPOSE) ps --format '{{.Service}}\t{{.State}}\t{{.Health}}' | \
		awk -F'\t' '{printf "%-20s %-12s %s\n", $$1, $$2, ($$3 == "" ? "-" : $$3)}'

wait: ## Дождаться, пока все сервисы станут healthy
	@echo "==> Ожидание готовности сервисов (до 5 минут)"
	@deadline=$$(( $$(date +%s) + 300 )); \
	while true; do \
	  pending=""; \
	  for s in $(HEALTH_SERVICES); do \
	    st=$$($(COMPOSE) ps --format '{{.Health}}' $$s 2>/dev/null | head -1); \
	    [ "$$st" = "healthy" ] || pending="$$pending $$s"; \
	  done; \
	  if [ -z "$$pending" ]; then echo "==> Все сервисы healthy"; break; fi; \
	  if [ $$(date +%s) -ge $$deadline ]; then \
	    echo "!!! Не дождались:$$pending"; $(MAKE) --no-print-directory health; exit 1; fi; \
	  echo "    ждём:$$pending"; sleep 10; \
	done

urls: ## Показать адреса всех сервисов
	@echo ""
	@echo "  Сервис            URL"
	@echo "  ---------------   ------------------------------------------------"
	@echo "  PostgreSQL        postgresql://localhost:$(POSTGRES_PORT)/$(POSTGRES_DB)"
	@echo "  Redis             redis://localhost:$(REDIS_PORT)"
	@echo "  ClickHouse        http://localhost:$(CLICKHOUSE_HTTP_PORT)  (native: $(CLICKHOUSE_NATIVE_PORT))"
	@echo "  Kafka             localhost:$(KAFKA_EXTERNAL_PORT)"
	@echo "  Schema Registry   http://localhost:$(SCHEMA_REGISTRY_PORT)"
	@echo "  MinIO API         http://localhost:$(MINIO_API_PORT)"
	@echo "  MinIO Console     http://localhost:$(MINIO_CONSOLE_PORT)"
	@echo "  Keycloak          http://localhost:$(KEYCLOAK_PORT)"
	@echo "  Camunda           http://localhost:$(CAMUNDA_PORT)/camunda"
	@echo "  Grafana           http://localhost:$(GRAFANA_PORT)"
	@echo "  Prometheus        http://localhost:$(PROMETHEUS_PORT)"
	@echo "  Loki              http://localhost:$(LOKI_PORT)"
	@echo "  Tempo             http://localhost:$(TEMPO_HTTP_PORT)  (OTLP: $(TEMPO_OTLP_GRPC_PORT)/$(TEMPO_OTLP_HTTP_PORT))"
	@echo "  Alloy             http://localhost:$(ALLOY_PORT)"
	@echo ""

## ------------------------------------------------------------------ Клиенты
psql: ## Открыть psql в контейнере postgres
	@$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

redis-cli: ## Открыть redis-cli
	@$(COMPOSE) exec redis redis-cli -a $(REDIS_PASSWORD)

clickhouse-cli: ## Открыть clickhouse-client
	@$(COMPOSE) exec clickhouse clickhouse-client --user $(CLICKHOUSE_USER) --password $(CLICKHOUSE_PASSWORD)

topics: ## Список топиков Kafka
	@$(COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:$(KAFKA_INTERNAL_PORT) --list

## ----------------------------------------------------------------- Проверки
check: ## Проверить корректность docker-compose.yaml
	@$(COMPOSE) config --quiet && echo "docker-compose.yaml — OK"

diagrams: ## Отрендерить C4-диаграммы в PNG (нужен plantuml)
	@command -v plantuml >/dev/null 2>&1 || { echo "plantuml не установлен: brew install plantuml"; exit 1; }
	@plantuml -tpng -o . $(ROOT_DIR)/diagrams/*.puml && echo "PNG сохранены в diagrams/"
