SHELL         := /bin/bash
ROOT_DIR      := $(shell cd $(dir $(lastword $(MAKEFILE_LIST))) && pwd)
INFRA_DIR     := $(ROOT_DIR)/infrastructure
ENV_FILE      := $(ROOT_DIR)/.env
COMPOSE_FILE  := $(INFRA_DIR)/docker-compose.yaml
COMPOSE       := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

# Значение переменной в том виде, как его разобрал Compose из .env.
# Только для портов и имён - пароли берутся из окружения контейнера.0
compose_env = $$($(COMPOSE) config --environment | sed -n 's/^$(1)=//p')

# Сервисы, у которых есть healthcheck — их ждём при `make up`
HEALTH_SERVICES := postgres redis clickhouse kafka schema-registry minio keycloak camunda prometheus loki tempo grafana

.DEFAULT_GOAL := help
.PHONY: help up down stop start restart ps logs health urls pull build clean reset kafka-reset \
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

KAFKA_SERVICES := kafka kafka-init schema-registry kafka-exporter

kafka-reset: ## Пересоздать только том Kafka (сообщения, офсеты и схемы будут потеряны)
	@echo "!!! Будут удалены ВСЕ данные Kafka: сообщения во всех топиках, офсеты"
	@echo "!!! консьюмер-групп и Avro-схемы Schema Registry (топик _schemas)."
	@echo "    Данные PostgreSQL, ClickHouse, MinIO и остальных сервисов не затрагиваются."
	@[ "$(FORCE)" = "1" ] || { read -p "Продолжить? [y/N] " a; [ "$$a" = "y" ] || { echo "Отменено"; exit 1; };}
	@$(COMPOSE) rm -sf $(KAFKA_SERVICES)
	@docker volume rm "$(call compose_env,COMPOSE_PROJECT_NAME)_kafka-data"
	@$(MAKE) --no-print-directory up

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
	@env=$$($(COMPOSE) config --environment); v() { sed -n "s/^$$1=//p" <<<"$$env"; }; \
	echo ""; \
	echo "  Сервис            URL"; \
	echo "  ---------------   ------------------------------------------------"; \
	echo "  PostgreSQL        postgresql://localhost:$$(v POSTGRES_PORT)/$$(v POSTGRES_DB)"; \
	echo "  Redis             redis://localhost:$$(v REDIS_PORT)"; \
	echo "  ClickHouse        http://localhost:$$(v CLICKHOUSE_HTTP_PORT)  (native: $$(v CLICKHOUSE_NATIVE_PORT))"; \
	echo "  Kafka             localhost:$$(v KAFKA_EXTERNAL_PORT)"; \
	echo "  Schema Registry   http://localhost:$$(v SCHEMA_REGISTRY_PORT)"; \
	echo "  MinIO API         http://localhost:$$(v MINIO_API_PORT)"; \
	echo "  MinIO Console     http://localhost:$$(v MINIO_CONSOLE_PORT)"; \
	echo "  Keycloak          http://localhost:$$(v KEYCLOAK_PORT)"; \
	echo "  Camunda           http://localhost:$$(v CAMUNDA_PORT)/camunda"; \
	echo "  Grafana           http://localhost:$$(v GRAFANA_PORT)"; \
	echo "  Prometheus        http://localhost:$$(v PROMETHEUS_PORT)"; \
	echo "  Loki              http://localhost:$$(v LOKI_PORT)"; \
	echo "  Tempo             http://localhost:$$(v TEMPO_HTTP_PORT)  (OTLP: $$(v TEMPO_OTLP_GRPC_PORT)/$$(v TEMPO_OTLP_HTTP_PORT))"; \
	echo "  Alloy             http://localhost:$$(v ALLOY_PORT)"; \
	echo ""

## ------------------------------------------------------------------ Клиенты
psql: ## Открыть psql в контейнере postgres
	@$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

redis-cli: ## Открыть redis-cli
	@$(COMPOSE) exec redis redis-cli

clickhouse-cli: ## Открыть clickhouse-client
	@$(COMPOSE) exec clickhouse sh -c 'exec clickhouse-client --user "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD"'

topics: ## Список топиков Kafka
	@$(COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:$(call compose_env,KAFKA_INTERNAL_PORT) --list

## ----------------------------------------------------------------- Проверки
check: ## Проверить корректность docker-compose.yaml
	@$(COMPOSE) config --quiet && echo "docker-compose.yaml — OK"

diagrams: ## Отрендерить C4-диаграммы в PNG (нужен plantuml)
	@command -v plantuml >/dev/null 2>&1 || { echo "plantuml не установлен: brew install plantuml"; exit 1; }
	@plantuml -tpng -o . $(ROOT_DIR)/diagrams/*.puml && echo "PNG сохранены в diagrams/"
