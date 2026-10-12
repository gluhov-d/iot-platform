SHELL         := /bin/bash
ROOT_DIR      := $(shell cd $(dir $(lastword $(MAKEFILE_LIST))) && pwd)
INFRA_DIR     := $(ROOT_DIR)/infrastructure
ENV_FILE      := $(ROOT_DIR)/.env
COMPOSE_FILE  := $(INFRA_DIR)/docker-compose.yaml
SERVICE_DIR   := $(ROOT_DIR)/services/events-collector-service
# Многомодульная сборка: wrapper в корне, задачи сервиса - по пути Gradle-проекта
GRADLE        := cd $(ROOT_DIR) && ./gradlew
SERVICE_PROJ  := :services:events-collector-service

# Профили Compose: пусто - базовый стенд;
#	make up PROFILES=observability		- плюс Prometheus/Grafana/Loki/Tempo/Alloy
#	make up PROFILES=tools	- плюс Kafka UI
#	make up PROFILES="observability platform" - плюс окружение модуля
PROFILES ?=
COMPOSE       := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) $(foreach p, $(PROFILES),--profile $(p))

# Значение переменной в том виде, как его разобрал Compose из .env.
# Только для портов и имён - пароли берутся из окружения контейнера.0
compose_env = $$($(COMPOSE) config --environment | sed -n 's/^$(1)=//p')

# Одноразовые задачи: должны завершиться с кодом 0.
# Все остальные сервисы из compose считаются долгоживущими и обязаны стать healthy.
INIT_SERVICES := kafka-init minio-init
WAIT_TIMEOUT ?= 300

.DEFAULT_GOAL := help
.PHONY: help up up-obs down stop start restart ps logs health urls pull build clean reset kafka-reset \
        psql redis-cli clickhouse-cli topics diagrams check check-local test integration-test coverage

## ----------------------------------------------------------------- Справка
help: ## Показать список команд
	@echo "IoT Platform — доступные команды:"
	@echo ""
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""

## ------------------------------------------------------------ Жизненный цикл
up: ## Поднять всю инфраструктуру (PROFILES=observability - с наблюдаемостью) и дождаться готовности
	@echo "==> Запуск инфраструктуры IoT Platform (профили: $(if $(PROFILES),$(PROFILES),базовый))"
	@$(COMPOSE) up -d --build --remove-orphans
	@$(MAKE) --no-print-directory wait
	@$(MAKE) --no-print-directory urls

up-obs: ## То же, что make up PROFILES=observability
	@$(MAKE) --no-print-directory up PROFILES=observability

down: ## Остановить и удалить контейнеры всех профилей (тома сохраняются)
	@echo "==> Остановка инфраструктуры"
	@$(COMPOSE) --profile '*' down --remove-orphans

stop: ## Приостановить контейнеры без удаления
	@$(COMPOSE) stop

start: ## Запустить ранее остановленные контейнеры
	@$(COMPOSE) start

restart: ## Перезапустить всё окружение
	@$(MAKE) --no-print-directory down
	@$(MAKE) --no-print-directory up

clean: ## Удалить контейнеры всех профилей вместе с томами (данные будут потеряны)
	@echo "==> Удаление контейнеров и томов"
	@$(COMPOSE) --profile '*' down -v --remove-orphans

reset: clean up ## Полный пересоздание окружения с нуля

KAFKA_SERVICES := kafka kafka-init schema-registry kafka-exporter events-collector-service

kafka-reset: ## Пересоздать только том Kafka (сообщения, офсеты и схемы будут потеряны)
	@echo "!!! Будут удалены ВСЕ данные Kafka: сообщения во всех топиках, офсеты"
	@echo "!!! консьюмер-групп и Avro-схемы Schema Registry (топик _schemas)."
	@echo "    Данные PostgreSQL, ClickHouse, MinIO и остальных сервисов не затрагиваются."
	@[ "$(FORCE)" = "1" ] || { read -p "Продолжить? [y/N] " a; [ "$$a" = "y" ] || { echo "Отменено"; exit 1; };}
	@$(COMPOSE) --profile '*' rm -sf $(KAFKA_SERVICES)
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
	@printf "%-20s %-12s %-10s %s\n" "SERVICE" "STATE" "HEALTH" "EXIT"; \
	$(COMPOSE) ps -a --format '{{.Service}}\t{{.State}}\t{{.Health}}\t{{.ExitCode}}' | \
		awk -F'\t' '{printf "%-20s %-12s %-10s %s\n", $$1, $$2, ($$3 == "" ? "-" : $$3), $$4}'

wait: ## Дождаться, пока все сервисы станут healthy
	@echo "==> Ожидание готовности сервисов (до $(WAIT_TIMEOUT) с)"
	@init=" $(INIT_SERVICES) "; \
	services=$$($(COMPOSE) config --services) || exit 1; \
	deadline=$$(( $$(date +%s) + $(WAIT_TIMEOUT) )); \
	while true; do \
	  pending=""; failed=""; \
	  status=$$($(COMPOSE) ps -a --format '{{.Service}}|{{.State}}|{{.Health}}|{{.ExitCode}}') || exit 1; \
	  for s in $$services; do \
	    IFS='|' read -r _ state health code  <<<"$$(grep "^$$s|" <<<"$$status")"; \
	    if [[ "$$init" == *" $$s "* ]]; then \
	      case "$$state" in \
	      	exited) [ "$$code" = 0 ] || failed="$$failed $$s(код $$code)";; \
	     	*) pending="$$pending $$s";; \
	      esac; \
	    else \
	      case "$$state/$$health" in \
	      	running/healthy) ;; \
	     	running/) failed="$$failed $$s(нет healthcheck)" ;; \
	    	exited/*|dead/*) failed="$$failed $$s($$state)" ;; \
	   		*) pending="$$pending $$s" ;; \
	   	  esac; \
	   	fi; \
	  done; \
	  if [ -n "$$failed" ]; then echo "!!! Ошибка: $$failed"; $(MAKE) --no-print-directory health; exit 1; fi; \
	  if [ -z "$$pending" ]; then echo "==> Все сервисы healthy, init-задачи завершены с кодом 0"; break; fi; \
	  if [ $$(date +%s) -ge $$deadline ]; then \
	    echo "!!! Не дождались:$$pending"; $(MAKE) --no-print-directory health; exit 1; fi; \
	  echo "    ждём:$$pending"; sleep 10; \
	done

urls: ## Показать адреса всех сервисов
	@env=$$($(COMPOSE) config --environment); v() { sed -n "s/^$$1=//p" <<<"$$env"; }; \
	echo ""; \
	echo "  Сервис                    URL"; \
	echo "  ------------------------  ------------------------------------------------"; \
	port=$$($(COMPOSE) port events-collector-service 8080 2>/dev/null | head -1 | sed 's/.*://'); \
	echo "  events-collector-service  http://localhost:$${port:-<не запущен>}/actuator/health  (/actuator/prometheus; порт из диапазона $$(v EVENTS_COLLECTOR_PORTS))"; \
	echo "  Kafka                     localhost:$$(v KAFKA_EXTERNAL_PORT)"; \
	echo "  Schema Registry           http://localhost:$$(v SCHEMA_REGISTRY_PORT)"; \
	echo "  ClickHouse                http://localhost:$$(v CLICKHOUSE_HTTP_PORT)  (native: $$(v CLICKHOUSE_NATIVE_PORT))"; \
	echo "  Redis                     redis://localhost:$$(v REDIS_PORT)"; \
	echo "  --- профиль tools"; \
	echo "  Kafka UI                  http://localhost:$$(v KAFKA_UI_PORT)"; \
	echo "  --- профиль observability"; \
	echo "  Grafana                   http://localhost:$$(v GRAFANA_PORT)"; \
	echo "  Prometheus                http://localhost:$$(v PROMETHEUS_PORT)"; \
	echo "  Loki                      http://localhost:$$(v LOKI_PORT)"; \
	echo "  Tempo                     http://localhost:$$(v TEMPO_HTTP_PORT)  (OTLP: $$(v TEMPO_OTLP_GRPC_PORT)/$$(v TEMPO_OTLP_HTTP_PORT))"; \
	echo "  Alloy                     http://localhost:$$(v ALLOY_PORT)"; \
	echo "  kafka-exporter            http://localhost:$$(v KAFKA_EXPORTER_PORT)/metrics"; \
	echo "  --- профиль platform"; \
	echo "  PostgreSQL                postgresql://localhost:$$(v POSTGRES_PORT)/$$(v POSTGRES_DB)"; \
	echo "  postgres-exporter         http://localhost:$$(v POSTGRES_EXPORTER_PORT)/metrics"; \
	echo "  MinIO API                 http://localhost:$$(v MINIO_API_PORT)"; \
	echo "  MinIO Console             http://localhost:$$(v MINIO_CONSOLE_PORT)"; \
	echo "  Keycloak                  http://localhost:$$(v KEYCLOAK_PORT)"; \
	echo "  Camunda                   http://localhost:$$(v CAMUNDA_PORT)/camunda"; \
	echo ""

## ------------------------------------------------------------------ Клиенты
psql: ## Открыть psql в контейнере postgres
	@$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

redis-cli: ## Открыть redis-cli
	@$(COMPOSE) exec redis redis-cli

clickhouse-cli: ## Открыть clickhouse-client
	@$(COMPOSE) exec clickhouse sh -c 'exec clickhouse-client --user "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD"'

topics: ## Описание топиков Kafka (разделы, cleanup, policy)
	@$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:$(call compose_env,KAFKA_INTERNAL_PORT) --describe --exclude-internal

## ----------------------------------------------------------------- Проверки
check: ## Проверить корректность docker-compose.yaml
	@$(COMPOSE) --profile '*' config --quiet && echo "docker-compose.yaml — OK"

diagrams: ## Отрендерить C4-диаграммы в PNG (нужен plantuml)
	@command -v plantuml >/dev/null 2>&1 || { echo "plantuml не установлен: brew install plantuml"; exit 1; }
	@plantuml -tpng -o . $(ROOT_DIR)/diagrams/*.puml && echo "PNG сохранены в diagrams/"

## ----------------------------------------------------------------- events-collector-service
check-local: ## Быстрая проверка поднятого стенда (scripts/check-local.sh)
	@$(SERVICE_DIR)/scripts/check-local.sh

test: ## Модульные тесты сервиса (Docker не нужен)
	@$(GRADLE) $(SERVICE_PROJ):test

integration-test: ## Интеграционные тесты сервиса (Testcontainers, нужен Docker)
	@$(GRADLE) $(SERVICE_PROJ):integrationTest

coverage: ## Все тесты + проверка порога покрытия 65% + HTML-отчет
	@$(GRADLE) $(SERVICE_PROJ):check && echo "Отчет: $(SERVICE_DIR)/build/reports/jacoco/test/html/index.html"
