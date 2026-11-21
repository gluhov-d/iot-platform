package com.github.gluhov.iotplatform.deviceservice.it;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ContainerPort;
import com.github.dockerjava.api.model.Image;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.ComposeContainer;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Сквозной тест на полном стенде платформы: весь infrastructure/docker-compose.yaml поднимается через ComposeContainer
 * под своим проектом {@value #PROJECT}. Путь: PUT на одной реплике device-service -> Kafka -> device-collector-service ->
 * реплика шарда -> GET на другой реплике device-service с новыми данными не позже чем через 5 с (SC-001, FR-026).
 * <p>
 * Тест не входит в integrationTest и check: задача e2eTest (тег e2e), стенд не помещается в память VPS и
 * конфликтует с рабочим стендом. Предусловия проверяются до старта, и при их нарушении тест падает с понятным
 * сообщением (не пропускается). Файл .env создаётся во временном каталоге из .env.example; реальный .env не читается.
 */
@Tag("e2e")
class ItEndToEndTest {

    private static final String PROJECT = "iot-e2e";
    private static final Path REPO_ROOT = Path.of("../..").toAbsolutePath().normalize();
    private static final Path COMPOSE_FILE = REPO_ROOT.resolve("infrastructure/docker-compose.yaml");
    private static final Path ENV_EXAMPLE = REPO_ROOT.resolve(".env.example");
    private static final String EULA_VARIABLE = "E2E_NEXUS_ACCEPT_EULA";
    private static final String COMPOSE_PROJECT_LABEL = "com.docker.compose.project";
    private static final String COMPOSE_SERVICE_LABEL = "com.docker.compose.service";

    // Сервисы, которые тест запрашивает у Compose; их зависимости Compose поднимает сам
    private static final String[] STARTED_SERVICES = {"device-service", "device-collector-service"};
    private static final String DEVICE_SERVICE = "device-service";
    private static final String DEVICE_COLLECTOR_SERVICE = "device-collector-service";
    private static final int DEVICE_SERVICE_REPLICAS = 2;
    private static final int APP_PORT = 8080;

    // container_name из compose: фиксированные имена, поэтому чужой запущенный стенд с ними сразу заметен
    private static final List<String> FIXED_CONTAINER_NAMES = List.of(
            "iot-redis", "iot-redis-replica", "iot-redis-sentinel-1", "iot-redis-sentinel-2", "iot-redis-sentinel-3",
            "iot-kafka", "iot-kafka-init", "iot-schema-registry", "iot-postgres-shard-0", "iot-postgres-shard-1",
            "iot-postgres-shard-0-replica", "iot-postgres-shard-1-replica", "iot-shard-replication-init",
            "iot-db-migrator", "iot-shardingsphere-proxy", "iot-nexus", "iot-nexus-init", "iot-avros-publish",
            "iot-schema-register", "iot-keycloak");

    private static final Duration STACK_START_TIMEOUT = Duration.ofMinutes(15);
    private static final Duration READY_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration READY_POLL = Duration.ofSeconds(2);
    // Создание: сообщение обрабатывает device-collector-service асинхронно (как TIMEOUT в quick-check.sh)
    private static final Duration CREATE_TIMEOUT = Duration.ofSeconds(60);
    // Изменение и удаление видны в GET не позже чем через 5 с (SC-001, R7)
    private static final Duration SLA = Duration.ofSeconds(5);
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);

    private static final String DEVICES_PATH = "/api/v1/devices/";
    private static final String TENANT = "e2e-tenant";
    private static final String DEVICE_TYPE = "e2e-sensor";
    private static final Pattern ACCESS_TOKEN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();

    private static ComposeContainer stand;
    private static Path tempDir;
    private static Path envFile;
    private static Path generatedCompose;
    private static Map<String, String> env;

    @BeforeAll
    static void startStand() throws IOException, InterruptedException {
        requireDocker();
        requireFreeFixedContainerNames();
        tempDir = Files.createTempDirectory("iot-e2e-");
        envFile = tempDir.resolve(".env");
        env = createEnvFile(envFile);
        requireBuiltImages();

        // Образы собраны предыдущим make up: без пересборки; публикация avros - в одноразовом Nexus проекта
        generatedCompose = writeComposeWithoutContainerNames();
        stand = new ComposeContainer(PROJECT, generatedCompose.toFile())
                .withEnv(env)
                .withServices(STARTED_SERVICES)
                .withBuild(false)
                .withPull(false)
                .withTailChildContainers(true)
                .withRemoveVolumes(true)
                .withStartupTimeout(STACK_START_TIMEOUT);
        stand.start();
        awaitReady();
    }

    /**
     * ComposeContainer не поддерживает container_name (проверено: IllegalStateException на первом сервисе), а в общем
     * compose-файле платформы они заданы. Поэтому тест запускает копию файла без этих строк; копия лежит рядом с
     * оригиналом, чтобы относительные пути (контексты сборки, config/) остались верными, и удаляется после теста.
     */
    private static Path writeComposeWithoutContainerNames() throws IOException {
        Path target = COMPOSE_FILE.resolveSibling("docker-compose.e2e-generated.yaml");
        List<String> lines = Files.readAllLines(COMPOSE_FILE);
        Files.write(target, lines.stream().filter(line -> !line.trim().startsWith("container_name:")).toList());
        return target;
    }

    @AfterAll
    static void stopStand() throws IOException {
        try {
            if (stand != null) {
                // Убираются контейнеры, сети и тома только проекта iot-e2e
                stand.stop();
            }
        } finally {
            if (generatedCompose != null) {
                Files.deleteIfExists(generatedCompose);
            }
            if (envFile != null) {
                Files.deleteIfExists(envFile);
            }
            if (tempDir != null) {
                Files.deleteIfExists(tempDir);
            }
        }
    }

    @Test
    void givenFullStand_whenDeviceCreatedUpdatedDeleted_thenReplicaReadsFollowWithinSla() throws Exception {
        List<String> bases = replicaBases();
        // Запись - на первой реплике, чтение - на второй: проверяем, что изменение дошло через Kafka и шард
        String writeBase = bases.get(0);
        String readBase = bases.get(1);
        String token = consumerToken();
        String deviceId = "e2e-" + UUID.randomUUID();
        String devicePath = DEVICES_PATH + deviceId;

        Instant postSent = Instant.now();
        HttpResponse<String> created = send("POST", writeBase + DEVICES_PATH.substring(0, DEVICES_PATH.length() - 1),
                token, deviceBody(deviceId, "ACTIVE", "e2e-v1"));
        assertThat(created.statusCode()).as("POST %s: HTTP %s, ожидался 202", deviceId, created.statusCode())
                .isEqualTo(202);
        Observation read = awaitRead(readBase, devicePath, token, present("e2e-v1"), CREATE_TIMEOUT, postSent);
        assertThat(read.matched())
                .as("создание не видно на второй реплике за %s (последний HTTP %d)", CREATE_TIMEOUT, read.status())
                .isTrue();
        System.out.printf("E2E: создание видно через %d мс%n", read.elapsed().toMillis());

        Instant putSent = Instant.now();
        HttpResponse<String> updated = send("PUT", writeBase + devicePath, token,
                deviceBody(null, "MAINTENANCE", "e2e-v2"));
        assertThat(updated.statusCode()).as("PUT %s: HTTP %s, ожидался 202", deviceId, updated.statusCode())
                .isEqualTo(202);
        Observation changed = awaitRead(readBase, devicePath, token, present("e2e-v2"), SLA, putSent);
        assertThat(changed.matched())
                .as("SC-001: новые данные не видны на второй реплике за %s (последний HTTP %d)", SLA, changed.status())
                .isTrue();
        assertThat(changed.elapsed())
                .as("SC-001: изменение видно через %d мс, лимит %s", changed.elapsed().toMillis(), SLA)
                .isLessThanOrEqualTo(SLA);
        System.out.printf("E2E: изменение (PUT -> GET с новыми данными) за %d мс, лимит %s%n",
                changed.elapsed().toMillis(), SLA);

        Instant deleteSent = Instant.now();
        HttpResponse<String> deleted = send("DELETE", writeBase + devicePath, token, null);
        assertThat(deleted.statusCode()).as("DELETE %s: HTTP %s, ожидался 202", deviceId, deleted.statusCode())
                .isEqualTo(202);
        Observation gone = awaitRead(readBase, devicePath, token, notFound(), SLA, deleteSent);
        assertThat(gone.matched())
                .as("SC-001: устройство не исчезло со второй реплики за %s (последний HTTP %d)", SLA, gone.status())
                .isTrue();
        assertThat(gone.elapsed())
                .as("SC-001: удаление видно через %d мс, лимит %s", gone.elapsed().toMillis(), SLA)
                .isLessThanOrEqualTo(SLA);
        System.out.printf("E2E: удаление (DELETE -> GET 404) за %d мс, лимит %s%n", gone.elapsed().toMillis(), SLA);
    }

    // ------------------------------------------------------------------ Предусловия

    private static void requireDocker() {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            throw new IllegalStateException("Docker недоступен: запустите Docker и повторите e2eTest");
        }
    }

    /** Тест не гасит чужой стенд: если фиксированное имя контейнера уже занято, тест падает до старта. */
    private static void requireFreeFixedContainerNames() {
        Map<String, Container> byName = new LinkedHashMap<>();
        for (Container container : docker().listContainersCmd().withShowAll(true).exec()) {
            for (String name : Objects.requireNonNullElse(container.getNames(), new String[0])) {
                // Имена в Docker API начинаются с "/"
                byName.put(name.startsWith("/") ? name.substring(1) : name, container);
            }
        }
        for (String fixedName : FIXED_CONTAINER_NAMES) {
            Container busy = byName.get(fixedName);
            if (busy != null) {
                throw new IllegalStateException("Контейнер " + fixedName + " уже есть (состояние: " + busy.getState()
                        + ", проект: " + projectOf(busy) + "). Тест не трогает чужой стенд: остановите его"
                        + " (make down) и повторите");
            }
        }
    }

    /** Файл .env во временном каталоге: значения из .env.example, секреты - случайные, проект и EULA - тестовые. */
    private static Map<String, String> createEnvFile(Path file) throws IOException {
        String eula = System.getenv(EULA_VARIABLE);
        if (!"true".equals(eula)) {
            throw new IllegalStateException("Задайте переменную окружения " + EULA_VARIABLE + "=true: условия"
                    + " лицензии Nexus CE принимает человек (https://links.sonatype.com/products/nxrm/ce-eula)");
        }
        Map<String, String> template = parseEnv(ENV_EXAMPLE);
        StringBuilder content = new StringBuilder();
        for (Map.Entry<String, String> entry : template.entrySet()) {
            String value = entry.getValue();
            if (value.startsWith("change_me")) {
                value = UUID.randomUUID().toString().replace("-", "");
            }
            content.append(entry.getKey()).append('=').append(value).append('\n');
        }
        content.append("COMPOSE_PROJECT_NAME=").append(PROJECT).append('\n');
        content.append("NEXUS_ACCEPT_EULA=").append(eula).append('\n');
        Files.writeString(file, content, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        try {
            Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // Файловая система без POSIX-прав (Windows): права не меняем
        }
        // Значения берём из созданного файла: он и есть источник настроек стенда
        return parseEnv(file);
    }

    private static void requireBuiltImages() {
        List<String> images = List.of(
                "iot-platform/" + DEVICE_SERVICE + ":" + required("DEVICE_SERVICE_IMAGE_TAG"),
                "iot-platform/" + DEVICE_COLLECTOR_SERVICE + ":" + required("DEVICE_COLLECTOR_IMAGE_TAG"),
                "iot-platform/db-migrator:" + required("DB_MIGRATOR_IMAGE_TAG"),
                "iot-platform/avros-publish:" + required("AVROS_IMAGE_TAG"));
        Set<String> present = docker().listImagesCmd().exec().stream()
                .flatMap(image -> Arrays.stream(Objects.requireNonNullElse(image.getRepoTags(), new String[0])))
                .collect(Collectors.toSet());
        for (String image : images) {
            if (!present.contains(image)) {
                throw new IllegalStateException("Нет образа " + image + ": сначала make up (он собирает образы стенда)");
            }
        }
    }

    // ------------------------------------------------------------------ Готовность и адреса

    private static void awaitReady() throws InterruptedException {
        Instant deadline = Instant.now().plus(READY_TIMEOUT);
        while (true) {
            String report = readinessReport();
            if (report == null) {
                return;
            }
            if (!Instant.now().isBefore(deadline)) {
                throw new IllegalStateException("Стенд не готов за " + READY_TIMEOUT + ": " + report
                        + ". Логи: docker compose -p " + PROJECT + " logs");
            }
            Thread.sleep(READY_POLL.toMillis());
        }
    }

    /** null - готов: две реплики device-service и device-collector-service, все healthy. */
    private static String readinessReport() {
        List<Container> service = stackContainers(DEVICE_SERVICE);
        List<Container> collector = stackContainers(DEVICE_COLLECTOR_SERVICE);
        long serviceHealthy = service.stream().filter(ItEndToEndTest::isHealthy).count();
        long collectorHealthy = collector.stream().filter(ItEndToEndTest::isHealthy).count();
        if (service.size() == DEVICE_SERVICE_REPLICAS && serviceHealthy == service.size()
                && !collector.isEmpty() && collectorHealthy == collector.size()) {
            return null;
        }
        return "device-service healthy %d из %d (нужно %d), device-collector-service healthy %d из %d"
                .formatted(serviceHealthy, service.size(), DEVICE_SERVICE_REPLICAS, collectorHealthy, collector.size());
    }

    /** Базовые адреса реплик на хосте (http://localhost:порт), по имени контейнера - стабильный порядок. */
    private static List<String> replicaBases() {
        List<Container> replicas = stackContainers(DEVICE_SERVICE).stream()
                .sorted(Comparator.comparing(c -> c.getNames()[0]))
                .toList();
        List<String> bases = new java.util.ArrayList<>();
        for (Container replica : replicas) {
            ContainerPort port = Arrays.stream(Objects.requireNonNullElse(replica.getPorts(), new ContainerPort[0]))
                    .filter(p -> p.getPrivatePort() != null && p.getPrivatePort() == APP_PORT && p.getPublicPort() != null)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Реплика device-service без опубликованного порта " + APP_PORT));
            bases.add("http://localhost:" + port.getPublicPort());
        }
        if (bases.size() < 2) {
            throw new IllegalStateException("Ожидались " + DEVICE_SERVICE_REPLICAS + " реплики device-service, найдено "
                    + bases.size());
        }
        return bases;
    }

    /** Сервисный токен клиента device-service-consumer (client credentials) с хоста, порт Keycloak - из стенда. */
    private static String consumerToken() throws IOException, InterruptedException {
        String secret = required("DEVICE_SERVICE_CONSUMER_SECRET");
        String url = "http://localhost:%s/realms/%s/protocol/openid-connect/token"
                .formatted(required("KEYCLOAK_PORT"), required("KEYCLOAK_REALM"));
        String form = "grant_type=client_credentials&client_id=device-service-consumer&client_secret="
                + URLEncoder.encode(secret, StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(HTTP_TIMEOUT)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            // Тело ответа не печатаем: там может быть информация о клиенте
            throw new IllegalStateException("Keycloak не выдал токен клиента device-service-consumer: HTTP "
                    + response.statusCode() + " (см. docker compose logs keycloak)");
        }
        Matcher matcher = ACCESS_TOKEN.matcher(response.body());
        if (!matcher.find()) {
            throw new IllegalStateException("В ответе Keycloak нет access_token (тело не печатается)");
        }
        return matcher.group(1);
    }

    // ------------------------------------------------------------------ Запросы и ожидание

    private record Observation(boolean matched, int status, Duration elapsed) {
    }

    /** Опрашивает GET на реплике, пока условие не выполнится или не истечёт лимит, отсчитанный от since. */
    private static Observation awaitRead(String base, String devicePath, String token,
                                         Predicate<HttpResponse<String>> expected, Duration limit, Instant since)
            throws IOException, InterruptedException {
        Instant deadline = since.plus(limit);
        while (true) {
            HttpResponse<String> response = send("GET", base + devicePath, token, null);
            boolean matched = expected.test(response);
            Duration elapsed = Duration.between(since, Instant.now());
            if (matched) {
                return new Observation(true, response.statusCode(), elapsed);
            }
            if (!Instant.now().isBefore(deadline)) {
                return new Observation(false, response.statusCode(), elapsed);
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }
    }

    private static Predicate<HttpResponse<String>> present(String name) {
        Pattern namePattern = Pattern.compile("\"name\"\\s*:\\s*\"" + Pattern.quote(name) + "\"");
        return response -> response.statusCode() == 200 && namePattern.matcher(response.body()).find();
    }

    private static Predicate<HttpResponse<String>> notFound() {
        return response -> response.statusCode() == 404 && response.body().contains("DEVICE_NOT_FOUND");
    }

    private static HttpResponse<String> send(String method, String url, String token, String body)
            throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .timeout(HTTP_TIMEOUT)
                .header("Authorization", "Bearer " + token);
        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(body));
        }
        return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    /** Тело POST и PUT. deviceId = null - для PUT (идентификатор в пути). */
    private static String deviceBody(String deviceId, String status, String name) {
        String id = deviceId == null ? "" : "\"deviceId\":\"" + deviceId + "\",";
        return "{" + id + "\"tenantId\":\"" + TENANT + "\",\"deviceType\":\"" + DEVICE_TYPE
                + "\",\"status\":\"" + status + "\",\"name\":\"" + name + "\"}";
    }

    // ------------------------------------------------------------------ Служебное

    private static DockerClient docker() {
        return DockerClientFactory.instance().client();
    }

    private static List<Container> stackContainers(String service) {
        // ComposeContainer добавляет к имени проекта случайный суффикс (iot-e2eXXXXXXXX): проект сверяется по префиксу
        return docker().listContainersCmd().withShowAll(true)
                .withLabelFilter(Map.of(COMPOSE_SERVICE_LABEL, service))
                .exec().stream()
                .filter(container -> projectOf(container).startsWith(PROJECT))
                .toList();
    }

    private static boolean isHealthy(Container container) {
        var state = docker().inspectContainerCmd(container.getId()).exec().getState();
        return state != null && state.getHealth() != null && "healthy".equals(state.getHealth().getStatus());
    }

    private static String projectOf(Container container) {
        Map<String, String> labels = container.getLabels();
        return labels == null || labels.get(COMPOSE_PROJECT_LABEL) == null
                ? "без проекта Compose" : labels.get(COMPOSE_PROJECT_LABEL);
    }

    private static String required(String key) {
        String value = env.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("В тестовом .env нет переменной " + key + " (проверьте .env.example)");
        }
        return value;
    }

    /** Разбор KEY=VALUE: пустые строки и комментарии пропускаются; значение - всё после первого '='. */
    private static Map<String, String> parseEnv(Path file) throws IOException {
        Map<String, String> values = new LinkedHashMap<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            String trimmed = line.strip();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            int eq = trimmed.indexOf('=');
            if (eq <= 0) {
                throw new IllegalStateException("Некорректная строка в " + file.getFileName() + " (нет '=')");
            }
            values.put(trimmed.substring(0, eq).strip(), trimmed.substring(eq + 1).strip());
        }
        return values;
    }
}
