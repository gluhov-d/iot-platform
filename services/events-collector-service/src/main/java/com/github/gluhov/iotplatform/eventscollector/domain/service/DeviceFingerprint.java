package com.github.gluhov.iotplatform.eventscollector.domain.service;

import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;

/**
 * Отпечаток данных устройства: SHA-256 от набора полей, описывающих само устройство.
 * <p>
 * В отпечаток входят: идентификаторы, тип, серийный номер, производитель, модель, прошивка, имя, ОС и её версия,
 * статус, время регистрации, настройки и атрибуты. Не входят {@code lastSeenAtMs} и {@code sensors}: время
 * последней активности и показания сенсоров меняются с каждым событием, и по ним устройство в реестре не меняется.
 * <p>
 * Значения кодируются с длиной (null отличается от пустой строки, граница между полями не сдвигается), карты
 * упорядочены по ключу: один и тот же набор данных всегда даёт один и тот же отпечаток. Начальный маркер версии
 * формата позволит изменить набор полей: старые отпечатки в Redis перестанут совпадать, и каждое устройство один раз
 * опубликуется заново (повтор безопасен: device-collector-service идемпотентен).
 */
public final class DeviceFingerprint {
    private static final String FORMAT = "device-fingerprint-v1";

    private DeviceFingerprint() {
    }

    /** Шестнадцатеричная строка из 64 символов. */
    public static String of(DeviceSnapshot snapshot) {
        MessageDigest digest = sha256();
        put(digest, FORMAT);
        field(digest, "deviceId", snapshot.getDeviceId());
        field(digest, "tenantId", snapshot.getTenantId());
        field(digest, "ownerId", snapshot.getOwnerId());
        field(digest, "deviceType", snapshot.getDeviceType());
        field(digest, "serialNumber", snapshot.getSerialNumber());
        field(digest, "manufacturer", snapshot.getManufacturer());
        field(digest, "model", snapshot.getModel());
        field(digest, "firmwareVersion", snapshot.getFirmwareVersion());
        field(digest, "name", snapshot.getName());
        field(digest, "osName", snapshot.getOsName());
        field(digest, "osVersion", snapshot.getOsVersion());
        field(digest, "status", snapshot.getStatus() == null ? null : snapshot.getStatus().name());
        field(digest, "registeredAtMs", snapshot.getRegisteredAtMs() == null ? null : snapshot.getRegisteredAtMs().toString());
        map(digest, "settings", snapshot.getSettings());
        map(digest, "attributes", snapshot.getAttributes());
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void field(MessageDigest digest, String name, String value) {
        put(digest, name);
        put(digest, value);
    }

    private static void map(MessageDigest digest, String name, Map<String, String> values) {
        put(digest, name);
        Map<String, String> sorted = new TreeMap<>();
        if (values != null) {
            values.forEach((key, value) -> sorted.put(String.valueOf(key), value));
        }
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(sorted.size()).array());
        sorted.forEach((key, value) -> {
            put(digest, key);
            put(digest, value);
        });
    }

    // Длина (-1 - null) и байты UTF-8: без разделителей, которые могли бы встретиться в значении
    private static void put(MessageDigest digest, String value) {
        if (value == null) {
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(-1).array());
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}