package com.github.gluhov.iotplatform.failedeventsprocessor.domain.model;

/** Результат записи объекта: новый объект или объект с этим ключом уже есть (повторная доставка). */
public enum PutOutcome {
    CREATED,
    DUPLICATE
}
