package com.github.gluhov.iotplatform.eventscollector.domain.model;

/** Результат атомарной записи отметок публикации устройства в Redis. */
public enum PublicationMarkResult {
    /** Аренда наша, отметки записаны. */
    WRITTEN,
    /** Аренда наша, но в Redis уже более свежий снимок: время снимка и отпечаток не тронуты. */
    STALE,
    /** Аренда не наша (истекла или взята другим экземпляром): в Redis ничего не записано. */
    LEASE_LOST
}