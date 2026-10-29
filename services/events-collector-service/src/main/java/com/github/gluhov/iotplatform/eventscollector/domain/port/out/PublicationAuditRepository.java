package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

import com.github.gluhov.iotplatform.eventscollector.domain.model.PublicationAuditRecord;

public interface PublicationAuditRepository {

    void insert(PublicationAuditRecord record);
}