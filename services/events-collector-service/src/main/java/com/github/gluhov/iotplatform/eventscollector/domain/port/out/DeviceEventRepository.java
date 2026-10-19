package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

import com.github.gluhov.iotplatform.eventscollector.domain.model.DeviceEventRecord;

public interface DeviceEventRepository {

    void insert(DeviceEventRecord record);
}
