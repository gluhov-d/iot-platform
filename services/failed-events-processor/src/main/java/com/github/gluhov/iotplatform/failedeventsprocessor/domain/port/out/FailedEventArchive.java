package com.github.gluhov.iotplatform.failedeventsprocessor.domain.port.out;

import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;

import java.util.Map;

/**
 * Порт архива: условная запись объекта. Повторная запись того же ключа не создаёт новой версии
 * и возвращает {@link PutOutcome#DUPLICATE}.
 */
public interface FailedEventArchive {

    /**
     * @throws com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException
     *         архив недоступен или не ответил в срок: повтор
     * @throws com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveDeniedException
     *         архив отказал в доступе: повтор не поможет, нужна настройка учётки
     */
    PutOutcome put(String key, byte[] body, Map<String, String> metadata);
}
