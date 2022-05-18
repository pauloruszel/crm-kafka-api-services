package br.com.cea.crm.service;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public final class ConsumerService {

    @Getter
    @Setter
    private String payload = null;

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.consumerGroupId}")
    public void consume(String message) {
        log.info(String.format("$$$$ => Mensagem consumida: %s", message));
        setPayload(message);
    }

}
