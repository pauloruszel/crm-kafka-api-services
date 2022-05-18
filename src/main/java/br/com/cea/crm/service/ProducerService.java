package br.com.cea.crm.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Service
public final class ProducerService {
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic}")
    private String topic;

    public ProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Bloqueia a thread usando ListenableFuturee e a thread aguardará o resultado,
     * caso precise do resultado, mas essa implementação retardará o processo
     *
     * @param message message
     */
    public void sendMessageWithResult(String message) {
        log.info(String.format("$$$$ => Produzindo mensagem: %s", message));

        ListenableFuture<SendResult<String, String>> future = this.kafkaTemplate.send(topic, message);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                log.info("Não foi possível enviar a mensagem=[ {} ] devido a : {}", message, ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("Mensagem enviada=[ {} ] com offset=[ {} ]", message, result.getRecordMetadata().offset());
            }
        });

    }

    /**
     * Se você não deseja obter o resultado, mantenha apenas o log.
     * Não é recomendado bloquear o produtor, pois o
     * Kafka é um plataforma de processamento de fluxo rápido.
     *
     * @param message message
     */
    public void sendMessage(String message) {
        log.info("Produzindo mensagem= '{}' para o tópico= '{}'", message, topic);
        this.kafkaTemplate.send(topic, message)
                .addCallback(
                    result -> log.info("Mensagem enviada ao tópico: {}", message),
                    ex -> log.error("Falha no envio da mensagem ", ex)
        );
    }


}
