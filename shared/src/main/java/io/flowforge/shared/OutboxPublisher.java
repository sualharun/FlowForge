package io.flowforge.shared;

import java.util.concurrent.TimeUnit;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/** At-least-once relay: publish acknowledgement precedes the database published marker. */
@Component
@ConditionalOnProperty(name="flowforge.outbox.enabled",havingValue="true",matchIfMissing=true)
public class OutboxPublisher {
    private final JdbcTemplate db;
    private final KafkaTemplate<String,String> kafka;
    private final TransactionTemplate transactions;
    public OutboxPublisher(JdbcTemplate db,KafkaTemplate<String,String> kafka,TransactionTemplate transactions) {
        this.db=db;this.kafka=kafka;this.transactions=transactions;
    }
    @Scheduled(fixedDelayString="${flowforge.outbox.interval-ms:100}")
    public void publish() {
        try {
            transactions.executeWithoutResult(transaction -> {
                var messages=db.queryForList("SELECT id,topic,message_key,payload::text AS payload FROM outbox WHERE published_at IS NULL ORDER BY created_at,id LIMIT 25 FOR UPDATE SKIP LOCKED");
                for (var message : messages) {
                    try {
                        kafka.send((String)message.get("topic"),(String)message.get("message_key"),(String)message.get("payload")).get(10,TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();throw new IllegalStateException("Outbox interrupted",e);
                    } catch (Exception e) { throw new IllegalStateException("Kafka publish failed; outbox will retry",e); }
                    db.update("UPDATE outbox SET published_at=clock_timestamp() WHERE id=?",message.get("id"));
                }
            });
        } catch (RuntimeException e) {
            LoggerFactory.getLogger(OutboxPublisher.class).warn("Outbox batch retained for retry: {}",e.getMessage());
        }
    }
}
