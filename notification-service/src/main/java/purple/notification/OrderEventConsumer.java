package purple.notification;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

	private static final Logger logger = LoggerFactory.getLogger(OrderEventConsumer.class);
	private static final DateTimeFormatter IST_FORMATTER = DateTimeFormatter
			.ofPattern("dd MMM yyyy, hh:mm:ss a 'IST'", Locale.ENGLISH)
			.withZone(ZoneId.of("Asia/Kolkata"));

	@KafkaListener(topics = "order-events")
	public void consumeOrderEvent(ConsumerRecord<Integer, Map<String, Object>> event) {
		String consumedAt = IST_FORMATTER.format(Instant.now());
		logger.info("notification service consumed event at {} for order id {}", consumedAt, event.key());
	}
}
