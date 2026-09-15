package purple.notification;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

	private static final Logger logger = LoggerFactory.getLogger(OrderEventConsumer.class);
	private static final DateTimeFormatter IST_FORMATTER = DateTimeFormatter
			.ofPattern("dd MMM yyyy, hh:mm:ss a 'IST'", Locale.ENGLISH)
			.withZone(ZoneId.of("Asia/Kolkata"));
	private final JdbcTemplate database;

	public OrderEventConsumer(JdbcTemplate database) {
		this.database = database;
	}

	@KafkaListener(topics = "order-events")
	public void consumeOrderEvent(ConsumerRecord<Integer, Map<String, Object>> event) {
		String eventType = (String) event.value().get("eventType");
		if (!"ORDER_CREATED".equals(eventType)) {
			return;
		}
		Map<?, ?> payload = (Map<?, ?>) event.value().get("payload");
		String itemName = (String) payload.get("itemName");
		int inserted = database.update("""
				INSERT INTO processed_orders (id, order_id, event_type, item_name)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (order_id, event_type) DO NOTHING
				""", UUID.randomUUID(), event.key(), eventType, itemName);

		String consumedAt = IST_FORMATTER.format(Instant.now());
		logger.info("notification service consumed event at {} for order id {}: {}",
				consumedAt, event.key(), inserted == 1 ? "saved" : "already saved, skipped");
	}
}
