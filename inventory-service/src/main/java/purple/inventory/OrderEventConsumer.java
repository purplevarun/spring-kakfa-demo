package purple.inventory;

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
import org.springframework.transaction.annotation.Transactional;

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

	@Transactional
	@KafkaListener(id = "inventory-listener", idIsGroup = false, topics = "order-events")
	public void consumeOrderEvent(ConsumerRecord<Integer, Map<String, Object>> event) {
		String eventType = (String) event.value().get("eventType");
		if (!"ORDER_CREATED".equals(eventType)) {
			return;
		}
		Map<?, ?> payload = (Map<?, ?>) event.value().get("payload");
		String itemName = (String) payload.get("itemName");
		UUID processedOrderId = UUID.randomUUID();
		int inserted = database.update("""
				INSERT INTO processed_orders (id, order_id, event_type, item_name)
				VALUES (?, ?, ?, ?)
				ON CONFLICT (order_id, event_type) DO NOTHING
				""", processedOrderId, event.key(), eventType, itemName);

		String outcome = "PROCESSED";
		if (inserted == 0) {
			Map<String, Object> existing = database.queryForMap(
					"SELECT id, item_name FROM processed_orders WHERE order_id = ? AND event_type = ?",
					event.key(), eventType);
			processedOrderId = (UUID) existing.get("id");
			outcome = itemName.equals(existing.get("item_name")) ? "DUPLICATE" : "CONFLICT";
		}

		database.update("""
				INSERT INTO event_deliveries
				(id, processed_order_id, order_id, event_type, item_name, topic, partition_id, kafka_offset, outcome)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT (topic, partition_id, kafka_offset) DO NOTHING
				""", UUID.randomUUID(), processedOrderId, event.key(), eventType, itemName,
				event.topic(), event.partition(), event.offset(), outcome);

		String consumedAt = IST_FORMATTER.format(Instant.now());
		logger.info("inventory service consumed event at {} for order id {}: {}", consumedAt, event.key(), outcome);
	}
}
