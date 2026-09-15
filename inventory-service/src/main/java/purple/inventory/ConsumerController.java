package purple.inventory;

import java.util.Map;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/consumer")
public class ConsumerController {

	private final JdbcTemplate database;
	private final KafkaListenerEndpointRegistry listeners;

	public ConsumerController(JdbcTemplate database, KafkaListenerEndpointRegistry listeners) {
		this.database = database;
		this.listeners = listeners;
	}

	@GetMapping
	@Transactional(readOnly = true)
	public Map<String, Object> snapshot() {
		MessageListenerContainer listener = listeners.getListenerContainer("inventory-listener");
		return Map.of(
				"service", "inventory", "groupId", "inventory-group",
				"pauseRequested", listener.isPauseRequested(), "paused", listener.isContainerPaused(),
				"running", listener.isRunning(),
				"counts", database.queryForMap("""
						SELECT COUNT(*) AS deliveries,
						COUNT(*) FILTER (WHERE outcome = 'PROCESSED') AS processed,
						COUNT(*) FILTER (WHERE outcome = 'DUPLICATE') AS duplicates,
						COUNT(*) FILTER (WHERE outcome = 'CONFLICT') AS conflicts
						FROM event_deliveries
						"""),
				"orders", database.query("SELECT * FROM processed_orders ORDER BY processed_at DESC LIMIT 200",
						(row, rowNumber) -> Map.of("id", row.getObject("id", UUID.class),
								"orderId", row.getInt("order_id"), "itemName", row.getString("item_name"),
								"eventType", row.getString("event_type"),
								"processedAt", row.getTimestamp("processed_at").toInstant().toString())),
				"deliveries", database.query("SELECT * FROM event_deliveries ORDER BY received_at DESC LIMIT 200",
						(row, rowNumber) -> Map.of("id", row.getObject("id", UUID.class),
								"orderId", row.getInt("order_id"), "itemName", row.getString("item_name"),
								"eventType", row.getString("event_type"), "partition", row.getInt("partition_id"),
								"offset", row.getLong("kafka_offset"), "outcome", row.getString("outcome"),
								"receivedAt", row.getTimestamp("received_at").toInstant().toString())));
	}

	@PostMapping("/pause")
	public Map<String, Boolean> setPaused(@RequestParam("paused") boolean paused) {
		MessageListenerContainer listener = listeners.getListenerContainer("inventory-listener");
		if (paused) {
			listener.pause();
		} else {
			listener.resume();
		}
		return Map.of("pauseRequested", listener.isPauseRequested());
	}
}