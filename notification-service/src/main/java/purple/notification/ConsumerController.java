package purple.notification;

import java.util.Map;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConsumerController {

	private final JdbcTemplate database;

	public ConsumerController(JdbcTemplate database) {
		this.database = database;
	}

	@GetMapping("/consumer")
	public Map<String, Object> snapshot() {
		return Map.of("orders", database.query("""
				SELECT id, order_id, item_name, processed_at FROM processed_orders
				WHERE event_type = 'ORDER_CREATED'
				ORDER BY processed_at DESC, id
				""", (row, rowNumber) -> Map.of(
						"id", row.getObject("id", UUID.class),
						"orderId", row.getInt("order_id"),
						"itemName", row.getString("item_name"),
						"processedAt", row.getTimestamp("processed_at").toInstant().toString())));
	}
}