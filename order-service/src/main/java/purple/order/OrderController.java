package purple.order;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class OrderController {

	private final KafkaTemplate<Integer, Object> kafkaTemplate;

	public OrderController(KafkaTemplate<Integer, Object> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@PostMapping("/create")
	public ResponseEntity<Map<String, Object>> createOrder(
			@RequestParam("orderId") Integer orderId,
			@RequestParam("itemName") String itemName) {
		if (itemName.isBlank() || itemName.length() > 160) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "itemName must contain 1 to 160 characters");
		}
		Map<String, Object> event = Map.of(
				"eventType", "ORDER_CREATED",
				"payload", Map.of("orderId", orderId, "itemName", itemName));

		// The order ID is the Kafka key, keeping events for the same order together.
		// join() waits for Kafka to acknowledge the event before returning success.
		var metadata = kafkaTemplate.send("order-events", orderId, event).join().getRecordMetadata();

		return ResponseEntity.accepted()
				.header("X-Kafka-Partition", Integer.toString(metadata.partition()))
				.header("X-Kafka-Offset", Long.toString(metadata.offset()))
				.body(event);
	}
}
