package purple.order;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

	private final KafkaTemplate<Integer, Object> kafkaTemplate;

	public OrderController(KafkaTemplate<Integer, Object> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@PostMapping("/create")
	@ResponseStatus(HttpStatus.ACCEPTED)
	public Map<String, Object> createOrder(
			@RequestParam("orderId") Integer orderId,
			@RequestParam("itemName") String itemName) {
		Map<String, Object> event = Map.of(
				"eventType", "ORDER_CREATED",
				"payload", Map.of("orderId", orderId, "itemName", itemName));

		// The order ID is the Kafka key, keeping events for the same order together.
		// join() waits for Kafka to acknowledge the event before returning success.
		kafkaTemplate.send("order-events", orderId, event).join();

		return event;
	}
}
