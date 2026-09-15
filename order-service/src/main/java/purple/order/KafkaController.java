package purple.order;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class KafkaController {

	private static final String TOPIC = "order-events";
	private final Admin kafka;

	public KafkaController(Admin kafka) {
		this.kafka = kafka;
	}

	@GetMapping("/kafka")
	public Map<String, Object> snapshot() {
		try {
			return readSnapshot();
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka request interrupted");
		} catch (ExecutionException | TimeoutException exception) {
			throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka metadata is temporarily unavailable");
		}
	}

	private Map<String, Object> readSnapshot() throws InterruptedException, ExecutionException, TimeoutException {
		var topic = kafka.describeTopics(List.of(TOPIC)).allTopicNames().get(5, TimeUnit.SECONDS).get(TOPIC);
		List<TopicPartition> topicPartitions = topic.partitions().stream()
				.map(partition -> new TopicPartition(TOPIC, partition.partition())).toList();
		Map<TopicPartition, Long> starts = offsets(topicPartitions, OffsetSpec.earliest());
		Map<TopicPartition, Long> ends = offsets(topicPartitions, OffsetSpec.latest());
		var partitions = topic.partitions().stream().map(partition -> {
			TopicPartition key = new TopicPartition(TOPIC, partition.partition());
			return Map.of("id", partition.partition(), "leader", partition.leader().id(),
					"replicas", partition.replicas().stream().map(node -> node.id()).toList(),
					"inSyncReplicas", partition.isr().stream().map(node -> node.id()).toList(),
					"startOffset", starts.get(key), "endOffset", ends.get(key));
		}).toList();

		List<Map<String, Object>> groups = new ArrayList<>();
		for (String service : List.of("inventory", "notification")) {
			groups.add(readGroup(service, topicPartitions, starts, ends));
		}
		var cluster = kafka.describeCluster();
		var brokers = cluster.nodes().get(5, TimeUnit.SECONDS).stream()
				.map(node -> Map.of("id", node.id(), "host", node.host(), "port", node.port())).toList();
		return Map.of("topic", TOPIC, "clusterId", cluster.clusterId().get(5, TimeUnit.SECONDS),
				"brokers", brokers, "partitions", partitions, "groups", groups,
				"sampledAt", Instant.now().toString());
	}

	private Map<TopicPartition, Long> offsets(List<TopicPartition> partitions, OffsetSpec specification)
			throws InterruptedException, ExecutionException, TimeoutException {
		Map<TopicPartition, OffsetSpec> request = partitions.stream()
				.collect(Collectors.toMap(partition -> partition, partition -> specification));
		return kafka.listOffsets(request).all().get(5, TimeUnit.SECONDS).entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
	}

	private Map<String, Object> readGroup(String service, List<TopicPartition> partitions,
			Map<TopicPartition, Long> starts, Map<TopicPartition, Long> ends)
			throws InterruptedException, ExecutionException, TimeoutException {
		String groupId = service + "-group";
		ConsumerGroupDescription description;
		try {
			description = kafka.describeConsumerGroups(List.of(groupId)).describedGroups()
					.get(groupId).get(5, TimeUnit.SECONDS);
		} catch (ExecutionException exception) {
			if (!(exception.getCause() instanceof GroupIdNotFoundException)) {
				throw exception;
			}
			return Map.of("groupId", groupId, "service", service, "state", "MISSING",
					"members", List.of(), "partitions", List.of());
		}
		var committed = kafka.listConsumerGroupOffsets(groupId)
				.partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
		List<Map<String, Object>> positions = new ArrayList<>();
		for (TopicPartition partition : partitions) {
			var offset = committed.get(partition);
			Map<String, Object> position = new LinkedHashMap<>();
			position.put("id", partition.partition());
			position.put("committedOffset", offset == null ? null : offset.offset());
			position.put("endOffset", ends.get(partition));
			position.put("lag", Math.max(0, ends.get(partition) - (offset == null ? starts.get(partition) : offset.offset())));
			positions.add(position);
		}
		var members = description.members().stream().map(member -> Map.of(
				"id", member.consumerId(), "clientId", member.clientId(), "host", member.host(),
				"partitions", member.assignment().topicPartitions().stream()
						.filter(partition -> TOPIC.equals(partition.topic()))
						.map(TopicPartition::partition).sorted().toList())).toList();
		return Map.of("groupId", groupId, "service", service, "state", description.groupState().toString(),
				"members", members, "partitions", positions);
	}
}