package purple.order;

import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfiguration {

	@Bean
	public NewTopic orderEventsTopic() {
		return TopicBuilder.name("order-events").partitions(3).replicas(1).build();
	}

	@Bean(destroyMethod = "close")
	public Admin kafkaAdminClient(@Value("${spring.kafka.bootstrap-servers}") String brokers) {
		return Admin.create(Map.of(
				AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
				AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000,
				AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000));
	}
}