package purple.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderApplication {

	public static void main(String[] args) {
		System.out.println("I am order-service");
		SpringApplication.run(OrderApplication.class, args);
	}

}
