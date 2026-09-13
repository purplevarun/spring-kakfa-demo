package purple.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class InventoryApplication {

	public static void main(String[] args) {
		System.out.println("I am inventory-service");
		SpringApplication.run(InventoryApplication.class, args);
	}

}
