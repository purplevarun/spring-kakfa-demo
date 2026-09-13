package purple.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class NotificationApplication {

	public static void main(String[] args) {
		System.out.println("I am notification-service");
		SpringApplication.run(NotificationApplication.class, args);
	}

}
