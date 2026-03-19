package bg.tid.iec104;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
@EnableCaching
@EnableAsync
@EnableScheduling
@ComponentScan(basePackages = {"bg.tid.shared.services", "bg.tid.iec104"})
public class Iec104DataConcentratorApplication {

	public static void main(String[] args) {
		SpringApplication.run(Iec104DataConcentratorApplication.class, args);
		log.info("GridOneIec104 start completed !");

	}

}
