package bg.tid.iec104.microservice;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.context.event.SpringApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class MicroserviceCoordinator implements ApplicationListener<SpringApplicationEvent> {
	
	private final ApplicationEventPublisher publisher;
	
	private volatile boolean executeLaunchSequenceStarted = false;
	private volatile CompletableFuture<Boolean> appReadyFuture = new CompletableFuture<>(); 
	
	public void executeLaunchSequence() {
		log.debug("executeLaunchSequence entered");
		
		if (executeLaunchSequenceStarted) {
			return;
		}
		executeLaunchSequenceStarted = true;

		log.info("executeLaunchSequence started...");

		try {
			appReadyFuture.get(30, TimeUnit.SECONDS);

			log.info("executeLaunchSequence ended.");
			publisher.publishEvent("appStartCompleted");

		} catch (Exception e) {
			log.error("executeLaunchSequence: Error:{}", e.getMessage());
			executeLaunchSequenceStarted = false;
		}

	}

	@Override
	public void onApplicationEvent(SpringApplicationEvent event) {
		if (event instanceof ApplicationReadyEvent) {
			log.info("App ready !");
			appReadyFuture.complete(true);
			executeLaunchSequence();
		} else if(event instanceof ApplicationStartedEvent) {
			log.info("App started !");
		}
	}

}
