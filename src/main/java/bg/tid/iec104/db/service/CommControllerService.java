package bg.tid.iec104.db.service;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;

import bg.tid.iec104.db.entity.CommController;
import bg.tid.iec104.db.entity.CommControllerArchive;
import bg.tid.iec104.db.repository.CommControllerArchiveRepository;
import bg.tid.iec104.db.repository.CommControllerRepository;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class CommControllerService {

	private final CommControllerRepository commControllerRepository;
	private final CommControllerArchiveRepository commControllerArchiveRepository;

	private ExecutorService executor = Executors.newCachedThreadPool();

	@PreDestroy
	public void cleanUp() {
		executor.shutdown(); // Stop accepting new tasks
		try {
			// Wait for 5 seconds to finish ongoing tasks
			if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
				log.warn("Executor did not terminate in the specified time. Forcing shutdown...");
				executor.shutdownNow(); // Cancel remaining tasks
			}
		} catch (InterruptedException e) {
			log.error("Shutdown interrupted. Forcing shutdown...");
			executor.shutdownNow(); // Force shutdown if interrupted
			Thread.currentThread().interrupt(); // Restore interrupted status
		}
	}
	
	public void upsertCommControllerByName(String name, Boolean online) {
		executor.execute(() -> {
			log.debug("upsertCommControllerByName");

			CommController newCommController = null;
			try {
				Optional<CommController> oldCommController = commControllerRepository.findByName(name);
				if (oldCommController.isPresent()) {
					oldCommController.get().setOnline(online);
					newCommController = commControllerRepository.save(oldCommController.get());
				} else {
					CommController commController = new CommController();
					commController.setName(name);
					commController.setOnline(true);
					newCommController = commControllerRepository.save(commController);

					// Needed to check last offline state in ArchiveHandler.sendArchiveTransferRequest
					CommControllerArchive commControllerArchive = new CommControllerArchive();
					commControllerArchive.setCommController(newCommController);
					commControllerArchive.setConnected(false);
					commControllerArchiveRepository.save(commControllerArchive);

				}
			} catch (Exception e) {
				log.error("UpsertCommControllerByName error: {}, {}", name, online);
			}

			try {
				if (newCommController != null) {
					CommControllerArchive commControllerArchive = new CommControllerArchive();
					commControllerArchive.setCommController(newCommController);
					commControllerArchive.setConnected(online);
					commControllerArchiveRepository.save(commControllerArchive);
				}
			} catch (Exception e) {
				log.error("InsertCommControllerArchive error: {}, {}", name, online);
			}
		});
	}

	public List<CommController> getAll() {
		log.debug("getAll");

		return commControllerRepository.findAll();
	}

	public CommController getCommControllerById(Long id) {
		log.debug("getCommControllerById");

		return commControllerRepository.findById(id).get();
	}

	public Optional<CommController> getCommControllerByName(String name) {
		log.debug("getCommControllerByName");

		return commControllerRepository.findByName(name);
	}

}
