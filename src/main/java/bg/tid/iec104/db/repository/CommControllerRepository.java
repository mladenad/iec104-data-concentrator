package bg.tid.iec104.db.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import bg.tid.iec104.db.entity.CommController;

public interface CommControllerRepository extends JpaRepository<CommController, Long> {

	Optional<CommController> findByName(String name);

}
