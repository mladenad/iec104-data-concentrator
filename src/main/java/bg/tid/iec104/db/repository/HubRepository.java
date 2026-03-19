package bg.tid.iec104.db.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import bg.tid.iec104.db.entity.Hub;

public interface HubRepository extends JpaRepository<Hub, Long> {

	Optional<Hub> findByUriAndEnabledTrue(String uri);

	List<Hub> findByEnabledTrue();

}
