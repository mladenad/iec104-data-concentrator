package bg.tid.iec104.db.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import bg.tid.iec104.db.entity.Command;

public interface CommandRepository extends JpaRepository<Command, Long> {

	Optional<Command> findByTopic(String topic);

}
