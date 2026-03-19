package bg.tid.iec104.db.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import bg.tid.iec104.db.entity.CommControllerArchive;

public interface CommControllerArchiveRepository extends JpaRepository<CommControllerArchive, Long> {

	@Query(
		value = "select cca.* from comm_controllers_archive cca \n"
			+ "inner join comm_controllers cc on cc.id = cca.comm_controller_id \n"
			+ "where cc.name = :name and cca.connected = false \n"
			+ "order by cca.created_at desc \n"
			+ "limit 1",
		nativeQuery = true)
	Optional<CommControllerArchive> findByName(@Param("name") String name);
			
}
