package bg.tid.iec104.db.entity;

import jakarta.persistence.*;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Table(name="comm_controllers_archive",
indexes = { 
		@Index(name = "comm_controllers_archive_id_idx", columnList = "comm_controller_id"),
		@Index(name = "comm_controllers_archive_timestamp_idx", columnList = "createdAt") 
}
)
@Data
public class CommControllerArchive {
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	@Column(unique=true, nullable=false)
	private Long id;

	private Boolean connected;

	// bi-directional many-to-one association to CommController
	@ManyToOne
	@JoinColumn(name="comm_controller_id", nullable=false, foreignKey = @ForeignKey(name = "comm_controller_archive_comm_controllers_fk"))
	@JsonIgnore
	@OnDelete(action = OnDeleteAction.CASCADE)
	private CommController commController;
	
	@CreationTimestamp    
	@Column(updatable = false)
    private LocalDateTime createdAt;

}