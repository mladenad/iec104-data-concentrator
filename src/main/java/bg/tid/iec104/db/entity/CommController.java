package bg.tid.iec104.db.entity;

import jakarta.persistence.*;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(
		name="comm_controllers",
		uniqueConstraints = { @UniqueConstraint(name="comm_controllers_name_uq", columnNames = { "name" }) },
		indexes = @Index(name="comm_controllers_name_idx", columnList = "name")
)
@Data
public class CommController {
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	@Column(unique=true, nullable=false)
	private Long id;

	@Column(columnDefinition = "text", nullable=false)
	private String name;

	@Column(nullable=false)
	private Boolean online;

	// bi-directional one-to-many association to CommControllerArchive
	@OneToMany(mappedBy = "commController")
	@JsonIgnore
    private List<CommControllerArchive> commControllerArchive;
	
	@CreationTimestamp    
	@Column(updatable = false)
    private LocalDateTime createdAt;

	@UpdateTimestamp    
    @Column(nullable = true)
    private LocalDateTime updatedAt;

}