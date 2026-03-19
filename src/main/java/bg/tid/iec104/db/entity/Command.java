package bg.tid.iec104.db.entity;

import jakarta.persistence.*;

import lombok.Data;

import java.sql.Timestamp;

@Entity
@Table(name="commands")
@Data
public class Command {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private Timestamp timestamp;

	@Column(columnDefinition = "text", nullable = true)
	private String gridOneUser;
	
	@Column(columnDefinition = "text", nullable = true)
	private String reason;

	@Column(columnDefinition = "text", nullable = false)
	private String topic;
	
	private Boolean confirmed;

}