package bg.tid.iec104.db.entity;

import jakarta.persistence.*;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import lombok.Data;

import java.time.LocalDateTime;

//-- 1. Remove the old, problematic constraint
//-- Note: The name might be 'hub_uri_uq' or similar based on your previous code
//ALTER TABLE hub DROP CONSTRAINT IF EXISTS hub_uri_uq;
//
//-- 2. Create the 'Smart' Index
//CREATE UNIQUE INDEX idx_hub_uri_active_only 
//ON hub (uri) 
//WHERE (enabled IS TRUE);

@Entity
@Table(
		name="hub",
		indexes = @Index(name="hub_uri_idx", columnList = "uri")
)
@Data
public class Hub {
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	@Column(unique=true, nullable=false)
	private Long id;

	@Column(columnDefinition = "text", nullable=true)
	private String uri;

	@Column(columnDefinition = "text", nullable=true)
	private String descr;

	@Column(columnDefinition = "text", nullable=true)
	private String iol;
	
	@Column(nullable = true)
	private Boolean enabled;

	@Column(nullable = false)
	private Boolean online;

	@CreationTimestamp    
	@Column(updatable = false)
    private LocalDateTime createdAt;

	@UpdateTimestamp    
    @Column(nullable = true)
    private LocalDateTime updatedAt;
}
