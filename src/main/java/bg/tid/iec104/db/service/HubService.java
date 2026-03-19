package bg.tid.iec104.db.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.modelmapper.ModelMapper;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import bg.tid.iec104.db.entity.Hub;
import bg.tid.iec104.db.repository.HubRepository;
import bg.tid.shared.iec104.IolData;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@AllArgsConstructor
public class HubService {

	private final HubRepository hubRepository;
	private final ModelMapper mm = new ModelMapper();
	private final ObjectMapper objectMapper;

	public Map<String, Boolean> getAllOnlineHubMap() {
		log.debug("getAllOnlineHubMap");

		List<Hub> hub = hubRepository.findByEnabledTrue();
		Map<String, Boolean> res = new HashMap<>();
		for (Hub row : hub) {
			res.put(row.getUri(), row.getOnline());
		}
		return res;
	}
	
	public List<Hub> getAllEnabledHubs() {
		log.debug("getAllEnabledHubs");

		return hubRepository.findByEnabledTrue();
	}

	@CacheEvict(value="getHubByUri", allEntries=true)
	public Hub upsertHubByHubUri(String hubIp, Integer hubPort, Boolean online) {
		log.debug("upsertHubByHubUri");
		
		String hubUri = hubIp + ":" + hubPort;

		try {
			Optional<Hub> oldHub = hubRepository.findByUriAndEnabledTrue(hubUri);
			if (oldHub.isPresent()) {
				oldHub.get().setOnline(online);
				Hub newHub = hubRepository.save(oldHub.get());
				return newHub;
			} else {
				Hub hub = new Hub();
				hub.setUri(hubUri);
				hub.setOnline(true);
				hub.setEnabled(true);
				Hub newHub = hubRepository.save(hub);
				return newHub;
			}
		} catch (Exception e) {
			log.error("upsertHubByHubUri Error: {}", e.getMessage());
			return null;
		}
		
	}

	@CacheEvict(value="getHubByUri", allEntries=true)
	public Hub updateHub(Hub newHub) {
		log.debug("updateHub");

		try {
			Hub oldHub = hubRepository.findById(newHub.getId()).get();
			if (oldHub != null) {
				mm.map(newHub, oldHub);
				Hub hub = hubRepository.save(oldHub);
				return hub;
			} else {
				return null;
			}

		} catch (Exception e) {
			return null;
		}
	}

	@Cacheable(value = "getHubByUri", key = "{#hubIp, #hubPort}")
	public Optional<Hub> getHubByUri(String hubIp, Integer hubPort) {
		String hubUri = hubIp + ":" + hubPort;

		return hubRepository.findByUriAndEnabledTrue(hubUri);

	}

	public Optional<Hub> getHubById(Long hubId) {
		log.debug("getHubById");

		return hubRepository.findById(hubId);
	}
	
	@CacheEvict(value="getHubByUri", allEntries=true)
	public Hub createHubByIol(IolData iol) {
		log.debug("createHubByIol: iol:{}", iol);
		
		try {
			Hub hub = new Hub();
			hub.setDescr(iol.getHubDescription());
			hub.setEnabled(true);
			hub.setIol(objectMapper.writeValueAsString(iol));
			hub.setOnline(false);
			String hubUri = iol.getHubIpAddress() + ":" + iol.getHubPort();
			hub.setUri(hubUri);
			
			return hubRepository.save(hub);
		} catch (Exception e) {
			log.error("", e.getMessage());
			return null;
		}
	}
	
}
