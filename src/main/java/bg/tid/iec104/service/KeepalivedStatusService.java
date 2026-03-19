package bg.tid.iec104.service;

import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import java.io.BufferedReader;
import java.io.InputStreamReader;

@Slf4j
@Service
@RequiredArgsConstructor
public class KeepalivedStatusService {

	private final Environment env;
	private final ApplicationEventPublisher eventPublisher;

	private volatile boolean isMaster = false;
	private volatile boolean isVIPEnabled = false;
	private volatile String vipAddress;
	private volatile String networkInterface;

	@PostConstruct
	private void init() {
		isVIPEnabled = env.getProperty("iec104.vip.enabled", Boolean.class, false);
		vipAddress = env.getProperty("iec104.vip.address", "");
		networkInterface = env.getProperty("iec104.vip.interface", "ens37");

		// Validate configuration
		if (isVIPEnabled && (vipAddress == null || vipAddress.isEmpty())) {
			log.warn("VIP is enabled but no VIP address configured. Disabling VIP functionality.");
			isVIPEnabled = false;
			return;
		}

		if (isVIPEnabled) {
			log.info("VIP monitoring enabled - Address: {}, Interface: {}", vipAddress, networkInterface);

			// Initial check
			isMaster = hasVirtualIp();
			log.info("Initial VIP status: {}", isMaster ? "MASTER 🟢" : "BACKUP 🔴");

			// Publish initial state event so services can initialize properly
			eventPublisher.publishEvent(new MasterStateEvent(this, isMaster));
		} else {
			log.info("VIP monitoring disabled");
		}
	}

	public boolean isVIPEnabled() {
		return isVIPEnabled;
	}

	/**
	 * Check if this server currently owns the VIP (is MASTER)
	 */
	public boolean isMaster() {
		return isMaster;
	}

	/**
	 * Check VIP status every 2 seconds
	 */
	@Scheduled(fixedRate = 2000)
	public void checkVipStatus() {
		if (!isVIPEnabled) {
			return;
		}

		boolean previousState = isMaster;
		isMaster = hasVirtualIp();

		if (previousState != isMaster) {
			if (isMaster) {
				log.info("🟢 TRANSITIONED TO MASTER - VIP {} is now active on {}", vipAddress, networkInterface);
				eventPublisher.publishEvent(new MasterStateEvent(this, true));
			} else {
				log.info("🔴 TRANSITIONED TO BACKUP - VIP {} is no longer active", vipAddress);
				eventPublisher.publishEvent(new MasterStateEvent(this, false));
			}
		}
	}

	/**
	 * Check if the VIP exists on the specified network interface
	 */
	private boolean hasVirtualIp() {
		try {
			ProcessBuilder pb = new ProcessBuilder("ip", "addr", "show", networkInterface);
			Process process = pb.start();

			try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {

				String line;
				while ((line = reader.readLine()) != null) {
					if (line.contains("inet " + vipAddress)) {
						return true;
					}
				}
			}

			int exitCode = process.waitFor();
			if (exitCode != 0) {
				log.warn("Command 'ip addr show {}' exited with code {}", networkInterface, exitCode);
			}
			return false;

		} catch (Exception e) {
			log.error("Error checking VIP status for {} on {}", vipAddress, networkInterface, e);
			return false;
		}
	}

}
