package bg.tid.iec104.service;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.openmuc.j60870.ASdu;
import org.openmuc.j60870.ASduType;
import org.openmuc.j60870.CauseOfTransmission;
import org.openmuc.j60870.Connection;
import org.openmuc.j60870.ConnectionEventListener;
import org.openmuc.j60870.Server;
import org.openmuc.j60870.ServerEventListener;
import org.openmuc.j60870.Server.Builder;
import org.openmuc.j60870.ie.IeBinaryStateInformation;
import org.openmuc.j60870.ie.IeDoubleCommand;
import org.openmuc.j60870.ie.IeRegulatingStepCommand;
import org.openmuc.j60870.ie.IeScaledValue;
import org.openmuc.j60870.ie.IeShortFloat;
import org.openmuc.j60870.ie.IeSingleCommand;
import org.openmuc.j60870.ie.IeSinglePointWithQuality;
import org.openmuc.j60870.ie.IeTime56;
import org.openmuc.j60870.ie.InformationElement;
import org.openmuc.j60870.ie.InformationObject;
import org.openmuc.j60870.ie.IeNormalizedValue;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import bg.tid.iec104.db.entity.Hub;
import bg.tid.iec104.db.service.HubService;
import bg.tid.shared.iec104.InformationObjectData;
import bg.tid.shared.iec104.IolData;
import bg.tid.shared.mqtt.MqttRemoteRequestDto;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Service
@RequiredArgsConstructor
public class IecScada implements ServerEventListener {
	
	private final Environment env;
	private final TaskScheduler taskScheduler;
	private final HubService hubService;
	private final ObjectMapper objectMapper;
	private final ApplicationEventPublisher publisher;
	private final KeepalivedStatusService keepalivedStatus;

	private Server iecServer;
	private volatile Map<String, Connection> iecServerConnections = new ConcurrentHashMap<>();
	private Map<String, IecHub> hubs;
	private List<IolData> dataList;
	private int connectionIdCounter = 1;
	
	private final List<ScheduledFuture<?>> scheduledTasks = new CopyOnWriteArrayList<>();
	
	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	private ExecutorService executor = Executors.newCachedThreadPool();
	
	@PostConstruct
	private void initHubs() {
		log.debug("initHubs");
		
		boolean iecServerEnabled = env.getProperty("iec104.server.enabled", Boolean.class, false);
		if (iecServerEnabled) {
			String addr =  env.getProperty("iec104.server.address", String.class, "0.0.0.0");
			int port =  env.getProperty("iec104.server.port", Integer.class, 2404);
			int maxConnections =  env.getProperty("iec104.server.maxconnections", Integer.class, 10);
			String allowedClientsStr = env.getProperty("iec104.server.allowedclients", String.class, "");
			try {
				iecServer = startIecServer(addr, port, allowedClientsStr, maxConnections);
			} catch (UnknownHostException e) {
				log.error("Start iecServer: UnknownHostException:{}", e.getMessage());;
			} catch (Exception e) {
				log.error("Start iecServer: Error:{}", e.getMessage());;
			}
		}
		
		hubs = new HashMap<>();
		
        dataList = new ArrayList<>();
		List<Hub> hubList = hubService.getAllEnabledHubs();
		for (Hub hub : hubList) {
			try {
				if (hub.getIol() == null || hub.getIol().isBlank() || !hub.getEnabled()) {
					continue;
				}
				IolData iolData = objectMapper.readValue(hub.getIol(), IolData.class);
				
				iolData.setHubId(hub.getId());
				iolData.getInformationObjects().forEach(io -> {
					io.setHubId(hub.getId());
				});
				iolData.setModbusServerEnabled(false);
				
				dataList.add(iolData);
			} catch (JsonProcessingException e) {
				log.error("initHubs: Error:{}", e.getMessage());
			}
		}
		for (IolData iolData : dataList) {
			IecHub iecHub = new IecHub(this, iolData);
			hubs.put(iolData.getHubIpAddress() + ":" + iolData.getHubPort(), iecHub);
		}
		
		if (keepalivedStatus.isVIPEnabled() && !keepalivedStatus.isMaster()) {
			log.info("Currently in BACKUP mode - not opening Iec104 hub connections");
			return;
		}
		
		scheduleTasks(dataList.stream()
			    .flatMap(iolData -> iolData.getInformationObjects().stream())
//			    .filter(o -> o.getAverageable())
			    .collect(Collectors.toList()));
		openHubsConnections();
	}
	
	@PreDestroy
	private void closeHubsConnections() {
	    for (Map.Entry<String, IecHub> entry : hubs.entrySet()) {
	        String hubIpAddress = entry.getKey();
	        IecHub hub = entry.getValue();

	        try {
	            hub.getConnectionGlobal().stopDataTransfer();
	            log.info("Hub {} - stopDataTransfer executed successfully", hub.getIolData().getHubDescription());
	        } catch (IOException e) {
	            log.error("Hub {}:{} - stopDataTransfer error: {}", hubIpAddress, hub.getIolData().getHubDescription(), e.getMessage());
	        } finally {
	            hub.getConnectionGlobal().close();
	            hub.cleanUpExecutors(); // Ensure Hub-specific executors are cleaned up
	            cleanUpExecutors();     // Clean up general executors
	            log.info("Hub {} - connection closed", hub.getIolData().getHubDescription());
	        }
	    }
	}
	
	private void cleanUpExecutors() {
	    log.info("Shutting down ScheduledExecutorService...");
	    shutdownExecutor(scheduler);

	    log.info("Shutting down ExecutorService...");
	    shutdownExecutor(executor);

	    log.info("All executors shut down successfully.");
	}

	private void shutdownExecutor(ExecutorService executor) {
	    executor.shutdown(); // Stop accepting new tasks
	    try {
	        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) { // Wait for ongoing tasks to finish
	            log.warn("Executor did not terminate in time. Forcing shutdown...");
	            executor.shutdownNow(); // Cancel remaining tasks
	        }
	    } catch (InterruptedException e) {
	        log.error("Error during executor shutdown: {}", e.getMessage());
	        executor.shutdownNow(); // Force shutdown if interrupted
	        Thread.currentThread().interrupt(); // Restore interrupt status
	    }
	}
	
	private void disconnectHubsConnections() {
	    for (Map.Entry<String, IecHub> entry : hubs.entrySet()) {
	        String hubIpAddress = entry.getKey();
	        IecHub hub = entry.getValue();

	        try {
	        	hub.stopTimer(IecHub.RECONNECT_TIMER_NAME);
	            hub.getConnectionGlobal().stopDataTransfer();
	            log.info("Hub {} - stopDataTransfer executed successfully", hub.getIolData().getHubDescription());
	        } catch (IOException e) {
	            log.error("Hub {}:{} - stopDataTransfer error: {}", hubIpAddress, hub.getIolData().getHubDescription(), e.getMessage());
	        } finally {
	            hub.getConnectionGlobal().close();
	            log.info("Hub {} - connection closed", hub.getIolData().getHubDescription());
	        }
	    }
	}
	private void openHubsConnections() {
		for (String hubIpAddress : hubs.keySet()) {
			try {
				executor.execute(() -> {
					Boolean connected = hubs.get(hubIpAddress).connect();
					log.info("Hub {} - {}", hubs.get(hubIpAddress).getIolData().getHubDescription(), connected ? "is connected" : "is not connected");
				});
			} catch (Exception e) {
				log.error("Hub {}:{} - startDataTransfer error: {}", hubIpAddress, hubs.get(hubIpAddress).getIolData().getHubDescription(), e.getMessage());
			}
		}
	}
	
    /**
     * This method is called ONLY when state changes
     */
    @EventListener
    public void handleMasterStateChange(MasterStateEvent event) {
        if (event.isMaster()) {
            log.info("Became MASTER - opening IEC104 hub connections");
    		scheduleTasks(dataList.stream()
    			    .flatMap(iolData -> iolData.getInformationObjects().stream())
    			    .collect(Collectors.toList()));
            openHubsConnections();
        } else {
            log.info("Became BACKUP - closing IEC104 hub connections");
    		scheduledTasks.forEach(scheduledTask -> scheduledTask.cancel(true));
    		disconnectHubsConnections();
        }
    }
    
	@Scheduled(cron = "*/5 * * * * *")
	private void sendAllAverageableTags() {
		log.trace("sendAllTags");
		
		if (keepalivedStatus.isVIPEnabled() && !keepalivedStatus.isMaster()) {
			log.debug("Currently in BACKUP mode - skip sendAllAverageableTags");
			return;
		}
		dataList.stream().flatMap(iol -> iol.getInformationObjects().stream()).filter(io -> io.getAverageable()).forEach(io -> {
			try {
				log.trace("sendAllTags: hubName:{}, topic:{}, value:{}", io.getHubName(), io.getTopic(), io.getTag().getValue());
				if (io.getTopic() == null || io.getTopic().isBlank()) {
					log.trace("sendAllTags: Error: topic is null or empty for hub: {}", io.getHubName());
					return;
				}
				// websocket send removed
			} catch (Exception e) {
				log.error("sendAllTags:{}", e.getMessage());
			}
		});
	}

	// BmsController and PMS tracking methods were removed

	@Override
	public ConnectionEventListener connectionIndication(Connection connection) {
		int myConnectionId = connectionIdCounter++;
		log.info("A client (Originator Address {}) has connected using TCP/IP. Will listen for a StartDT request. Connection ID: ", connection.getOriginatorAddress());
		log.info("Started data transfer on connection ({}) Will listen for incoming commands.", myConnectionId);

		iecServerConnections.put(connection.getRemoteInetAddress().getHostAddress(), connection);
		return new ConnectionListener(connection, myConnectionId);
	}

	@Override
	public void serverStoppedListeningIndication(IOException e) {
		log.warn("Server has stopped listening for new connections : {}. Will quit.", e.getMessage());
	}

	@Override
	public void connectionAttemptFailed(IOException e) {
		log.warn("Connection attempt failed: ", e.getMessage());
	}
	
	public class ConnectionListener implements ConnectionEventListener {

		@SuppressWarnings("unused")
		private final Connection connection;
		private final int connectionId;

		public ConnectionListener(Connection connection, int connectionId) {
			this.connection = connection;
			this.connectionId = connectionId;
		}

		@Override
		public void newASdu(Connection connection, ASdu aSdu) {
			log.info("Got new ASdu: {}, connection: {}", aSdu, connection.getRemoteInetAddress().getHostName());
			try {
				InformationObject informationObject = aSdu.getInformationObjects()[0];
				String ip = connection.getRemoteInetAddress().getHostAddress();
				int addr = informationObject.getInformationObjectAddress();
				switch (aSdu.getTypeIdentification()) {
				case C_CS_NA_1:
					IeTime56 ieTime56 = new IeTime56(System.currentTimeMillis());
					log.info("Got Clock synchronization command (103). Send current time: {}", ieTime56.toString());
					connection.synchronizeClocks(aSdu.getCommonAddress(), ieTime56);
					connection.sendActivationTermination(aSdu);
					return;
				// interrogation command
				case C_IC_NA_1: {
					log.info("Got interrogation command (100). Will send scaled measured values.");
					hubs.values().stream()
					    .filter(iecHub -> iecHub.getIolData().getInformationObjects().stream()  // Filter IecHub by checking the IolData's InformationObjectData
				            .flatMap(ioo -> ioo.getRetranslationSettings().stream())  // Flatten RetranslationSettings
				            .anyMatch(r -> ip.equals(r.getIp())))  // Check if the IP matches
					    .forEach(iecHub -> {
					    	iecHub.sendGI();
					    });
					connection.sendActivationTermination(aSdu);
					return;
				}
					
				case C_SC_NA_1, C_DC_NA_1, C_RC_NA_1, C_SE_NA_1, C_SE_NB_1, C_SE_NC_1, C_BO_NA_1: {
					Optional<InformationObjectData> ioData = dataList.stream()
						.flatMap(dl -> dl.getInformationObjects().stream()) // Flatten the list of InformationObjectData
						.filter(ioo -> ioo.getRetranslationSettings().stream().anyMatch(r -> ip.equals(r.getIp()) && addr == r.getAddr()))
						.findFirst();
					if (ioData.isPresent() && ioData.get().getTypeId().equals(aSdu.getTypeIdentification().getId())) {
						String mqttTopic = ioData.get().getTopic();
						Optional<IecHub> iecHub = hubs.values().stream()
						    .filter(h -> h.getIolData().getInformationObjects().stream()  // Filter IecHub by checking the IolData's InformationObjectData
					            .flatMap(ioo -> ioo.getRetranslationSettings().stream())  // Flatten RetranslationSettings
					            .anyMatch(r -> ip.equals(r.getIp()) && addr == r.getAddr()))  // Check if the IP matches
						    .findFirst();
						boolean select = false;
						if (iecHub.isPresent()) {
							String valueStr = switch (aSdu.getTypeIdentification()) {
							case C_SC_NA_1 -> {
								IeSingleCommand singleCommand = (IeSingleCommand) informationObject.getInformationElements()[0][0];
								select = singleCommand.isSelect();
								int value = singleCommand.isCommandStateOn() ? 1 : 0;
								yield String.valueOf(value);
							}
							
							case C_DC_NA_1 -> {
								IeDoubleCommand doubleCommand = (IeDoubleCommand) informationObject.getInformationElements()[0][0];
								select = doubleCommand.isSelect();
								int value = switch (doubleCommand.getCommandState()) {
									case NOT_PERMITTED_A -> 0;
									case OFF -> 1;
									case ON -> 2;
									case NOT_PERMITTED_B -> 3;
									default -> throw new IllegalArgumentException("Unexpected value: " + doubleCommand.getCommandState());
								};
								yield String.valueOf(value);
							}
							
							case C_RC_NA_1 -> {
								IeRegulatingStepCommand regulatingStepCommand = (IeRegulatingStepCommand) informationObject.getInformationElements()[0][0];
								select = regulatingStepCommand.isSelect();
								int value = switch (regulatingStepCommand.getCommandState()) {
									case NOT_PERMITTED_A -> 0;
									case NEXT_STEP_LOWER -> 1;
									case NEXT_STEP_HIGHER -> 2;
									case NOT_PERMITTED_B -> 3;
									default -> throw new IllegalArgumentException("Unexpected value: " + regulatingStepCommand.getCommandState());
								};
								yield String.valueOf(value);
							}
							
							case C_SE_NA_1 -> {
								IeNormalizedValue normalizedValueCommand = (IeNormalizedValue) informationObject.getInformationElements()[0][0];
								int value = normalizedValueCommand.getUnnormalizedValue();
								yield String.valueOf(value);
							}
							
							case C_SE_NB_1 -> {
								IeScaledValue scaledValueCommand = (IeScaledValue) informationObject.getInformationElements()[0][0];
								int value = scaledValueCommand.getUnnormalizedValue();
								yield String.valueOf(value);
							}
								
							case C_SE_NC_1 -> {
								IeShortFloat shortFloatCommand = (IeShortFloat) informationObject.getInformationElements()[0][0];
								float value = shortFloatCommand.getValue();
								yield String.valueOf(value);
							}
							
							case C_BO_NA_1 -> {
								IeBinaryStateInformation binaryStateInformationCommand = (IeBinaryStateInformation) informationObject.getInformationElements()[0][0];
								int value = binaryStateInformationCommand.getValue();
								yield String.valueOf(value);
							}
							
							default -> {
								connection.sendConfirmation(aSdu, aSdu.getCommonAddress(), true, CauseOfTransmission.UNKNOWN_TYPE_ID);
								log.warn("newASdu: Warning: unknown type:{} for: {}", aSdu.getTypeIdentification(), aSdu.toString());
								throw new IllegalArgumentException("Unexpected value: " + aSdu.getTypeIdentification());
							}
							};
							
							if (!ioData.get().getIncomingTopic().isBlank()) {
								// Command from external SCADA system
								log.info("newASdu: Command from external SCADA system received: Retranslation settings:{}, topic:{}:{}", 
									ioData.get().getRetranslationSettings(), ioData.get().getIncomingTopic(), valueStr);
								
								MqttRemoteRequestDto remoteTransferDto = MqttRemoteRequestDto.builder()
										.topic(ioData.get().getIncomingTopic())
										.command("MqttManualCommand")
										.data(valueStr)
										.retained(false)
										.user("External SCADA system")
										.updateTime(LocalDateTime.now())
										.build();
								publisher.publishEvent(remoteTransferDto);
							} else if (select) {
								log.warn("newASdu: Warning: select=true for: {}", aSdu.toString());
								connection.sendConfirmation(aSdu, aSdu.getCommonAddress(), true, CauseOfTransmission.UNKNOWN_CAUSE_OF_TRANSMISSION);
							} else if (iecHub.get().sendCmd("Retranslation from " + connection.getRemoteInetAddress().getHostName(), mqttTopic, valueStr, "manual")) {
								connection.sendActivationTermination(aSdu);
							} else {
								log.warn("newASdu: Warning: command not sent for: {}", aSdu.toString());
								connection.sendConfirmation(aSdu, aSdu.getCommonAddress(), true, CauseOfTransmission.UNKNOWN_INFORMATION_OBJECT_ADDRESS);
							}
						} else {
							log.warn("newASdu: Warning: iecHub not present for: {}", aSdu.toString());
							connection.sendConfirmation(aSdu, aSdu.getCommonAddress(), true, CauseOfTransmission.UNKNOWN_INFORMATION_OBJECT_ADDRESS);
						}
					} else {
						log.warn("newASdu: Warning: ioData not present for: {}", aSdu.toString());
						connection.sendConfirmation(aSdu, aSdu.getCommonAddress(), true, CauseOfTransmission.UNKNOWN_INFORMATION_OBJECT_ADDRESS);
					}
					connection.sendConfirmation(aSdu);
					return;
				}
					
//				case C_SC_NA_1:
//					informationObject = aSdu.getInformationObjects()[0];
//					IeSingleCommand singleCommand = (IeSingleCommand) informationObject.getInformationElements()[0][0];
//
//					if (informationObject.getInformationObjectAddress() != 5000) {
//						break;
//					}
//					if (singleCommand.isSelect()) {
//						log.info("Got single command (45) with select true. Select command.");
//						selected = true;
//						connection.sendConfirmation(aSdu);
//					} else if (!singleCommand.isSelect() && selected) {
//						log.info("Got single command (45) with select false. Execute selected command.");
//						selected = false;
//						connection.sendConfirmation(aSdu);
//					} else {
//						log.info("Got single command (45) with select false. But no command is selected, no execution.");
//					}
//					break;
				default:
					log.warn("newASdu: Warning2: unknown type:{} for: {}", aSdu.getTypeIdentification(), aSdu.toString());
					connection.sendConfirmation(aSdu, aSdu.getCommonAddress(), true, CauseOfTransmission.UNKNOWN_TYPE_ID);
					return;
				}

			} catch (EOFException e) {
				log.info("Will quit listening for commands on connection ({}) because socket was closed. Error:{}", connectionId, e.getMessage());
			} catch (IOException e) {
				log.info("Will quit listening for commands on connection ({}) because of error: {}.", connectionId, e.getMessage());
			}

		}

		@Override
		public void connectionClosed(Connection connection, IOException e) {
			log.info("Connection ({}) was closed. ", connectionId, e.getMessage());
		}

		@Override
		public void dataTransferStateChanged(Connection connection, boolean stopped) {
			String dtState = "started";
			if (stopped) {
				dtState = "stopped";
			}
			log.info("Data transfer of connection ({}) was {}.", connectionId, dtState);
		}

	}
	
	private Server startIecServer(String addr, int port, String allowedClientsStr, int maxConnections) throws Exception {
		log.debug("startIecServer:{}:{}", addr, port);
		
		List<String> allowedClientsList = allowedClientsStr.isBlank() ? null : List.of(allowedClientsStr.split(","));
		Builder builder = Server.builder();
		InetAddress bindAddress = InetAddress.getByName(addr);
		Server server = builder
			.setBindAddr(bindAddress)
			.setPort(port)
			.setMaxConnections(maxConnections)
			.setAllowedClients(allowedClientsList)
			.build();
		try {
			server.start(this);
			log.info("Iec104Server started on: {}:{}", addr, port);
			return server;
		} catch (IOException e) {
			log.error("Unable to start listening: Error:{}. Will quit.", e.getMessage());
			throw new IOException(e.getMessage());
		}
	}

	public void scheduleTasks(List<InformationObjectData> ioDataList) {
	    // Cancel and clear tasks safely
	    scheduledTasks.forEach(task -> task.cancel(true));
	    scheduledTasks.clear();

	    // Prepare grouped maps
	    Map<String, List<InformationObjectData>> averageableMap =
	            groupByCron(ioDataList, io -> io.getArchive() && io.getAverageable());

	    Map<String, List<InformationObjectData>> triggerTagMap =
	            groupByCron(ioDataList, io -> "TriggerTag".equals(io.getFormula()));

	    // Schedule tasks
	    scheduleGroupedTasks(averageableMap, this::executeAverageableCron);
	    scheduleGroupedTasks(triggerTagMap, this::executeTriggerTagCron);
	}

	private Map<String, List<InformationObjectData>> groupByCron(List<InformationObjectData> ioDataList, Predicate<InformationObjectData> condition) {

	    Map<String, List<InformationObjectData>> result = new HashMap<>();

	    for (InformationObjectData ioData : ioDataList) {
	        if (condition.test(ioData)) {
	            String cronExpression = ioData.getArchive_period();
	            if (cronExpression != null && CronExpression.isValidExpression(cronExpression)) {
	                result.computeIfAbsent(cronExpression, k -> new ArrayList<>()).add(ioData);
	            }
	        }
	    }
	    return result;
	}

	private void scheduleGroupedTasks(Map<String, List<InformationObjectData>> groupedMap, Consumer<List<InformationObjectData>> taskExecutor) {
	    groupedMap.forEach((cronExpression, dataList) ->
            scheduledTasks.add(taskScheduler.schedule(() -> taskExecutor.accept(dataList), new CronTrigger(cronExpression))));
	}

	private void executeTriggerTagCron(List<InformationObjectData> ioDataList) {
		try {
			LocalDateTime scheduleTime = LocalDateTime.now().withNano(0);
			log.debug("scheduleTasks: run at:{}", scheduleTime);
			for (InformationObjectData iod : ioDataList) {
				try {
					log.debug("Executing scheduled task with archive period:{} - {}", iod.getArchive_period(), iod.getDescription());
					Optional<IecHub> hub = hubs.values().stream()
						.filter(iecHub -> iecHub.getIolData().getHubId().equals(iod.getHubId()))
						.findFirst();
					
					if (hub.isEmpty()) {
						log.warn("run: Hub not found for ioData: {}", iod.getDescription());
						continue;
					}
					
					ASdu asdu =  new ASdu(ASduType.M_SP_NA_1, false, CauseOfTransmission.PERIODIC, false, false, 0,	1, 
							new InformationObject(iod.getAddress(),	new InformationElement[] { new IeSinglePointWithQuality(true, false, false, false, false) }));
					
					hub.get().processAsdu(asdu);
				} catch (Exception e) {
					log.error("executeTriggerTagCron: ioData: {}, Error: {}", iod.getDescription(), e.getMessage());
				}
			}
		} catch (Exception e) {
			log.error("run: Error: {}", e.getMessage());
		}
	}
	
	private void executeAverageableCron(List<InformationObjectData> ioDataList) {
		try {
			LocalDateTime scheduleTime = LocalDateTime.now().withSecond(0).withNano(0);
			log.debug("scheduleTasks: run at:{}", scheduleTime);
			for (InformationObjectData ioData : ioDataList) {
				log.debug("Executing scheduled task with archive period:{} - {}: value:{}", ioData.getArchive_period(), ioData.getDescription(), ioData.getValue(false));
			}
		} catch (Exception e) {
			log.error("run: Error: {}", e.getMessage());
		}
	}

	public List<Integer> convertToList(String commaSeparatedString) {
		return Arrays.stream(commaSeparatedString.split(",")).map(String::trim).map(Integer::parseInt).collect(Collectors.toList());
	}

}
