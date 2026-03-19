package bg.tid.iec104.service;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.swing.Timer;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.openmuc.j60870.ASdu;
import org.openmuc.j60870.ASduType;
import org.openmuc.j60870.CauseOfTransmission;
import org.openmuc.j60870.ClientConnectionBuilder;
import org.openmuc.j60870.Connection;
import org.openmuc.j60870.ConnectionEventListener;
import org.openmuc.j60870.ie.IeBinaryCounterReading;
import org.openmuc.j60870.ie.IeBinaryStateInformation;
import org.openmuc.j60870.ie.IeBinaryCounterReading.Flag;
import org.openmuc.j60870.ie.IeDoubleCommand;
import org.openmuc.j60870.ie.IeDoubleCommand.DoubleCommandState;
import org.openmuc.j60870.ie.IeDoublePointWithQuality;
import org.openmuc.j60870.ie.IeQualifierOfInterrogation;
import org.openmuc.j60870.ie.IeDoublePointWithQuality.DoublePointInformation;
import org.openmuc.j60870.ie.IeNormalizedValue;
import org.openmuc.j60870.ie.IeQualifierOfSetPointCommand;
import org.openmuc.j60870.ie.IeQuality;
import org.openmuc.j60870.ie.IeRegulatingStepCommand;
import org.openmuc.j60870.ie.IeRegulatingStepCommand.StepCommandState;
import org.springframework.core.env.Environment;
import org.openmuc.j60870.ie.IeScaledValue;
import org.openmuc.j60870.ie.IeShortFloat;
import org.openmuc.j60870.ie.IeSingleCommand;
import org.openmuc.j60870.ie.IeSinglePointWithQuality;
import org.openmuc.j60870.ie.IeTime56;
import org.openmuc.j60870.ie.InformationElement;
import org.openmuc.j60870.ie.InformationObject;

import bg.tid.iec104.db.service.HubService;
import bg.tid.shared.iec104.Iec104RetranslationSettings;
import bg.tid.shared.iec104.InformationObjectData;
import bg.tid.shared.iec104.IolData;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
public class IecHub implements ConnectionEventListener {

	private final IecScada iecScada;
	private final Environment env;
	private final HubService hubService;
	private final Map<String, Connection> iecServerConnections;
	private final KeepalivedStatusService keepalivedStatus;
	
	private IolData iolData; // it is not final in order to have a setter, be able to be set in "uploadIol"
	
	private Connection connectionGlobal;
	
	private JexlEngine jexl = new JexlBuilder().create();
	private HashMap<String, Timer> timers = new HashMap<String, Timer>();
	
	public static final String RECONNECT_TIMER_NAME = "ReconnectIecHub";
	private static final int RECONNECT_TIMER_TIMEOUT = 120_000; // 2 min
	
	private volatile Map<Integer, Long> notConfirmedCommands = new ConcurrentHashMap<>();
	private volatile Map<Integer, Runnable> commandQueue = new ConcurrentHashMap<>();
	
	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
	private ExecutorService executor = Executors.newCachedThreadPool();
	
	public IecHub(IecScada iecScada, IolData iolData) {
		this.iecScada = iecScada;
		this.env = iecScada.getEnv();
		this.keepalivedStatus = iecScada.getKeepalivedStatus();
		this.hubService = iecScada.getHubService();
		this.iecServerConnections = iecScada.getIecServerConnections();
		this.iolData = iolData;
	}

	public void cleanUpExecutors() {
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
	
	@Override
	public void newASdu(Connection connection, ASdu aSdu) {

//		try {
//			String json = objectMapper.writeValueAsString(aSdu);
//			int a = 1;
//		} catch (JsonProcessingException e) {
//			e.printStackTrace();
//		}

		processAsdu(aSdu);

		log.trace(String.join("\nReceived ASDU:\n", aSdu.toString()));
		log.trace("ASDU isSequenceOfElements: {}", aSdu.isSequenceOfElements());
		log.trace("ASDU getSequenceLength: {}", aSdu.getSequenceLength());
	}

	@Override
	public void connectionClosed(Connection connection, IOException cause) {
		executor.execute(() -> {
			log.info("Received connection closed signal. Reason: ");
			if (!cause.getMessage().isEmpty()) {
				log.info(cause.getMessage());
			} else {
				log.info("unknown");
			}
			updateHubStatus(false, LocalDateTime.now());
			startReconnectIecHubTimer(RECONNECT_TIMER_TIMEOUT, RECONNECT_TIMER_NAME);
//			connect();
		});
	}

	@Override
	public void dataTransferStateChanged(Connection connection, boolean stopped) {
		log.info("dataTransferStateChanged: {}", stopped ? "stopped" : "started");
	}

	private void createReconnectIecHubTimer(int interval, String sessionId) {
		Timer t = new Timer(interval, new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent arg0) {
				executor.execute(() -> {
					connect();
				});
			}
		});
		timers.put(sessionId, t);
	}

	private void startReconnectIecHubTimer(int interval, String sessionId) {
		if (!timers.containsKey(sessionId))
			createReconnectIecHubTimer(interval, sessionId);
		if (!timers.get(sessionId).isRunning())
			timers.get(sessionId).start();
	}

	public void stopTimer(String sessionId) {
		if (timers.containsKey(sessionId))
			if (timers.get(sessionId).isRunning())
				timers.get(sessionId).stop();
			else
				log.error("Timer with this session number is already stopped !!!");
		else
			log.error("Timer with this session number does't exist !!!");
	}

	private Integer qualityToBitMask(IeQuality quality) {
		int bitMask = 0;
		if (quality.isBlocked()) {
			bitMask += 1;
		}
		if (quality.isInvalid()) {
			bitMask += 2;
		}
		if (quality.isNotTopical()) {
			bitMask += 4;
		}
		if (quality.isOverflow()) {
			bitMask += 8;
		}
		if (quality.isSubstituted()) {
			bitMask += 16;
		}
		return bitMask;
	}

	public void processAsdu(ASdu aSdu) {
		executor.execute(() -> {
			LocalDateTime receivedTime = LocalDateTime.now();
			log.trace("processAsdu: aSdu: {}", aSdu);

			try {
				ASduType asduType = aSdu.getTypeIdentification();

				// Remove !!!
//				if (asduType.getId() == 13) {
//					return;
//				}

				// Check if ASdu is a command confirmation
				switch (aSdu.getCauseOfTransmission()) {
				case ACTIVATION_CON, ACTIVATION_TERMINATION: {
					int ioa = aSdu.getInformationObjects()[0].getInformationObjectAddress();
					if (ioa == 0) {
						// GI command
						log.info("processAsdu: ioa = 0 for asdu:{}", aSdu);
						return;
					}
					log.info("processAsdu: COT:{} for ioa:{}. notConfirmedCommands size:{}", aSdu.getCauseOfTransmission(), ioa, notConfirmedCommands.size());

					Runnable command = commandQueue.remove(ioa); // Remove and retrieve the command
					if (command != null) {
						log.info("processAsdu: Execute the command with select = false for: {}", ioa);
						command.run(); // Execute the command with select = false
					}

					return;
				}

				case UNKNOWN_TYPE_ID, UNKNOWN_CAUSE_OF_TRANSMISSION, UNKNOWN_COMMON_ADDRESS_OF_ASDU, UNKNOWN_INFORMATION_OBJECT_ADDRESS: {
					log.warn("processAsdu: Warning: UNKNOWN for {}", aSdu);
					return;
				}

				default:
					break;
				}

				for (InformationObject aSduIo : aSdu.getInformationObjects()) {
					AtomicInteger counter = new AtomicInteger(0);
					for (InformationElement[] aSduIeList : aSduIo.getInformationElements()) {
						for (InformationElement aSduIe : aSduIeList) {
							iolData.getInformationObjects().stream()
								.filter(io -> io.getAddress() != null && io.getAddress().equals(aSduIo.getInformationObjectAddress() + counter.get())).findFirst()
								.ifPresent(iolIo -> {
									try {
										iolIo.setCauseOfTransmission(aSdu.getCauseOfTransmission().name());
										iolIo.setOriginatorAddress(aSdu.getOriginatorAddress());
										iolIo.setCommonAddress(aSdu.getCommonAddress());

										Float value = null;
										if (aSduIe instanceof IeBinaryCounterReading) {
											value = binaryCounterReadingHandler(aSduIe);
										} else if (aSduIe instanceof IeDoublePointWithQuality) {
											value = doublePointWithQualityHandler(aSduIe, iolIo);
										} else if (aSduIe instanceof IeSinglePointWithQuality) {
											value = singlePointWithQualityHandler(aSduIe, iolIo);
										} else if (aSduIe instanceof IeScaledValue) {
											value = scaledValueHandler(aSduIe, iolIo);
//											log.debug("Scaled value: {}: {}", iolIo.getTopic(), value);
										} else if (aSduIe instanceof IeShortFloat) {
											value = shortFloatHandler(aSduIe, iolIo);
//											log.debug("Short float value: {}: {}", iolIo.getTopic(), value);
										} else if (aSduIe instanceof IeQuality quality) {
											int q = qualityToBitMask(quality);
											iolIo.getTag().setQuality(q);
											if (q != 0) {
												log.debug("Quality: {}: {}", iolIo.getTopic(), q);
											}
											return;
										} else if (aSduIe instanceof IeTime56 time56) {
											log.debug("Timestamp: {}: {}", iolIo.getTopic(), time56.getTimestamp());
											return;
										} else {
											log.error("processAsdu: Error: Missing ASDU type: {}", aSduIe.getClass().getName());
										}

										value = iolIo.getScaledValue(value);
										log.debug("Input value: {}: {}", iolIo.getDescription(), value);
										if (iolIo.getArchive()) {
											if (iolIo.getAverageable()) {
												iolIo.setValue(value);
											} else if (!value.equals(iolIo.getValue(false)) && iolIo.getTopic() != null && !iolIo.getTopic().isBlank()) {
												iolIo.setValue(value);
											} else {
												log.debug("processAsdu: value not saved: descr:{}, value:{}, topic:{}", iolIo.getDescription(), value, iolIo.getTopic());
											}
										}

										if (!iolIo.getTargetAddress().isEmpty()) {
											processFormula(iolIo, receivedTime);
										}

										if (iolIo.getRetranslationSettings().size() > 0) {
										    for (Iec104RetranslationSettings settings : iolIo.getRetranslationSettings()) {
										        log.debug("{}", iecServerConnections);
										        Connection conn = iecServerConnections.get(settings.getIp());
										        if (conn != null && !conn.isClosed()) {
										            InformationElement[] scaledIeList = buildScaledIeList(aSduIeList, value);
										            ASdu asdu = new ASdu(asduType, true, aSdu.getCauseOfTransmission(), false, false, 0, iolIo.getCommonAddress(),
										                    new InformationObject(settings.getAddr(), scaledIeList));
										            conn.send(asdu);
										        }
										    }
										}										
									} catch (Exception e) {
										log.error(".ifPresent(iolIo): Error: {}, iol: {}", e.getMessage(), iolIo.getDescription());
									}
								});
						}
						counter.incrementAndGet();
					}
				}
			} catch (Exception e) {
				log.error("processAsdu: Error: {}", e.getMessage());
			}
		});
	}

	private InformationElement[] buildScaledIeList(InformationElement[] original, Float scaledValue) {
	    InformationElement[] result = new InformationElement[original.length];
	    for (int i = 0; i < original.length; i++) {
	        InformationElement ie = original[i];
	        if (ie instanceof IeShortFloat) {
	            result[i] = new IeShortFloat(scaledValue);
	        } else if (ie instanceof IeScaledValue) {
	            result[i] = new IeScaledValue(scaledValue.intValue());
	        } else {
	            // IeQuality, IeTime56, etc. — pass through untouched
	            result[i] = ie;
	        }
	    }
	    return result;
	}
	
	private Float binaryCounterReadingHandler(InformationElement aSduIe) {
		int binaryCounter = ((IeBinaryCounterReading) aSduIe).getCounterReading();
		int sequenceNumber = ((IeBinaryCounterReading) aSduIe).getSequenceNumber();
		Set<Flag> flags = ((IeBinaryCounterReading) aSduIe).getFlags();
		if (flags.size() > 0 || sequenceNumber > 0) {
			log.error("binaryCounterReadingHandler: unprocessed props - sequenceNumber:{}, flags:{}, value:{}", sequenceNumber, flags.toString(), binaryCounter);
		}
		return (float) binaryCounter;
	}

	private Float doublePointWithQualityHandler(InformationElement aSduIe, InformationObjectData iolIo) {
		DoublePointInformation status = ((IeDoublePointWithQuality) aSduIe).getDoublePointInformation();
		Float value = 0f;
		switch (status) {
		case INDETERMINATE_OR_INTERMEDIATE: {
			value = 0f;
			break;
		}
		case OFF: {
			value = 1f;
			break;
		}
		case ON: {
			value = 2f;
			break;
		}
		case INDETERMINATE: {
			value = 3f;
			break;
		}
		default:
			break;
		}
		return value;

	}

	private Float singlePointWithQualityHandler(InformationElement aSduIe, InformationObjectData iolIo) {
		Float value = ((IeSinglePointWithQuality) aSduIe).isOn() ? 1f : 0f;
		return value;
	}

	private Float shortFloatHandler(InformationElement aSduIe, InformationObjectData iolIo) {
		float value = ((IeShortFloat) aSduIe).getValue();
		return value;
	}

	private Float scaledValueHandler(InformationElement aSduIe, InformationObjectData iolIo) {
	    String str = ((IeScaledValue) aSduIe).toString();
	    log.debug("scaledValueHandler: value: {}", str);
	    float value = Float.parseFloat(str.replaceAll("[^0-9\\-.]", "").trim());
		return value;
	}

	private void updateHubStatus(Boolean online, LocalDateTime receivedTime) {
		log.debug("updateHubStatus: hub{}, onlive:{}", iolData.getHubDescription(), online);

		iolData.getInformationObjects().forEach(io -> io.setOnline(online));

		hubService.upsertHubByHubUri(iolData.getHubIpAddress(), iolData.getHubPort(), online);
	}
	
	public void disconnect() {
		connectionGlobal.close();
	}

	public boolean connect() {
		if (keepalivedStatus.isVIPEnabled() && !keepalivedStatus.isMaster()) {
			log.debug("Currently in BACKUP mode - skip connect() to IEC104 Hub {}", iolData.getHubDescription());
			return false;
		}
		
		log.info("Trying to connect to {}...", iolData.getHubDescription());

		InetAddress address;
		try {
			address = InetAddress.getByName(iolData.getHubIpAddress());
		} catch (UnknownHostException e) {
			log.info("Unknown host: {}", iolData.getHubIpAddress());
			return false;
		}

		ClientConnectionBuilder clientConnectionBuilder = new ClientConnectionBuilder(address)
				.setPort(iolData.getHubPort())
				.setConnectionEventListener(this);

		try {
			connectionGlobal = clientConnectionBuilder.build();
			if (timers.containsKey(RECONNECT_TIMER_NAME) && timers.get(RECONNECT_TIMER_NAME).isRunning()) {
				stopTimer(RECONNECT_TIMER_NAME);
			}
		} catch (IOException e) {
			log.info("Unable to connect to remote host: {}:{}. {}", iolData.getHubIpAddress(), iolData.getHubPort(), e.getMessage());
			startReconnectIecHubTimer(RECONNECT_TIMER_TIMEOUT, RECONNECT_TIMER_NAME);
			return false;
		}

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
            	connectionGlobal.close();
            }
        });

		int retries = 2;
		int i = 1;
		boolean connected = false;
		while (!connected && i <= retries) {
			try {
				log.info("Send start DT to {}. Try no. {}", iolData.getHubDescription(), i);
				connectionGlobal.startDataTransfer();
			} catch (InterruptedIOException e2) {
				if (i == retries) {
					log.info("Starting data transfer timed out. Closing connection. Because of no more retries.");
					connectionGlobal.close();
					startReconnectIecHubTimer(RECONNECT_TIMER_TIMEOUT, RECONNECT_TIMER_NAME);
					return false;
				} else {
					log.info("Got Timeout.class Next try.");
					++i;
					continue;
				}
			} catch (IOException e) {
				log.info("Connection closed for the following reason: {}", e.getMessage());
				startReconnectIecHubTimer(RECONNECT_TIMER_TIMEOUT, RECONNECT_TIMER_NAME);
				return false;
			}
			connected = true;
		}
		
		sendGI();

		updateHubStatus(connected, LocalDateTime.now());

		log.info("Hub {}:{} successfully connected", iolData.getHubIpAddress(), iolData.getHubPort());

		return connected;
	}
	
	public void sendGI() {
		List<Integer> asduIds = getIolData().getInformationObjects().stream().map(InformationObjectData::getAsdu).distinct().collect(Collectors.toList());
		for (Integer asduId : asduIds) {
			try {
				if (connectionGlobal != null) {
					if (connectionGlobal.isStopped()) {
						log.warn("sendGI: connectionGlobal is stopped");
						continue;
					}
					if (connectionGlobal.isClosed()) {
						log.warn("sendGI: connectionGlobal is closed");
						continue;
					}
					connectionGlobal.interrogation(asduId, CauseOfTransmission.ACTIVATION, new IeQualifierOfInterrogation(20));
					log.info("GI sent: asduId:{}", asduId);
				} else {
					log.warn("sendGI: connectionGlobal is null");
				}
			} catch (IOException e) {
				log.error("Hub {}:{} - interrogation error: {}", iolData.getHubIpAddress(), getIolData().getHubDescription(), e.getMessage());
			}
		}
	}
	
	public void sendCounterInterrogation() {
//        log("Enter the freeze action: 0=read, 1=counter freeze without reset, 2=counter freeze with reset, 3=counter reset");
//        int action = ??;
//        log("** Sending counter interrogation command.");
//        connection.counterInterrogation(commonAddrParam.getValue(), CauseOfTransmission.ACTIVATION,
//                new IeQualifierOfCounterInterrogation(5, action));
	}

	public boolean sendCmd(String user, String mqttTopic, String value, String reason) {
		log.debug("sendCmd: {}, {}", mqttTopic, value);
		
		
// REMOVE !!!
//		if (!reason.equals("null")) {
//			log.debug("sendCmd skipped");
//			return false;
//		}
		
		LocalDateTime receivedTime = LocalDateTime.now();

		// Check if this hub is connected
		if (connectionGlobal == null) {
			log.error("Can not send command: {}:{} because Hub {}:{} is not connected", mqttTopic, value, iolData.getHubIpAddress(), iolData.getHubPort());
			return false;
		}

		try {
			int commonAddress = 1;
			CauseOfTransmission cot = CauseOfTransmission.ACTIVATION;
	
			int commandType = 0;
			int informationObjectAddress = 0;
			boolean requiresSelect = false;
	
			Optional<InformationObjectData> oio = iolData.getInformationObjects().stream().filter(io -> io.getTopic().equals(mqttTopic)).findFirst();
			if (oio.isPresent()) {
				oio.get().setValue(Float.valueOf(value));
				commandType = oio.get().getTypeId();
				informationObjectAddress = oio.get().getAddress();
				requiresSelect = oio.get().getRequiresSelect();
				log.info("Command: {}: {}: {}", oio.get().getTopic(), value, reason);
			} else {
				log.error("Can not send command: {}:{} because there is no information object with this topic", mqttTopic, value);
				return false;
			}

			switch (commandType) {
			case 45: {
				boolean commandState = value.equals("1") ? true : false;
				connectionGlobal.singleCommand(commonAddress, cot, informationObjectAddress, new IeSingleCommand(commandState, 0, requiresSelect));
				if (requiresSelect) {
					int ioa = informationObjectAddress;
					Runnable command = () -> {
						try {
							connectionGlobal.singleCommand(commonAddress, cot, ioa, new IeSingleCommand(commandState, 0, false));
						} catch (IOException e) {
							log.error("sendCmd: addr:{}, Error: {}", ioa, e.getMessage());
						}
					};
					commandQueue.put(informationObjectAddress, command);
				}
				
				break;
			}

			case 46: {
				DoubleCommandState commandState = switch (value) {
					case "0" -> DoubleCommandState.NOT_PERMITTED_A;
					case "1" -> DoubleCommandState.OFF;
					case "2" -> DoubleCommandState.ON;
					case "3" -> DoubleCommandState.NOT_PERMITTED_B;
					default -> throw new IllegalArgumentException("Unexpected value: " + value);
				};
				
				connectionGlobal.doubleCommand(commonAddress, cot, informationObjectAddress, new IeDoubleCommand(commandState, 0, requiresSelect));
				if (requiresSelect) {
					int ioa = informationObjectAddress;
					Runnable command = () -> {
						try {
							connectionGlobal.doubleCommand(commonAddress, cot, ioa, new IeDoubleCommand(commandState, 0, false));
						} catch (IOException e) {
							log.error("sendCmd: addr:{}, Error: {}", ioa, e.getMessage());
						}
					};
					commandQueue.put(informationObjectAddress, command);
				}
				
				break;
			}

			case 47: {
				StepCommandState commandState = switch (value) {
					case "0" -> StepCommandState.NOT_PERMITTED_A;
					case "1" -> StepCommandState.NEXT_STEP_HIGHER;
					case "2" -> StepCommandState.NEXT_STEP_LOWER;
					case "3" -> StepCommandState.NOT_PERMITTED_B;
					default -> throw new IllegalArgumentException("Unexpected value: " + value);
				};
				connectionGlobal.regulatingStepCommand(commonAddress, cot, informationObjectAddress, new IeRegulatingStepCommand(commandState, 0, requiresSelect));
				if (requiresSelect) {
					int ioa = informationObjectAddress;
					Runnable command = () -> {
						try {
							connectionGlobal.regulatingStepCommand(commonAddress, cot, ioa, new IeRegulatingStepCommand(commandState, 0, false));
						} catch (IOException e) {
							log.error("sendCmd: addr:{}, Error: {}", ioa, e.getMessage());
						}
					};
					commandQueue.put(informationObjectAddress, command);
				}

				break;
			}

			case 48: {
				try {
			        Double doubleVal = Double.valueOf(value);
			        if (doubleVal % 1 != 0) {
			            throw new NumberFormatException("Value is not a whole number: " + value);
			        }
			        Integer val = doubleVal.intValue();
					connectionGlobal.setNormalizedValueCommand(commonAddress, cot, informationObjectAddress, new IeNormalizedValue(val), new IeQualifierOfSetPointCommand(0, requiresSelect));
				} catch (NumberFormatException e) {
					log.error("sendCmd: NumberFormatException for 48: {}", e.getMessage());
				}

				break;
			}

			case 49: {
				try {
			        Double doubleVal = Double.valueOf(value);
			        if (doubleVal % 1 != 0) {
			            throw new NumberFormatException("Value is not a whole number: " + value);
			        }
			        Integer val = doubleVal.intValue();
					connectionGlobal.setScaledValueCommand(commonAddress, cot, informationObjectAddress, new IeScaledValue(val), new IeQualifierOfSetPointCommand(0, requiresSelect));
				} catch (NumberFormatException e) {
					log.error("sendCmd: NumberFormatException for 49: {}", e.getMessage());
				}

				break;
			}

			case 50: {
				try {
					Float val = Float.valueOf(value);
					connectionGlobal.setShortFloatCommand(commonAddress, cot, informationObjectAddress, new IeShortFloat(val), new IeQualifierOfSetPointCommand(0, requiresSelect));
					
				} catch (NumberFormatException e) {
					log.error("sendCmd: NumberFormatException for 50: {}", e.getMessage());
				}

				break;
			}

			case 51: {
				try {
					Integer val = Integer.valueOf(value);
					connectionGlobal.bitStringCommand(commonAddress, cot, informationObjectAddress, new IeBinaryStateInformation(val));
				} catch (NumberFormatException e) {
					log.error("sendCmd: NumberFormatException for 51: {}", e.getMessage());
				}

				break;
			}

			default: {
				log.error("sendCmd: Error: Not implemented commandType: {}", commandType);
				break;
			}
			}

			if (!oio.get().getTargetAddress().isEmpty()) {
				processFormula(oio.get(), receivedTime);
			}

			return true;

		} catch (Exception e) {
			log.error("sendCmd: Error: {}", e.getMessage());
			return false;
		}

	}

	private void processFormula(InformationObjectData iod, LocalDateTime receivedTime) {
		log.debug("processFormula: iod:{}", iod);
		try {
			String targetAddressesStr = iod.getTargetAddress();
			String hubName = iod.getHubName();
			List<Integer> targetAddresses = convertToList(targetAddressesStr);
			for (Integer targetAddressInt : targetAddresses) {
				Optional<InformationObjectData> optionalTargetIo = iolData.getInformationObjects().stream().filter(io -> targetAddressInt.equals(io.getAddress())).findFirst();
				if (optionalTargetIo.isPresent()) {
					if ("TriggerCommand".equals(iod.getFormula())) {
						handleTriggerCommand(iod);
					} else if ("TriggerAPI".equals(iod.getFormula())) {
						handleTriggerApi(iod);
					} else if ("TriggerHub".equals(iod.getFormula())) {
						handleTriggerHub(iod);
					} else if ("SetQuality".equals(iod.getFormula())) {
						handleSetQuality(iod, hubName, receivedTime);
					} else {
						handleTargetAddress(optionalTargetIo.get());
					}
				} else {
					log.error("processAsdu: targetAddress: {} not found", targetAddressInt);
				}
			}
		} catch (Exception e) {
			log.error("processFormula: Error: {}", e.getMessage());
		}

	}
	
	private Float calculateFormula(String formula, InformationObjectData targetIo) {
		// Create a JexlExpression from the expression string
		JexlExpression expression = jexl.createExpression(formula);
		// Create a JexlContext and bind the variable values
		JexlContext context = new MapContext();

		List<String> sourceAddresses = IecUtils.extractVariableNames(formula);
		for (String sourceAddress : sourceAddresses) {
			try {
				Integer sourceAddressInt = Integer.valueOf(sourceAddress.substring(1));
				Optional<InformationObjectData> optionalSourceIo = 
						iolData.getInformationObjects().stream().filter(io -> sourceAddressInt.equals(io.getAddress())).findFirst();
				if (optionalSourceIo.isPresent()) {
					Float sourceValue = optionalSourceIo.get().getTag().getValue() == null ? 0f : optionalSourceIo.get().getTag().getValue();
					context.set(sourceAddress, sourceValue);
				}
			} catch (Exception e) {
				log.error("sourceAddressInt: Error: {}", sourceAddress);
			}

		}

		// Evaluate the expression
		Object result = expression.evaluate(context);
		if (result == null) {
		    log.error("calculateFormula: Expression evaluated to null for io: {}, formula: {}",
		        targetIo.getDescription(), targetIo.getFormula());
		    return 0f;
		}
		return Float.valueOf(result.toString());
	}
	
	private void handleTriggerCommand(InformationObjectData iod) {
		try {
			String commandType = iod.getLevel1();
			switch (commandType) {
			case "DisableTriggerHub": {
				break;
			}
			
			case "CommandToBmsController": {
				log.warn("TriggerCommand - CommandToBmsController removed");
				break;
			}
			
			default:
				break;
			}
		} catch (Exception e) {
			log.error("TriggerCommand: Error: {}", e.getMessage());
		}
	}
	
	private void handleTriggerHub(InformationObjectData iod) {
		try {
		    String hubUri = iod.getLevel1();
		    Integer ioa = Integer.valueOf(iod.getLevel2());
		    Float value = iod.getTag().getValue();
		    Map<String, IecHub> hubs = iecScada.getHubs();
		    IecHub hub = hubs.get(hubUri);
		    if (hub == null) {
		        log.error("handleTriggerHub: Hub '{}' not found for io: {}",
		            hubUri, iod.getDescription());
		        return;
		    }
		    hub.getIolData().getInformationObjects().stream().filter(io -> io.getAddress().equals(ioa)).findFirst().ifPresentOrElse(
		        found -> hub.sendCmd("mqtt-converter", found.getTopic(), value.toString(), "scheduled"),
		        () -> log.error("handleTriggerHub: Information object with address {} not found in hub {} for io: {}",
		            ioa, hubUri, iod.getDescription())
		    );
		} catch (Exception e) {
		    log.error("handleTriggerHub: Error processing TriggerHub for io: {}, error: {}",
		        iod.getDescription(), e.getMessage());
		}
	}
	
	private void handleSetQuality(InformationObjectData iod, String hubName, LocalDateTime receivedTime) {
		try {
			int quality = (int) Math.floor(iod.getValue(false)) | iod.getTag().getQuality();
			iod.getTag().setQuality(quality);
		} catch (Exception e) {
		    log.error("handleSetQuality: Error processing SetQuality for io: {}, error: {}",
		        iod.getDescription(), e.getMessage());
		}
	}
	
	private void handleTriggerApi(InformationObjectData iod) {
		try {
		    String apiUrl = iod.getLevel1();
		    String methodType = iod.getLevel2();
		    String ioa = iod.getLevel3();
		    String localFormula = iod.getLevel4();
		    Float value = iod.getTag().getValue();
		    if (!localFormula.isBlank()) {
				value = calculateFormula(localFormula, iod);
			}
		    String template = "{\"ioa\": %s, \"value\": %s}";
		    String requestBody = String.format(template, ioa, value);
		    log.debug("handleTriggerAPI: for io: {}, requestBody: {}",
		        iod.getDescription(), requestBody);
		    try {
		        HttpClient client = HttpClient.newHttpClient();
		        HttpRequest request = HttpRequest.newBuilder()
		            .uri(URI.create(apiUrl))
		            .header("Content-Type", "application/json")
		            .method(methodType.toUpperCase(), HttpRequest.BodyPublishers.ofString(requestBody))
		            .build();

		        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
		        log.debug("handleTriggerAPI: for io: {}, apiUrl: {}, status: {}, response: {}",
		            iod.getDescription(), apiUrl, response.statusCode(), response.body());
		    } catch (Exception e) {
		        log.error("handleTriggerAPI: failed for io: {}, apiUrl: {}, error: {}",
		            iod.getDescription(), apiUrl, e.getMessage());
		    }
		} catch (Exception e) {
			log.error("handleTriggerAPI: Error: {}", e.getMessage());
		}
	}
	
	private void handleTargetAddress(InformationObjectData iod) {
		try {
		    String formula = iod.getFormula();

		    if ("TriggerAPI".equals(formula)) {
		        handleTriggerApi(iod);
		        return;
		    }

		    Float calcValue = calculateFormula(formula, iod);
		    log.debug("processFormula: CalcValue: {}: {}", iod.getDescription(), calcValue);

		    if (iod.getCommonAddress() == null) {
		        iod.setCommonAddress(1);
		    }

		    int typeId = iod.getTypeId();

		    if (typeId == 45 || typeId == 48 || typeId == 50) {
		        log.warn("processFormula: Unsupported type for formula processing: {}. Formula: {}. Value: {}. Sending command with short float type.",
		                typeId, formula, calcValue);
		        sendCmd("from formula", iod.getTopic(), calcValue.toString(), "automatic optimal");
		        return;
		    }

		    ASdu asdu = switch (typeId) {
		        case 1 -> new ASdu(ASduType.M_SP_NA_1, false, CauseOfTransmission.PERIODIC, false, false, 0,
		                iod.getCommonAddress(), new InformationObject(iod.getAddress(),
		                        new InformationElement[]{new IeSinglePointWithQuality(calcValue > 0, false, false, false, false)}));

		        case 13 -> new ASdu(ASduType.M_ME_NC_1, false, CauseOfTransmission.PERIODIC, false, false, 0,
		                iod.getCommonAddress(), new InformationObject(iod.getAddress(),
		                        new InformationElement[]{new IeShortFloat(calcValue), new IeQuality(false, false, false, false, false)}));

		        default -> throw new IllegalArgumentException("processFormula: Unexpected type: " + typeId);
		    };

		    processAsdu(asdu);
		} catch (Exception e) {
			log.error("handleTargetAddress: Error processing target address for io: {}, error: {}",
			    iod.getDescription(), e.getMessage());
		}
	}

	public List<Integer> convertToList(String commaSeparatedString) {
		return Arrays.stream(commaSeparatedString.split(",")).map(String::trim).map(Integer::parseInt).collect(Collectors.toList());
	}

}
