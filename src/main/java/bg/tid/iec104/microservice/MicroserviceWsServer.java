package bg.tid.iec104.microservice;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.java_websocket.WebSocket;
import org.java_websocket.drafts.Draft;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.exceptions.InvalidDataException;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.handshake.ServerHandshakeBuilder;
import org.java_websocket.protocols.IProtocol;
import org.java_websocket.protocols.Protocol;
import org.java_websocket.server.WebSocketServer;
import org.openmuc.j60870.ASdu;
import org.openmuc.j60870.ASduType;
import org.openmuc.j60870.CauseOfTransmission;
import org.openmuc.j60870.ie.IeQuality;
import org.openmuc.j60870.ie.IeShortFloat;
import org.openmuc.j60870.ie.IeSinglePointWithQuality;
import org.openmuc.j60870.ie.InformationElement;
import org.openmuc.j60870.ie.InformationObject;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import bg.tid.iec104.db.service.CommControllerService;
import bg.tid.iec104.service.IecScada;
import bg.tid.shared.mqtt.MqttRemoteRequestDto;
import bg.tid.shared.socket.SocketValueTransferDto;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Component
@RequiredArgsConstructor
public class MicroserviceWsServer {

	private final Environment env;
	private final CommControllerService commControllerService;
	private final IecScada iecScada;
	private final ObjectMapper objectMapper;

	private boolean serverEnabled;
	private int serverConectionLostTimeout;
	private String host;
	private int port;
	private volatile WebSocketServer server;

	private ExecutorService executor = Executors.newCachedThreadPool();
	
	@PostConstruct
	private void init() {
		serverEnabled = Boolean.valueOf(env.getProperty("micro.server.enabled"));
		serverConectionLostTimeout = env.getProperty("micro.server.conectionlosttimeout", Integer.class, 20); // seconds
		if (serverEnabled) {
			host = env.getProperty("micro.server.host");
			port = Integer.valueOf(env.getProperty("micro.server.port"));

			setupServer();
		}
	}

	@PreDestroy
	private void stopServer() {
		log.info("Stopping Microservice WS Server...");
		try {
			server.stop();
		} catch (InterruptedException e) {
			log.error(e.getMessage());
		}
		log.info("Microservice WS Server stopped");
		executor.shutdown(); // Stop accepting new tasks
		try {
			// Wait for 5 seconds to finish ongoing tasks
			if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
				log.warn("Executor did not terminate in the specified time. Forcing shutdown...");
				executor.shutdownNow(); // Cancel remaining tasks
			}
		} catch (InterruptedException e) {
			log.error("Shutdown interrupted. Forcing shutdown...");
			executor.shutdownNow(); // Force shutdown if interrupted
			Thread.currentThread().interrupt(); // Restore interrupted status
		}
	}

	@Async
	@EventListener
	void handleAsyncEvent(String event) {
		log.info(String.format("Async event recevied: %s", event));
		if ("appStartCompleted".equals(event)) {
			try {
				server.start();
			} catch (Exception e) {
				log.info("handleAsyncEvent: {}", e.getMessage());
			}
		}
	}

	@Async
	@EventListener
	void handleAsyncSocketEvent(MqttRemoteRequestDto event) {
		log.info("Async socket event recevied: {}", event);
		sendMqttRemoteCommand(event);
	}
	
	private void sendMqttRemoteCommand(MqttRemoteRequestDto dto) {
		log.debug("sendMqttRemoteCommand: dto:{}", dto);
		
		try {
			String json = objectMapper.writeValueAsString(dto);
			for (WebSocket webSocket : server.getConnections()) {
				try {
					webSocket.send(json);
					log.info("sendMqttRemoteCommand: sent command:{} , to:{}", json, webSocket.getResourceDescriptor().substring(1));
				} catch (Exception e) {
					log.error("sendMqttRemoteCommand: Error: {}", e.getMessage());
				}
			}
		} catch (JsonProcessingException e) {
			log.error("sendMqttRemoteCommand: objectMapper Error:{}", e.getMessage());
			return;
		}

	}

	private void setupServer() {
		ArrayList<IProtocol> protocols = new ArrayList<>();
		protocols.add(new Protocol("grid-one-socket"));
		protocols.add(new Protocol(""));
		Draft_6455 draft = new Draft_6455(Collections.emptyList(), protocols);
		List<Draft> drafts = new ArrayList<>();
		drafts.add(draft);

		server = new WebSocketServer(new InetSocketAddress(host, port), drafts) {

			@Override
			public void onStart() {
				try {

					log.info("Microservice websocket server started on port:{}", port);
				} catch (Exception e) {
					log.error("onStart: Error: {}", e.getMessage());
				}
			}

			@Override
			public void onOpen(WebSocket conn, ClientHandshake handshake) {
				try {
					String upgradeField = handshake.getFieldValue("Upgrade");
					if (upgradeField == null || upgradeField == "") {
						log.debug("On HTTP Request, for healthcheck");
						conn.close(10200);
						return;
					}
					String protocol = conn.getProtocol().getProvidedProtocol();
					switch (protocol) {
					case "grid-one-socket":
						break;

					default:
						break;
					}

					String microserviceName = conn.getResourceDescriptor().substring(1);
					commControllerService.upsertCommControllerByName(microserviceName, true);

					log.debug("New Microservice connected: {}", microserviceName);
				} catch (Exception e) {
					log.error("onOpen: Error: {}", e.getMessage());
				}

			}

			@Override
			public void onClose(WebSocket conn, int code, String reason, boolean remote) {
				try {
					String protocol = conn.getProtocol().getProvidedProtocol();
					switch (protocol) {
					case "grid-one-socket":
						break;

					default:
						break;
					}

					String microserviceName = conn.getResourceDescriptor().substring(1);
					commControllerService.upsertCommControllerByName(microserviceName, false);

					log.info("Microservice connection to {} closed. Reason: {}, Code: {}", conn.getResourceDescriptor(), reason, code);
				} catch (Exception e) {
					log.error("onClose: Error: {}", e.getMessage());
				}

			}

			@Override
			public void onMessage(WebSocket conn, String message) {
				try {
					log.debug("New Microservice message: {}", message.substring(0, message.length() > 1000 ? 1000 : message.length()));

					executor.execute(() -> {
						String protocol = conn.getProtocol().getProvidedProtocol();
						switch (protocol) {
						case "grid-one-socket": {

							if (message.length() > 0) {
								log.trace("onMessage: Received message: {}", message);
								int idx = message.indexOf("'");
								String msgType = message.substring(0, idx);
								String json = message.substring(idx + 1);

								switch (msgType) {
								case "status": {
									log.debug("onMessage: status: {} (ignored)", message);
									break;
								}
								case "status-batch": {
									log.debug("onMessage: status-batch: {} (ignored)", message);
									break;
								}
								case "value": {
									try {
										SocketValueTransferDto dto = objectMapper.readValue(json, SocketValueTransferDto.class);
										iecScada.getHubs().values().stream().forEach(hub -> {
											hub.getIolData().getInformationObjects().stream()
												.filter(io -> dto.getMqttTopic().equals(io.getIncomingTopic()))
												.findFirst() // Stop at the first matching InformationObjectData
												.ifPresent(io -> {
													ASdu aSdu = switch (io.getTypeId()) {
													case 1 -> {
														InformationObject newIo = new InformationObject(io.getAddress(),
																new InformationElement[][] { { new IeSinglePointWithQuality(dto.getValue() == 1, false, false, false, false) } });
														yield new ASdu(ASduType.M_SP_NA_1, false, CauseOfTransmission.SPONTANEOUS, false, false, 1, 1, newIo);
													}

													case 13 -> {
														InformationObject newIo = new InformationObject(io.getAddress(), new InformationElement[][] {
																{ new IeShortFloat(dto.getValue()), new IeQuality(false, false, false, false, false) } });
														yield new ASdu(ASduType.M_ME_NC_1, false, CauseOfTransmission.PERIODIC, false, false, 1, 1, newIo);
													}

													default -> throw new IllegalArgumentException("onMessage: Unexpected value: " + io.getTypeId());
													};

													hub.processAsdu(aSdu); // Process ASdu within the correct hub context
												});
										});
									} catch (Exception e) {
										log.error("onMessage: value Error: {}", e.getMessage());
									}
									break;
								}
								default: {
									log.error("onMessage: Unknown message type: {}", msgType);
									break;
								}
								}
								break;
							} else {
								log.error("onMessage: Received empty message from:{}", conn.getResourceDescriptor());
							}
						}

						default: {
							log.error("onMessage - default");
							break;
						}
						}

					});
				} catch (Exception e) {
					log.error("onMessage: Error: {}", e.getMessage());
				}

			}

			@Override
			public void onError(WebSocket conn, Exception ex) {
				log.error("Microservice server error: {}", ex.getMessage());

			}

			@Override
			public ServerHandshakeBuilder onWebsocketHandshakeReceivedAsServer(WebSocket webSocket, Draft draft, ClientHandshake clientHandshake) throws InvalidDataException {
				try {
					String user = env.getProperty("micro.username");
					String pass = env.getProperty("micro.password");

					String identifier = clientHandshake.getResourceDescriptor();

					String username = null;
					byte[] password = null;
					if (clientHandshake.hasFieldValue("Authorization")) {
						String authorization = clientHandshake.getFieldValue("Authorization");
						if (authorization != null && authorization.toLowerCase().startsWith("basic")) {
							// Authorization: Basic base64credentials
							String base64Credentials = authorization.substring("Basic".length()).trim();
							byte[] credDecoded = Base64.getDecoder().decode(base64Credentials);
							// split credentials on username and password
							for (int i = 0; i < credDecoded.length; i++) {
								if (credDecoded[i] == ':') {
									username = new String(Arrays.copyOfRange(credDecoded, 0, i), StandardCharsets.UTF_8);
									if (i + 1 < credDecoded.length) {
										password = Arrays.copyOfRange(credDecoded, i + 1, credDecoded.length);
									}
									break;
								}
							}
						}
					}

					if (password == null || password.length < 8)
						throw new InvalidDataException(401, "Invalid password length");
					String passwordStr = new String(password, StandardCharsets.UTF_8);
					log.debug("Authentication for MICROSERVICE: {} with username: {} and password: {}.", identifier, username, passwordStr);

					if (username != null && passwordStr != null) {
						if (!username.equals(user)) {
							log.error("Authentication for MICROSERVICE: {} failed. Wrong username", identifier);
						} else if (!passwordStr.equals(pass)) {
							log.error("Authentication for MICROSERVICE: {} failed. Wrong password", identifier);
						} else {
							log.debug("MICROSERVICE: {} successfully connected", identifier.substring(1));
						}
					} else {
						log.error("Invalid credentials provided for MICROSERVICE: {}", identifier);
					}

					return super.onWebsocketHandshakeReceivedAsServer(webSocket, draft, clientHandshake);

				} catch (Exception e) {
					log.error("onWebsocketHandshakeReceivedAsServer: Error: {}", e.getMessage());
					throw new InvalidDataException(1002, e.getMessage());
				}
			}
		};
		server.setConnectionLostTimeout(serverConectionLostTimeout);
		server.setReuseAddr(true);

	}

}
