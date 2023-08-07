package com.uci.transformer.broadcast.consumer;

import static messagerosa.core.model.XMessage.MessageState.NOT_SENT;
import static messagerosa.core.model.XMessage.MessageType.HSM;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.function.Consumer;

import javax.xml.bind.JAXBException;

import messagerosa.core.model.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uci.utils.kafka.SimpleProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import messagerosa.xml.XMessageParser;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;
import com.fasterxml.jackson.core.type.TypeReference;

@Component
@RequiredArgsConstructor
@Slf4j
public class BroadcastConsumerReactive {
	private final Flux<ReceiverRecord<String, String>> reactiveKafkaReceiver;

	@Autowired
	public SimpleProducer kafkaProducer;

	@Value("${processOutbound}")
	public String processOutbound;

	private long notificationProcessedCount;
	private long consumeCount;
	@Value("${notificationOutbound}")
	public String notificationOutbound;

	@EventListener(ApplicationStartedEvent.class)
	public void onMessage() {
		reactiveKafkaReceiver.doOnNext(new Consumer<ReceiverRecord<String, String>>() {
			@Override
			public void accept(ReceiverRecord<String, String> stringMessage) {
				consumeCount++;
				log.info("BroadcastConsumerReactive:Consume topic from kafka: "+consumeCount);

				final long startTime = System.nanoTime();
				try {
					XMessage msg = XMessageParser.parse(new ByteArrayInputStream(stringMessage.value().getBytes()));
					logTimeTaken(startTime, 0, null);
					ArrayList<XMessage> messages = (ArrayList<XMessage>) transformToMany(msg);
					if(messages.size() > 0){
						log.info("BroadcastConsumerReactive:transformToMany::Count: "+messages.size());
						for (XMessage message : messages) {
							try {
								if(message != null && message.getProviderURI().equals("firebase") && message.getChannelURI().equals("web")){
									kafkaProducer.send(notificationOutbound, message.toXML());
									notificationProcessedCount++;
									logTimeTaken(startTime, 0, "Notification processed by broadcast-transformer: " + notificationProcessedCount + "  :: broadcast-transformer-process-end: %d ms");
								} else{
									kafkaProducer.send(processOutbound, message.toXML());
								}
							} catch (JAXBException e) {
								log.error("BroadcastConsumerReactive: Unable to send topic to kafka:Exception: "+e.getMessage());
							}
						}
					} else {
						log.error("BroadcastConsumerReactive: Unable to send topic to kafka:Exception: No user messages to broadcast.");
					}
				} catch (Exception e) {
                    log.error("BroadcastConsumerReactive: Unable to send topic to kafka:Exception: "+e.getMessage());
				}
			}
		}).doOnError(new Consumer<Throwable>() {
			@Override
			public void accept(Throwable e) {
                log.error("BroadcastConsumerReactive: Unable to send topic to kafka:Exception: "+e.getMessage());
			}
		}).subscribe();

	}

	public List<XMessage> transformToMany(XMessage xMessage) {
		ArrayList<XMessage> messages = new ArrayList<>();

		try {
			ObjectMapper mapper = new ObjectMapper();
			ArrayList<Transformer> transformers = xMessage.getTransformers();

			/* Create XMessage clone & remove federated users data from it. */
			XMessage xMessageClone = getClone(xMessage);
			ArrayList<Transformer> transformers2 = xMessageClone.getTransformers();
			for (int i = 0; i < transformers2.size(); i++) {
				Transformer transformer2 = transformers2.get(i);
				if (transformer2.getMetaData() != null && transformer2.getMetaData().get("federatedUsers") != null) {
					HashMap<String, String> metaData = transformer2.getMetaData();
					metaData.put("federatedUsers", "");
					transformer2.setMetaData(metaData);
				}
				transformers2.set(i, transformer2);
			}
			xMessageClone.setTransformers(transformers2);

			if (transformers != null) {
				transformers.forEach(transformer -> {
					if (transformer.getMetaData() != null && transformer.getMetaData().get("type") != null
							&& transformer.getMetaData().get("type").toString().equals("broadcast")
							&& transformer.getMetaData().get("federatedUsers") != null) {
						try {
							if(transformer.getMetaData().get("federatedUsers") != null && !transformer.getMetaData().get("federatedUsers").isEmpty()) {
								JSONArray federatedUsers = new JSONObject(
										transformer.getMetaData().get("federatedUsers").toString()).getJSONArray("list");

								for (int i = 0; i < federatedUsers.length(); i++) {
									JSONObject user = (JSONObject) federatedUsers.get(i);
									XMessage nextMessage = getClone(xMessageClone);

									XMessagePayload payload = XMessagePayload.builder().build();
									payload.setText(user.get("message").toString());
									if (transformer.getMetaData().get("title") != null) {
										payload.setTitle(transformer.getMetaData().get("title").toString());
									}


									if(user.get("fcmToken") != null) {
										ArrayList<Data> dataArrayList = new ArrayList<>();
										Data data = new Data();
										data.setKey("fcmToken");
										data.setValue(user.get("fcmToken").toString());
										dataArrayList.add(data);
										if (transformer.getMetaData().get("data") != null) {
											Map<String, String> dataMapForTransformer = mapper.readValue(
												transformer.getMetaData().get("data").toString(),
												new TypeReference<Map<String, String>>() {});
											for(String dataKey : dataMapForTransformer.keySet()){
												data = new Data();
												data.setKey(dataKey);
												data.setValue(dataMapForTransformer.get(dataKey));
												dataArrayList.add(data);
											}
										}
										if (user.get("fcmClickActionUrl") != null) {
											data = new Data();
											data.setKey("fcmClickActionUrl");
											data.setValue(user.get("fcmClickActionUrl").toString());
											dataArrayList.add(data);
										}
										if(!user.isNull("data")){
											Map<String, String> dataMap = mapper.readValue(user.get("data").toString(), new TypeReference<Map<String, String>>() {});
											for(String dataKey : dataMap.keySet()){
												data = new Data();
												data.setKey(dataKey);
												data.setValue(dataMap.get(dataKey));
												dataArrayList.add(data);
											}
										}
										payload.setData(dataArrayList);
									}


									log.info("message: " + user.get("message").toString() + ", phone:"
											+ user.get("phone").toString());

									nextMessage.setPayload(payload);

									// Update user info
									SenderReceiverInfo to = SenderReceiverInfo.builder().userID(user.get("phone").toString())
											.build();
//									Map<String, String> toMeta = new HashMap();
//									try{
//										if(user.get("fcmToken") != null) {
//											toMeta.put("fcmToken", user.get("fcmToken").toString());
//											if(user.get("fcmClickActionUrl") != null) {
//												toMeta.put("fcmClickActionUrl", user.get("fcmClickActionUrl").toString());
//											}
//										}
//									} catch (Exception e){
//
//									}
//									to.setMeta(toMeta);

									nextMessage.setTo(to);

									nextMessage.setMessageState(NOT_SENT);
									nextMessage.setMessageType(HSM);

									messages.add(nextMessage);
								}
							}
						} catch (Exception e) {
                            log.error("BroadcastConsumerReactive:transformToMany::Exception: "+e.getMessage());
						}

					} else {
						log.error("BroadcastConsumerReactive:transformToMany::Exception: Federated users are not found.");
					}
				});
			}
		} catch (Exception e) {
			log.error("BroadcastConsumerReactive:transformToMany::Exception: "+e.getMessage());
		}
		return messages;
	}

	@Nullable
	private XMessage getClone(XMessage nextMessage) {
		XMessage cloneMessage = null;
		try {
			cloneMessage = XMessageParser.parse(new ByteArrayInputStream(nextMessage.toXML().getBytes()));
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return cloneMessage;
	}

	private void logTimeTaken(long startTime, int checkpointID, String formatedMsg) {
		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / 1000000;
		if(formatedMsg == null) {
			log.info(String.format("CP-%d: %d ms", checkpointID, duration));
		} else {
			log.info(String.format(formatedMsg, duration));
		}
	}
}
