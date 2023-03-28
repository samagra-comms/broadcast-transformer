package com.uci.transformer.broadcast.consumer;

import static messagerosa.core.model.XMessage.MessageState.NOT_SENT;
import static messagerosa.core.model.XMessage.MessageType.HSM;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

	@EventListener(ApplicationStartedEvent.class)
	public void onMessage() {
		reactiveKafkaReceiver.doOnNext(new Consumer<ReceiverRecord<String, String>>() {
			@Override
			public void accept(ReceiverRecord<String, String> stringMessage) {

				log.info("kafka message received ");

				final long startTime = System.nanoTime();
				try {
					XMessage msg = XMessageParser.parse(new ByteArrayInputStream(stringMessage.value().getBytes()));
					logTimeTaken(startTime, 1);
					ArrayList<XMessage> messages = (ArrayList<XMessage>) transformToMany(msg);
					if(messages.size() > 0){
						for (XMessage message : messages) {
							try {
								kafkaProducer.send(processOutbound, message.toXML());
							} catch (JAXBException e) {
								e.printStackTrace();
							}
						}
					} else {
						log.error("No user messages to broadcast.");
					}
				} catch (JAXBException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).doOnError(new Consumer<Throwable>() {
			@Override
			public void accept(Throwable e) {
				System.out.println(e.getMessage());
				log.error("KafkaFlux exception", e);
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
										if (user.get("fcmClickActionUrl") != null) {
											data = new Data();
											data.setKey("fcmClickActionUrl");
											data.setValue(user.get("fcmClickActionUrl").toString());
											dataArrayList.add(data);
										}
										if(user.get("data") != null){
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
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					} else {
						log.info("No federatedUsers found.");
					}
				});
			}
		} catch (Exception e) {
			log.error("Exception in transformToMany: "+e.getMessage());
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

	private void logTimeTaken(long startTime, int checkpointID) {
		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / 1000000;
		log.info(String.format("CP-%d: %d ms", checkpointID, duration));
	}
}
