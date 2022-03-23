package com.uci.transformer.broadcast.consumer;

import static messagerosa.core.model.XMessage.MessageState.NOT_SENT;
import static messagerosa.core.model.XMessage.MessageType.HSM;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.xml.bind.JAXBException;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.uci.transformer.broadcast.user.UserService;
import com.uci.utils.CampaignService;
import com.uci.utils.kafka.SimpleProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import messagerosa.core.model.SenderReceiverInfo;
import messagerosa.core.model.XMessage;
import messagerosa.core.model.XMessagePayload;
import messagerosa.xml.XMessageParser;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

@Component
@RequiredArgsConstructor
@Slf4j
public class BroadcastConsumerReactive {
	private final Flux<ReceiverRecord<String, String>> reactiveKafkaReceiver;

    private static final String SMS_BROADCAST_IDENTIFIER = "Broadcast";
    public static final String XML_PREFIX = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

    @Autowired
    public SimpleProducer kafkaProducer;
    
    @Value("${outbound}")
    public String outboundTopic;
    
    @Autowired
    CampaignService campaignService;
    
    @Autowired
    UserService userService;
    
    @EventListener(ApplicationStartedEvent.class)
    public void onMessage() {
        reactiveKafkaReceiver
                .doOnNext(new Consumer<ReceiverRecord<String, String>>() {
                    @Override
                    public void accept(ReceiverRecord<String, String> stringMessage) {

                		log.info("kafka message received ");
                		
                        final long startTime = System.nanoTime();
                        try {
                            XMessage msg = XMessageParser.parse(new ByteArrayInputStream(stringMessage.value().getBytes()));
                            logTimeTaken(startTime, 1);
                            transformToMany(msg).subscribe(new Consumer<List<XMessage>>() {
                                @Override
                                public void accept(List<XMessage> messages) {
                                    messages = (ArrayList<XMessage>) messages;
                                    for (XMessage msg : messages) {
                                        try {
                                            kafkaProducer.send(outboundTopic, msg.toXML());
                                        } catch (JAXBException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            });
                        } catch (JAXBException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                })
                .doOnError(new Consumer<Throwable>() {
                    @Override
                    public void accept(Throwable e) {
                        System.out.println(e.getMessage());
                        log.error("KafkaFlux exception", e);
                    }
                }).subscribe();

    }

    public Mono<List<XMessage>> transformToMany(XMessage xMessage) {

        ArrayList<XMessage> messages = new ArrayList<>();

        // Get All Users with Data.
        return campaignService.getCampaignFromNameTransformer(xMessage.getCampaign()).map(new Function<JsonNode, List<XMessage>>() {
            @Override
            public List<XMessage> apply(JsonNode campaign) {
                String campaignID = campaign.get("id").asText();
                JSONArray users = userService.getUsersFromFederatedServers(campaignID);
                JsonNode firstTransformer = campaign.findValues("transformers").get(0).get(0);
                
            	ObjectMapper mapper = new ObjectMapper();
                ObjectNode node = mapper.createObjectNode();
            	node.put("body", firstTransformer.get("meta").get("body").asText());
            	node.put("type", firstTransformer.get("meta").get("type").asText());
            	node.put("user", firstTransformer.get("meta").get("user").asText());
            	
            	ArrayNode sampleData = mapper.createArrayNode();
            	for (int i = 0; i < users.length(); i++) {
                	ObjectNode userData = mapper.createObjectNode();
                	userData.put("task", "coding");
                	userData.put("name", ((JSONObject) users.get(i)).getString("whatsapp_mobile_number"));
                	userData.put("__index", i);
                	sampleData.add(userData);
            	}
            	node.put("sampleData", sampleData);
            	
            	ArrayList<JSONObject> usersMessage = userService.getUsersMessageByTemplate(node);
                
            	log.info("usersMessage: "+usersMessage);
            	
            	usersMessage.forEach(userMsg -> {
            		int j = Integer.parseInt(userMsg.get("__index").toString());
            		 String userPhone = ((JSONObject) users.get(j)).getString("whatsapp_mobile_number");
//            		 userPhone = "7597185708";
            		// Create new xMessage from response
                   XMessage nextMessage = getClone(xMessage);
                   XMessagePayload payload = XMessagePayload.builder().build();
                   payload.setText(userMsg.get("body").toString());
                   
                   log.info("index: "+j+", body: "+userMsg.get("body").toString()+", phone:"+userPhone);
                   
                   nextMessage.setPayload(payload);

//                   // Update user info
                   SenderReceiverInfo to = SenderReceiverInfo.builder().userID(userPhone).build();                   
                   nextMessage.setTo(to);

                   nextMessage.setMessageState(NOT_SENT);
                   nextMessage.setMessageType(HSM);
                   
                   messages.add(nextMessage);
            	});
            	
                return messages;
            }
        });

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
