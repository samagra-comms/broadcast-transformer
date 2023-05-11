package com.uci.transformer.broadcast.controller;

import java.util.Date;

import com.uci.utils.UtilHealthService;
import com.uci.utils.kafka.SimpleProducer;
import com.uci.utils.model.ApiResponse;
import com.uci.utils.model.ApiResponseParams;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
public class HealthController {

    @Autowired
    UtilHealthService healthService;

    @Autowired
    public SimpleProducer kafkaProducer;

    @RequestMapping(value = "/health", method = RequestMethod.GET, produces = {"application/json", "text/json"})
    public Mono<ResponseEntity<ApiResponse>> statusCheck() {
        return healthService.getAllHealthNode().map(health -> ApiResponse.builder()
                .id("api.health")
                .params(ApiResponseParams.builder().build())
                .result(health)
                .build()
        ).map(response -> {
            if (((JsonNode) response.result).get("status").textValue().equals(Status.UP.getCode())) {
                response.responseCode = HttpStatus.OK.name();
                return new ResponseEntity<>(response, HttpStatus.OK);
            } else {
                response.responseCode = HttpStatus.SERVICE_UNAVAILABLE.name();
                return new ResponseEntity<>(response, HttpStatus.SERVICE_UNAVAILABLE);
            }
        });
    }

    @RequestMapping(value = "/testNotification", method = RequestMethod.GET, produces = {"application/json", "text/json"})
    public ResponseEntity<String> testNotification(@RequestParam(value = "size", required = false) String size) {
        String xmessage = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<xMessage><adapterId>6efa8087-0939-49ab-b8e5-5676e036c17b</adapterId><app>Load Testing Firebase Broadcast Bot</app><botId>7e6f2bc7-1804-4704-86d5-8e49e2b2ab29</botId><channel>web</channel><channelURI>web</channelURI><from><bot>false</bot><broadcast>false</broadcast><userID>admin</userID></from><lastMessageID/><messageId><channelMessageId>7a66858d-6a1c-4737-8697-a562ec11a9fa</channelMessageId><replyId>7597185708</replyId></messageId><messageState>NOT_SENT</messageState><messageType>HSM</messageType><ownerId>8f7ee860-0163-4229-9d2a-01cef53145ba</ownerId><ownerOrgId>org01</ownerOrgId><payload><data><key>fcmToken</key><value>ds0SOYMeQE2nOb2C50y3z0:APA91bFWCB3Yhxi6oHh3yJXn5lr3a0FeNxYEuEQaxb0I_yeLpirz4w44iYThKE3hhapRvD9Udur7NIyIaoh_DijRlSBNwPxmBecfaEfJjoKo8vek2YLCV5Xbo_PSh7B0eqCiAUAhFftr</value></data><data><key>fcmClickActionUrl</key><value>nipunlakshya://chatbot</value></data><text>Hello Surabhi Test 2-9783246247, Test Notification</text><title>Firebase Test Notification</title></payload><provider>firebase</provider><providerURI>firebase</providerURI><sessionId>1a85b650-c747-4db9-9f60-82378f05aa5f</sessionId><timestamp>1683799221719</timestamp><to><bot>false</bot><broadcast>false</broadcast><userID>9783246247</userID></to><transformers><id>a9c757de-d3de-4ed8-9924-42d7554bf512</id><metaData><entry><key>formID</key><value/></entry><entry><key>startingMessage</key><value>Hi Load Testing</value></entry><entry><key>federatedUsers</key><value/></entry><entry><key>botOwnerOrgID</key><value>org01</value></entry><entry><key>botId</key><value>7e6f2bc7-1804-4704-86d5-8e49e2b2ab29</value></entry><entry><key>botOwnerID</key><value>8f7ee860-0163-4229-9d2a-01cef53145ba</value></entry><entry><key>id</key><value>a9c757de-d3de-4ed8-9924-42d7554bf512</value></entry><entry><key>type</key><value>broadcast</value></entry><entry><key>title</key><value>Firebase Test Notification</value></entry></metaData></transformers></xMessage>\n";
        try {
            Date start = new Date();
            int intSize = 0;
            if (size != null && !size.isEmpty()) {
                intSize = Integer.parseInt(size);
            } else {
                intSize = 0;
            }
            for (int i = 0; i < intSize; i++) {
                log.info("push notification count : " + i);
                kafkaProducer.send("process-outbound", xmessage);
            }
            Date endDate = new Date();
            return new ResponseEntity<>("process complete start date : " + start + " end : " + endDate, HttpStatus.OK);
        } catch (Exception ex) {
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }
}
