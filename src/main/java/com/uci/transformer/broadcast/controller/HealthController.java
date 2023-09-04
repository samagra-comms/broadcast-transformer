package com.uci.transformer.broadcast.controller;

import java.util.Date;

import com.uci.utils.UtilHealthService;
import com.uci.utils.kafka.SimpleProducer;
import com.uci.utils.model.ApiResponse;
import com.uci.utils.model.ApiResponseParams;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

    @Value("${test.transformer.xmessage}")
    private String testTransformerXmessage;

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

    @RequestMapping(value = "/sendNotification", method = RequestMethod.GET, produces = {"application/json", "text/json"})
    public ResponseEntity<String> testNotification(@RequestParam(value = "size", required = false) String size) {
        String xmessage = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<xMessage><adapterId>21f1d315-55cf-44e3-8355-4743d6519650</adapterId><app>Load Testing Firebase Broadcast Bot</app><botId>7e6f2bc7-1804-4704-86d5-8e49e2b2ab29</botId><channel>web</channel><channelURI>web</channelURI><from><bot>false</bot><broadcast>false</broadcast><userID>admin</userID></from><messageId><channelMessageId>f52ecb11-0428-47ce-86cc-0522f16f7300</channelMessageId><replyId>9876543210</replyId></messageId><messageState>NOT_SENT</messageState><messageType>HSM</messageType><ownerId>8f7ee860-0163-4229-9d2a-01cef53145ba</ownerId><ownerOrgId>org01</ownerOrgId><payload><data><key>fcmToken</key><value>e2-4QATfkPFKf0-zsACOMp:APA91bFBH9gz0RxrUqdIei6O-8DCNbHEw6CqJNVNiZyqMNguVDOZJWxSGkVdTmbO3JNf0aGBkj-rxz77K2Ns4xE1D4CU-hZF-MdikfjlZzi0Hcrr_9rNzk7802BTyecIMLD6mhyBacON</value></data><data><key>fcmClickActionUrl</key><value>nipunlakshya://chatbot</value></data><text>Hello Pankaj-9783246247, Test Notification</text><title>Firebase Test Notification</title></payload><provider>firebase</provider><providerURI>firebase</providerURI><sessionId>c1af25ab-ea15-4a20-9016-64cadc20cd09</sessionId><timestamp>1692609727746</timestamp><to><bot>false</bot><broadcast>false</broadcast><userID>9783246247</userID></to><transformers><id>a9c757de-d3de-4ed8-9924-42d7554bf512</id><metaData><entry><key>formID</key><value/></entry><entry><key>startingMessage</key><value>Hi Load Testing</value></entry><entry><key>federatedUsers</key><value/></entry><entry><key>botOwnerOrgID</key><value>org01</value></entry><entry><key>botId</key><value>7e6f2bc7-1804-4704-86d5-8e49e2b2ab29</value></entry><entry><key>botOwnerID</key><value>8f7ee860-0163-4229-9d2a-01cef53145ba</value></entry><entry><key>id</key><value>a9c757de-d3de-4ed8-9924-42d7554bf512</value></entry><entry><key>type</key><value>broadcast</value></entry><entry><key>title</key><value>Firebase Test Notification</value></entry></metaData></transformers></xMessage>\n";
        try {
            Date start = new Date();
            long longSize = 0;
            if (size != null && !size.isEmpty()) {
                longSize = Long.parseLong(size);
            } else {
                longSize = 0;
            }
            for (long i = 0; i < longSize; i++) {
                log.info("push notification count : " + i);
                kafkaProducer.send("notification-outbound", xmessage);
            }
            Date endDate = new Date();
            return new ResponseEntity<>("process complete start date : " + start + " end : " + endDate, HttpStatus.OK);
        } catch (Exception ex) {
            ex.printStackTrace();
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @RequestMapping(value = "/testTransformer", method = RequestMethod.GET, produces = {"application/json", "text/json"})
    public ResponseEntity<String> testTransformer(@RequestParam(value = "size", required = false) String size) {
        try {
            Date start = new Date();
            long longSize = 0;
            if (size != null && !size.isEmpty()) {
                longSize = Long.parseLong(size);
            } else {
                longSize = 0;
            }
            for (long i = 0; i < longSize; i++) {
                log.info("push notification count : " + i);
                kafkaProducer.send("com.odk.transformer", testTransformerXmessage);
            }
            Date endDate = new Date();
            return new ResponseEntity<>("process complete start date : " + start + " end : " + endDate, HttpStatus.OK);
        } catch (Exception ex) {
            ex.printStackTrace();
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }
}
