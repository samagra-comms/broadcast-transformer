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

    @RequestMapping(value = "/sendNotification", method = RequestMethod.GET, produces = {"application/json", "text/json"})
    public ResponseEntity<String> testNotification(@RequestParam(value = "size", required = false) String size) {
        String xmessage = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<xMessage><adapterId>6efa8087-0939-49ab-b8e5-5676e036c17c</adapterId><app>Custom Notification Broadcast</app><botId>334c9fc0-5033-40ab-9708-1045c1d4961f</botId><channel>web</channel><channelURI>web</channelURI><from><bot>false</bot><broadcast>false</broadcast><userID>admin</userID></from><messageId><channelMessageId>458621ea-5f4a-4fea-b609-f353c5a64c37</channelMessageId><replyId>7597185708</replyId></messageId><messageState>NOT_SENT</messageState><messageType>HSM</messageType><ownerId>8f7ee860-0163-4229-9d2a-01cef53145ba</ownerId><ownerOrgId>org01</ownerOrgId><payload><data><key>fcmToken</key><value>fJsJD2E-RG2ZOsTVDnEHEp:APA91bGNuzh6MQ0uYRWpLTTtHwf8Qy-Zb2xfSbpcaMHnpI9OAodWzpzizj1RhobUOX7vB4sG8BqFq-HgI7KiBfqx1MeoSdNoZZ2rmLsC_Lo8p6KO0l-RqbgeoN51-6F_enKGAPtr2Zno</value></data><data><key>fcmClickActionUrl</key><value>nipunlakshya://chatbot</value></data><data><key>botDesc</key><value>This is testing description</value></data><data><key>botId</key><value>This is testing bot id</value></data><text>Goa Role Recall</text><title>Goa Role Recall</title></payload><provider>firebase</provider><providerURI>firebase</providerURI><sessionId>2b450fba-dd1c-41e2-b44c-77c224eb28ce</sessionId><timestamp>1686649674946</timestamp><to><bot>false</bot><broadcast>false</broadcast><userID>7415148877</userID></to><transformers><id>0a80cd8b-cecd-4e50-ac8d-bab3dcd78781</id><metaData><entry><key>formID</key><value>recall_form_v6</value></entry><entry><key>startingMessage</key><value>Hi CN</value></entry><entry><key>federatedUsers</key><value/></entry><entry><key>botOwnerOrgID</key><value>org01</value></entry><entry><key>botId</key><value>334c9fc0-5033-40ab-9708-1045c1d4961f</value></entry><entry><key>botOwnerID</key><value>8f7ee860-0163-4229-9d2a-01cef53145ba</value></entry><entry><key>id</key><value>0a80cd8b-cecd-4e50-ac8d-bab3dcd78781</value></entry><entry><key>type</key><value>broadcast</value></entry><entry><key>title</key><value>Goa Role Recall</value></entry></metaData></transformers></xMessage>\n";
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
        String xmessage = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<xMessage><adapterId>44a9df72-3d7a-4ece-94c5-98cf26307324</adapterId><app>UCI Demo</app><botId>5768d2b7-15c5-4ab2-bab1-e7ba2c638ec2</botId><channel>WhatsApp</channel><channelURI>WhatsApp</channelURI><from><bot>false</bot><broadcast>false</broadcast><userID>admin</userID></from><messageId><channelMessageId>ABEGkZeDJGJHAhAhmDNbLpGYH51DIfbN_Qhb</channelMessageId></messageId><messageState>REPLIED</messageState><messageType>TEXT</messageType><ownerId>8f7ee860-0163-4229-9d2a-01cef53145ba</ownerId><ownerOrgId>org01</ownerOrgId><payload><text>Hi UCI</text></payload><provider>Netcore</provider><providerURI>Netcore</providerURI><sessionId>958bea64-9538-4972-9de1-abef8480a410</sessionId><timestamp>1690181147000</timestamp><to><bot>false</bot><broadcast>false</broadcast><campaignID>UCI Demo</campaignID><deviceID>4f7d3d09-602b-4c31-8371-00ed96b4e15b</deviceID><deviceType>PHONE</deviceType><encryptedDeviceID>RHzlIP6jVvPAmyOUEmhkmtb0wsmh8St/ty+pM5Q+4W4=</encryptedDeviceID><userID>9783246247</userID></to><transformers><id>9c46aa04-b72e-423a-aab7-a1a05842d99b</id><metaData><entry><key>formID</key><value>UCI-demo-1</value></entry><entry><key>startingMessage</key><value>Hi UCI</value></entry><entry><key>botOwnerOrgID</key><value>org01</value></entry><entry><key>botId</key><value>5768d2b7-15c5-4ab2-bab1-e7ba2c638ec2</value></entry><entry><key>botOwnerID</key><value>8f7ee860-0163-4229-9d2a-01cef53145ba</value></entry><entry><key>id</key><value>9c46aa04-b72e-423a-aab7-a1a05842d99b</value></entry><entry><key>type</key><value/></entry></metaData></transformers></xMessage>";
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
                kafkaProducer.send("com.odk.transformer", xmessage);
            }
            Date endDate = new Date();
            return new ResponseEntity<>("process complete start date : " + start + " end : " + endDate, HttpStatus.OK);
        } catch (Exception ex) {
            ex.printStackTrace();
            return new ResponseEntity<>(ex.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }
}
