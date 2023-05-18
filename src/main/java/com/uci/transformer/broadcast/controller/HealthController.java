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
}
