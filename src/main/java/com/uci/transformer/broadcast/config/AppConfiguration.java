package com.uci.transformer.broadcast.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.uci.utils.CampaignService;
import com.uci.utils.kafka.ReactiveProducer;
import io.fusionauth.client.FusionAuthClient;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Configuration
@EnableAutoConfiguration
@Slf4j
public class AppConfiguration {
    @Bean
    @Qualifier("rest")
    public RestTemplate getRestTemplate() {
        return new RestTemplate();
    }

    @Value("${fusionauth.url}")
    public String FUSIONAUTH_URL;

    @Value("${fusionauth.key}")
    public String FUSIONAUTH_KEY;
    
    @Autowired
    public Cache<Object, Object> cache;

    @Value("${spring.kafka.bootstrap-servers}")
    private String BOOTSTRAP_SERVERS;

    private final String GROUP_ID = "broadcast";

    @Bean
    public FusionAuthClient getFAClient() {
        return new FusionAuthClient(FUSIONAUTH_KEY, FUSIONAUTH_URL);
    }

    @Bean
    Map<String, Object> kafkaConsumerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return configuration;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(kafkaConsumerConfiguration(), new StringDeserializer(),
                new StringDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    kafkaListenerContainerFactory()  {
        ConcurrentKafkaListenerContainerFactory<String, String> factory
                = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setErrorHandler(((exception, data) -> {
            /*
             * here you can do you custom handling, I am just logging it same as default
             * Error handler does If you just want to log. you need not configure the error
             * handler here. The default handler does it for you. Generally, you will
             * persist the failed records to DB for tracking the failed records.
             */
            log.error("Error in process with Exception {} and the record is {}", exception, data);
        }));


        return factory;
    }

    @Bean
    ReceiverOptions<String, String> kafkaReceiverOptions(@Value("${broadcast-transformer}") String[] inTopicName) {
//    	System.out.println("inTopicName:"+inTopicName.toString());
//        System.out.println("BOOTSTRAP_SERVERS: "+BOOTSTRAP_SERVERS);
//        for (int i=0;i<inTopicName.length;i++) {
//        	System.out.println("Name: "+inTopicName[i]);
//        }
    	ReceiverOptions<String, String> options = ReceiverOptions.create(kafkaConsumerConfiguration());
        return options.subscription(Arrays.asList(inTopicName))
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new StringDeserializer());
    }

    @Bean
    Flux<ReceiverRecord<String, String>> reactiveKafkaReceiver(ReceiverOptions<String, String> kafkaReceiverOptions) {
        return KafkaReceiver.create(kafkaReceiverOptions).receive();
    }

    @Bean
    Map<String, Object> kafkaProducerConfiguration() {
        Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(org.springframework.kafka.support.serializer.JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return configuration;
    }

    @Bean
    SenderOptions<Integer, String> kafkaSenderOptions() {
        return SenderOptions.create(kafkaProducerConfiguration());
    }

    @Bean
    ProducerFactory<String, String> producerFactory(){
    	ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(kafkaProducerConfiguration());
    	return producerFactory;
    }
    
    @Bean
    KafkaTemplate<String, String> kafkaTemplate() {
    	KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
    	return kafkaTemplate;
    }
}
