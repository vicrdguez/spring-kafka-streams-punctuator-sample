package io.vrodriguez.delayedRecord;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

@Configuration
@EnableKafkaStreams
public class AppConfig {
    
    public AppConfig(){
    }

    public static final String METADATA_STORE = "metadata-store";
    public static final String RECORD_STORE = "record-store";
    public static final String TOPIC_INPUT = "input";
    public static final String TOPIC_OUTPUT = "output";

//    @Value("${bootstrap.servers}")
//    private String bootstrapServers;
//
//    @Value("${sasl.mechanism}")
//    private String saslMechanism;
//
//    @Value("${sasl.jaas.config}")
//    private String jaasConfig;
//
//    @Value("${security.protocol}")
//    private String securityProtocol;
//
//    @Value("${session.timeout.ms}")
//    private String sessionTimeout;
//
//    @Value("${basic.auth.credentials.source}")
//    public String srCredentialsSource;
//
//    @Value("${basic.auth.user.info}")
//    public String srAuthUserInfo;
//
//    @Value("${schema.registry.url}")
//    public String srUrl;



//    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
//    public KafkaStreamsConfiguration kStreamsConfigs() {
//        Map<String, Object> props = new HashMap<>();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "json-mapper");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put("sasl.mechanism", saslMechanism);
//        props.put("sasl.jaas.config", jaasConfig);
//        props.put("security.protocol", securityProtocol);
//
//        return new KafkaStreamsConfiguration(props);
//    }

    /**
     * tracks the streams app state and logs it in stdout
     */
    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return fb -> fb.setStateListener((newState, oldState) -> {
            System.out.println("State transition from " + oldState + " to " + newState);
       });
    }

//    public <T extends SpecificRecord> SpecificAvroSerde<T> getAvroSerde() {
//        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
//
//        Map<String,String> props = new HashMap<>();
//
//        props.put("basic.auth.credentials.source", srCredentialsSource);
//        props.put("basic.auth.user.info", srAuthUserInfo);
//        props.put("schema.registry.url", srUrl);
//
//        serde.configure(props, false);
//
//        return serde;
//    }
}

