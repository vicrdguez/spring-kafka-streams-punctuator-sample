package io.vrodriguez.delayedRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vrodriguez.delayedRecord.punctuator.PunctuatorMetadata;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
public class DelayedRecordApplication {
  private static final Logger log = LoggerFactory.getLogger(DelayedRecordApplication.class);
  public static void main(String[] args) {
    SpringApplication.run(DelayedRecordApplication.class, args);
  }

  @Autowired
  AppConfig appConfig;

  @Bean
  public KStream<String, JsonNode> delayedMessage(StreamsBuilder streamsBuilder) {
    Serde<JsonNode> jsonSerde = new Serdes.WrapperSerde<JsonNode>(new JsonSerializer<>(),
        new JsonDeserializer<>(JsonNode.class));
    Serde<PunctuatorMetadata> metadataSerde = new JsonSerde<>(PunctuatorMetadata.class);
    Serde<String> stringSerde = Serdes.String();

    streamsBuilder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(AppConfig.RECORD_STORE),
            Serdes.String(),
            jsonSerde
        ));

    streamsBuilder.addStateStore(
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(AppConfig.METADATA_STORE),
            Serdes.String(),
            metadataSerde
        ));

    KStream<String, JsonNode> inputStream = streamsBuilder.stream(
        AppConfig.TOPIC_INPUT,
        Consumed.with(stringSerde, jsonSerde).withName("json-reader")
    );
//    inputStream.peek((k, v) -> log.warn("record received: {}" , v));

    KStream<String, JsonNode> outputStream = inputStream
        .process(
            () -> new DelayedProcessor(Duration.ofMinutes(1)),
            Named.as("delay-processor"),
            AppConfig.METADATA_STORE,
            AppConfig.RECORD_STORE
        );

    outputStream.to(AppConfig.TOPIC_OUTPUT, Produced.with(stringSerde, jsonSerde));
    return outputStream;
  }
}
