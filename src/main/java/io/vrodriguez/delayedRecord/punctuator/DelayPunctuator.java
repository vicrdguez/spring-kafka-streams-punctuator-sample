package io.vrodriguez.delayedRecord.punctuator;

import com.fasterxml.jackson.databind.JsonNode;
import io.vrodriguez.delayedRecord.AppConfig;
import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DelayPunctuator implements Punctuator {

  private static final Logger log = LoggerFactory.getLogger(DelayPunctuator.class);
  protected ProcessorContext<String, JsonNode> context;
  private Cancellable cancellationHandler;
  private long recordTs;

  private String recordKey;

  public DelayPunctuator(String recordKey) {
    this.recordKey = recordKey;
  }

  /// Method executed after each interval
  @Override
  public void punctuate(long timestamp) {
    PunctuatorMetadata metadata = getMetadataStore().get(recordKey);
    JsonNode payload = getRecordStore().get(recordKey);
    Duration elapsedTime = Duration.between(Instant.ofEpochMilli(metadata.scheduledTs()),
        Instant.now());

    Instant scheduledTime = Instant.ofEpochMilli(metadata.scheduledTs());
    log.warn("Scheduled record sent downstream after [{}]s, Scheduled time: {}, Record: {}", elapsedTime.toMillis()/1000, scheduledTime,
        payload);
    context.forward(
        new Record<String, JsonNode>(recordKey, payload, Instant.now().toEpochMilli()));
    // We don't need this particular punctuator or this payload anymore to trigger again after
    // sending the message down
    cancellationHandler.cancel();
  }

//  public void cancel() {
//    //cancel the puctuator
//    if (cancellationHandler != null) {
//      cancellationHandler.cancel();
//    }
//    // remove the message and metadata from stores
//    getMetadataStore().delete(recordKey);
//    getRecordStore().delete(recordKey);
//  }

  public Cancellable cancellationHandler() {
    return cancellationHandler;
  }

  public DelayPunctuator setCancellationHandler(
      Cancellable cancellationHandler) {
    this.cancellationHandler = cancellationHandler;
    return this;
  }

  public long recordTs() {
    return recordTs;
  }

  public DelayPunctuator setRecordTs(long recordTs) {
    this.recordTs = recordTs;
    return this;
  }

  public ProcessorContext<String, JsonNode> taskContext() {
    return context;
  }

  public DelayPunctuator setContext(
      ProcessorContext<String, JsonNode> context) {
    this.context = context;
    return this;
  }

  private KeyValueStore<String, PunctuatorMetadata> getMetadataStore() {
    return context.getStateStore(AppConfig.METADATA_STORE);
  }

  private KeyValueStore<String, JsonNode> getRecordStore() {
    return context.getStateStore(AppConfig.RECORD_STORE);
  }

  public String recordKey() {
    return recordKey;
  }

  public DelayPunctuator setRecordKey(String recordKey) {
    this.recordKey = recordKey;
    return this;
  }
}
