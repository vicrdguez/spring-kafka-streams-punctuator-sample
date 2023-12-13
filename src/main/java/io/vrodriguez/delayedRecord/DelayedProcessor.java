package io.vrodriguez.delayedRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vrodriguez.delayedRecord.punctuator.DelayPunctuator;
import io.vrodriguez.delayedRecord.punctuator.PunctuatorMetadata;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DelayedProcessor implements Processor<String, JsonNode, String, JsonNode> {

  private static final Logger log = LoggerFactory.getLogger(DelayedProcessor.class);
  private ProcessorContext<String, JsonNode> localContext;
  private KeyValueStore<String, JsonNode> recordStore;
  private KeyValueStore<String, PunctuatorMetadata> metadataStore;
  private final HashMap<String, DelayPunctuator> inMemoryMetadataStore;
  private final Duration waitTime;

  public DelayedProcessor(Duration waitTime) {
    this.waitTime = waitTime;
    inMemoryMetadataStore = new HashMap<>();
  }

  @Override
  public void init(ProcessorContext<String, JsonNode> context) {
    localContext = context;
    recordStore = localContext.getStateStore(AppConfig.RECORD_STORE);
    metadataStore = localContext.getStateStore(AppConfig.METADATA_STORE);

  }

  @Override
  public void process(Record<String, JsonNode> record) {
    // get a unique key for the record, each operation needs to be performed uniquely for each
    String hashKey = String.valueOf(record.value().hashCode());
    // Set the "second" property as "Y"
    ObjectNode payload = (ObjectNode) record.value();
    payload.withObject("/inputs").put("second", "Y");
    // store the record into a kafka state store, so we can send it a second time after the wait time
    recordStore.put(hashKey, payload);
    // send the record the first time
    localContext.forward(record);
    // schedule the "task" that will send the record the second time. This is based on the record
    // creation time and not the current time
    schedulePunctuator(hashKey, record.timestamp());
    // resume the "tasks" that were present before a restart (if that is the case
    resumeScheduledPunctuators();
  }

  private void schedulePunctuator(String key, long recordTs) {
    DelayPunctuator punctuator = new DelayPunctuator(key);
    Duration scheduleInterval = getInterval(recordTs);
    Duration recordDelay = waitTime.minus(scheduleInterval);
    Cancellable punctuatorHandler = localContext.schedule(
        scheduleInterval,
        // Important so the punctuator triggers based on "real time" and not stream time (time does not
        // move forward until more messages arrive.
        PunctuationType.WALL_CLOCK_TIME,
        punctuator
    );
    log.warn("Record with key [{}] => has a delay of [{}]s", punctuator.recordKey(),
        recordDelay.toMillis() / 1000);
    log.warn("Record with key [{}] => scheduled to be sent in [{}]s", punctuator.recordKey(),
        scheduleInterval.toMillis() / 1000);

    punctuator.setCancellationHandler(cancelCallback(punctuatorHandler, key))
        .setContext(localContext)
        .setRecordTs(recordTs);
    saveMetadata(punctuator);
  }

  private Cancellable cancelCallback(Cancellable punctuatorHandler, String recordKey) {
    return () -> {
      punctuatorHandler.cancel();
      inMemoryMetadataStore.remove(recordKey);
      // remove the message and metadata from stores
      metadataStore.delete(recordKey);
      recordStore.delete(recordKey);
    };
  }

  /**
   * Stores the punctuator in memory and persist its metadata into a kafka state store. In case of a
   * restart the data on the state store will be recovered using the @{link
   * #resumeScheduledPunctuators} method
   *
   * @param punctuator
   * @param scheduleInterval
   */
  private void saveMetadata(DelayPunctuator punctuator) {
    PunctuatorMetadata metadata = new PunctuatorMetadata()
        .setRecordKey(punctuator.recordKey())
        .setInterval(waitTime.toMillis())
        .setScheduledTs(Instant.now().toEpochMilli())
        .setRecordTs(punctuator.recordTs());
    // we store the in-flight punctuators in memory to keep track of them
    inMemoryMetadataStore.put(punctuator.recordKey(), punctuator);
    // we save the punctuator metadata in a state store for persistence and recovery in case the application
    // fails or restarts (or both).
    // Under a restart, all the "timers" that were in-flight will be recovered from kafka using this
    // store see the
    metadataStore.put(punctuator.recordKey(), metadata);
  }

  private void resumeScheduledPunctuators() {
    // avoid querying the state store if the inMemory store is not empty, that means both the State
    // store and the inMemory store are reflecting the same information
//    if (!inMemoryMetadataStore.isEmpty()) {
//      return;
//    }
    KeyValueIterator<String, PunctuatorMetadata> metadataStoreIterator = metadataStore.all();
    while (metadataStoreIterator.hasNext()) {
      KeyValue<String, PunctuatorMetadata> metadataRecord = metadataStoreIterator.next();
      PunctuatorMetadata metadata = metadataRecord.value;
      // Only recover the punctuators that are not in the current inMemory store
      // We need to do this check because Kafka Streams will execute this method with every incoming
      // record. In practice, this should only be executed after a restart, when the inMemory store is
      // empty but there were in-flight punctuators before restarting
      if (!inMemoryMetadataStore.containsKey(metadata.recordKey())) {
        log.warn("Resuming punctuator with key [{}}", metadata.recordKey());
        schedulePunctuator(metadata.recordKey(), metadata.recordTs());
      }
    }
    metadataStoreIterator.close();
  }

  private Duration getInterval(long recordTs) {
    Duration recordElapsedTime = Duration.between(Instant.ofEpochMilli(recordTs), Instant.now());
    log.warn("record ts: {}", recordTs);
    int timeCompare = recordElapsedTime.compareTo(this.waitTime);
    // The record was created longer ago than the waitTime value, meaning that we should not wait
    // any longer, the interval is zero
    if (timeCompare > 0) {
      return Duration.ofMillis(1); // minimum time to schedule a punctuator
    }
    // The record creation time is still smaller than the waitTime, return an interval of the time
    // left to reach the wait time value
    return this.waitTime.minus(recordElapsedTime);
  }

}