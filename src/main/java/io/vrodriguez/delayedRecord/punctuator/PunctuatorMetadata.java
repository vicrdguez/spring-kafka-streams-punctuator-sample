package io.vrodriguez.delayedRecord.punctuator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
@JsonDeserialize
public class PunctuatorMetadata {

  @JsonProperty
  public String recordKey;
  @JsonProperty
  private long interval;
  @JsonProperty
  private long scheduledTs;
  @JsonProperty
  private long recordTs;
  public String recordKey() {
    return recordKey;
  }

  public PunctuatorMetadata setRecordKey(String recordKey) {
    this.recordKey = recordKey;
    return this;
  }

  public long interval() {
    return interval;
  }

  public PunctuatorMetadata setInterval(long interval) {
    this.interval = interval;
    return this;
  }

  public long scheduledTs() {
    return scheduledTs;
  }

  public PunctuatorMetadata setScheduledTs(long scheduledTs) {
    this.scheduledTs = scheduledTs;
    return this;
  }

  public long recordTs() {
    return recordTs;
  }

  public PunctuatorMetadata setRecordTs(long recordTs) {
    this.recordTs = recordTs;
    return this;
  }

}
