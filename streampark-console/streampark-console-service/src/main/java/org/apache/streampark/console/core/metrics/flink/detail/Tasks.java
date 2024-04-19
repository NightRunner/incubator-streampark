package org.apache.streampark.console.core.metrics.flink.detail;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Tasks {

  @JsonProperty("CANCELLING")
  private long cancelling;

  @JsonProperty("RECONCILING")
  private long reconciling;

  @JsonProperty("CANCELED")
  private long canceled;

  @JsonProperty("RUNNING")
  private long running;

  @JsonProperty("FAILED")
  private long failed;

  @JsonProperty("DEPLOYING")
  private long deploying;

  @JsonProperty("CREATED")
  private long created;

  @JsonProperty("SCHEDULED")
  private long scheduled;

  @JsonProperty("FINISHED")
  private long finished;

  @JsonProperty("INITIALIZING")
  private long initializing;
}
