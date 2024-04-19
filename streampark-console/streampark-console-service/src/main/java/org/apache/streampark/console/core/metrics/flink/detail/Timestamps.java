package org.apache.streampark.console.core.metrics.flink.detail;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Timestamps {

  @JsonProperty("FINISHED")
  private long finish;

  @JsonProperty("FAILED")
  private long failed;

  @JsonProperty("RESTARTING")
  private long restarting;

  @JsonProperty("CREATED")
  private long create;

  @JsonProperty("FAILING")
  private long failing;

  @JsonProperty("RECONCILING")
  private long reconciling;

  @JsonProperty("CANCELLING")
  private long cancelling;

  @JsonProperty("SUSPENDED")
  private long suspended;

  @JsonProperty("INITIALIZING")
  private long initializing;

  @JsonProperty("CANCELED")
  private long canceled;

  @JsonProperty("RUNNING")
  private long running;
}
