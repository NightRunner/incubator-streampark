package org.apache.streampark.console.core.metrics.flink.detail;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Input {
  private int num;
  private String id;

  @JsonProperty("ship_strategy")
  private String shipStrategy;

  private String exchange;
}
