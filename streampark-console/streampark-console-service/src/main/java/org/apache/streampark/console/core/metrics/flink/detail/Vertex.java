package org.apache.streampark.console.core.metrics.flink.detail;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Vertex {
  private String id;
  private String name;
  private int maxParallelism;
  private int parallelism;
  private String status;

  @JsonProperty("start-time")
  private long startTime;

  @JsonProperty("end-time")
  private long endTime;

  private long duration;
  private Tasks tasks;
  private Metrics metrics;
}
