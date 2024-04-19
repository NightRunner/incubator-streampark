package org.apache.streampark.console.core.metrics.flink.detail;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Metrics {
  @JsonProperty("read-bytes")
  private long readBytes;

  @JsonProperty("read-bytes-complete")
  private boolean readBytesComplete;

  @JsonProperty("write-bytes")
  private long writeBytes;

  @JsonProperty("write-bytes-complete")
  private boolean writeBytesComplete;

  @JsonProperty("read-records")
  private long readRecords;

  @JsonProperty("read-records-complete")
  private boolean readRecordsComplete;

  @JsonProperty("write-records")
  private long writeRecords;

  @JsonProperty("write-records-complete")
  private boolean writeRecordsComplete;

  @JsonProperty("accumulated-backpressured-time")
  private long accumulatedBackpressuredTime;

  @JsonProperty("accumulated-idle-time")
  private long accumulatedIdleTime;

  @JsonProperty("accumulated-busy-time")
  private String accumulatedBusyTime;
}
