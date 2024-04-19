package org.apache.streampark.console.core.metrics.flink.detail;

import lombok.Data;

import java.util.List;

@Data
public class JobDetails {
  private String jid;
  private String name;
  private boolean isStoppable;
  private String state;
  private long startTime;
  private long endTime;
  private long duration;
  private int maxParallelism;
  private long now;
  private Timestamps timestamps;
  private List<Vertex> vertices;
  private Plan plan;
}
