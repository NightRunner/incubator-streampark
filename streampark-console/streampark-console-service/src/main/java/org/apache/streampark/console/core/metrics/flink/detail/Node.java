package org.apache.streampark.console.core.metrics.flink.detail;

import lombok.Data;

import java.util.List;

@Data
public class Node {
  private String id;
  private int parallelism;
  private String operator;
  private String operatorStrategy;
  private String description;
  private List<Input> inputs;
}
