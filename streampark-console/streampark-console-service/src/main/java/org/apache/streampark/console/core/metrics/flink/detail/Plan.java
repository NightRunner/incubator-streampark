package org.apache.streampark.console.core.metrics.flink.detail;

import lombok.Data;

import java.util.List;

@Data
public class Plan {
  private String jid;
  private String name;
  private String type;
  private List<Node> nodes;
}
