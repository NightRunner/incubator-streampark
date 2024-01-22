package org.apache.streampark.console.plugin.event;

import org.springframework.context.ApplicationEvent;

import java.time.Clock;

public class ApplicationChangeEvent extends ApplicationEvent {
  public ApplicationChangeEvent(Object source) {
    super(source);
  }

  public ApplicationChangeEvent(Object source, Clock clock) {
    super(source, clock);
  }
}
