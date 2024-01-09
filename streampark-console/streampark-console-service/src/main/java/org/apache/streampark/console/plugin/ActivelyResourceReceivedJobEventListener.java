package org.apache.streampark.console.plugin;

import org.apache.streampark.common.util.JsonUtils;
import org.apache.streampark.console.plugin.service.ApplicationOfJobService;

import com.vixtel.insight.core.dto.job.FlinkSQLJobDto;
import com.vixtel.insight.node.event.ActivelyResourceReceivedJobEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
public class ActivelyResourceReceivedJobEventListener
    implements ApplicationListener<ActivelyResourceReceivedJobEvent> {

  @Autowired ApplicationOfJobService applicationOfJobService;

  @Override
  public void onApplicationEvent(ActivelyResourceReceivedJobEvent event) {
    String data = event.getData();
    FlinkSQLJobDto flinkSQLJobDto = JsonUtils.read(data, FlinkSQLJobDto.class);

    applicationOfJobService.process(flinkSQLJobDto);
  }
}
