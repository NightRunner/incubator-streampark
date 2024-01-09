package org.apache.streampark.console.plugin;

import org.apache.streampark.common.util.JsonUtils;
import org.apache.streampark.console.plugin.service.ApplicationOfJobService;

import com.vixtel.insight.core.dto.job.FlinkSQLJobDto;
import com.vixtel.insight.node.event.ActivelyResourceReceivedAllDispatchedJobEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
public class ActivelyResourceReceivedAllJobEventListener
    implements ApplicationListener<ActivelyResourceReceivedAllDispatchedJobEvent> {

  @Autowired ApplicationOfJobService applicationOfJobService;

  @Override
  public void onApplicationEvent(ActivelyResourceReceivedAllDispatchedJobEvent event) {
    String data = event.getData();
    FlinkSQLJobDto[] flinkSQLJobDtoArray = JsonUtils.read(data, FlinkSQLJobDto[].class);

    for (FlinkSQLJobDto flinkSQLJobDto : flinkSQLJobDtoArray) {
      applicationOfJobService.process(flinkSQLJobDto);
    }
  }
}
