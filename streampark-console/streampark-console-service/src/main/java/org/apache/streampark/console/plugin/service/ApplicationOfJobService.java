package org.apache.streampark.console.plugin.service;

import org.apache.streampark.console.plugin.entity.ApplicationOfJob;

import com.baomidou.mybatisplus.extension.service.IService;
import com.vixtel.insight.core.dto.job.FlinkSQLJobDto;

public interface ApplicationOfJobService extends IService<ApplicationOfJob> {
  void process(FlinkSQLJobDto flinkSQLJobDto);
}
