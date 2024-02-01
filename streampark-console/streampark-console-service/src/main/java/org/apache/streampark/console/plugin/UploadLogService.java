package org.apache.streampark.console.plugin;

import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.plugin.entity.ApplicationOfJob;
import org.apache.streampark.console.plugin.mapper.ApplicationOfJobMapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.vixtel.insight.node.log.MgmtJobHelper;
import com.vixtel.insight.node.log.content.RunningLog;
import com.vixtel.insight.node.log.content.RunningLogContent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Component
public class UploadLogService {

  @Autowired ApplicationOfJobMapper applicationOfJobMapper;

  private Map<Long, Integer> countMap = new HashMap<>();

  public void process(Application application) {
    Long applicationId = application.getId();

    if (!countMap.containsKey(applicationId)) {
      countMap.put(applicationId, 0);
    }
    Integer currentCount = countMap.get(applicationId);
    if (currentCount % 5 > 0) {
      countMap.put(applicationId, currentCount + 1);
      return;
    }

    countMap.put(applicationId, currentCount + 1);
    try {
      String jobId = getJobId(application);

      FlinkAppState flinkAppState = FlinkAppState.of(application.getState());
      String content = null;
      boolean isRunning = true;
      switch (flinkAppState) {
        case ADDED:
          content = "添加成功";
          break;
        case INITIALIZING:
          content = "初始化";
          break;
        case CREATED:
          content = "创建完成";
          break;
        case STARTING:
          content = "启动中";
          break;
        case RESTARTING:
          content = "重启中";
          break;
        case RUNNING:
          content = "运行中";
          break;
        case FAILING:
          content = "失败 ";
          isRunning = false;
          break;
        case FAILED:
          content = "失败";
          isRunning = false;
          break;
        case CANCELLING:
          content = "取消中";
          break;
        case CANCELED:
          content = "取消";
          isRunning = false;
          break;
        case FINISHED:
          content = "完成";
          break;
        case SUSPENDED:
          content = "暂停";
          isRunning = false;
          break;
        case RECONCILING:
          content = "调整中";
          isRunning = false;
          break;
        case LOST:
          content = "丢失";
          isRunning = false;
          break;
        case MAPPING:
          content = "映射中";
          isRunning = false;
          break;
        case OTHER:
          content = "其他";
          break;
        case REVOKED:
          content = "取消";
          break;
        case SILENT:
          content = "无反应";
          break;
        case TERMINATED:
          content = "终止";
          isRunning = false;
          break;
        case POS_TERMINATED:
          content = "终止 ";
          isRunning = false;
          break;
        case SUCCEEDED:
          content = "成功 ";
          break;
        case KILLED:
          content = "被kill";
          isRunning = false;
          break;
        default:
          isRunning = false;
          content = "未知";
      }
      if (isRunning) {
        RunningLogContent runningLogContent = new RunningLogContent();
        ArrayList<RunningLog> details = new ArrayList<>();
        RunningLog runningLog = new RunningLog();
        runningLog.setName(getShortName(application.getJobName()));
        runningLog.setSuccessfulProcesses(1L);
        runningLog.setTotalRequests(100L);
        runningLog.setTotalRequestsUnit("1min");
        details.add(runningLog);
        runningLogContent.setDetails(details);
        MgmtJobHelper.running(jobId, runningLogContent);
      } else {
        MgmtJobHelper.failed(jobId, content);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private static String getShortName(String jobName) {
    return jobName.substring(0, jobName.lastIndexOf("-"));
  }

  private final Map<Long, String> map = new HashMap<>();

  private String getJobId(Application application) {
    if (map.containsKey(application.getId())) {
      return map.get(application.getId());
    } else {
      ApplicationOfJob applicationOfJob =
          applicationOfJobMapper.selectOne(
              new LambdaQueryWrapper<ApplicationOfJob>()
                  .eq(ApplicationOfJob::getAppId, application.getId()));
      if (applicationOfJob != null) {
        map.put(application.getId(), applicationOfJob.getJobId());
      } else {
        return null;
      }
    }
    return map.get(application.getId());
  }
}
