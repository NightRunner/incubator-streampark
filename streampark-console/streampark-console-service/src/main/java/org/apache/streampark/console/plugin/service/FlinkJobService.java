package org.apache.streampark.console.plugin.service;

import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.common.util.YarnUtils;
import org.apache.streampark.console.base.util.JacksonUtils;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.metrics.flink.detail.IdAndValue;
import org.apache.streampark.console.core.metrics.flink.detail.JobDetails;
import org.apache.streampark.console.core.metrics.flink.detail.Metrics;
import org.apache.streampark.console.core.metrics.flink.detail.Node;
import org.apache.streampark.console.core.metrics.flink.detail.Plan;
import org.apache.streampark.console.core.metrics.flink.detail.Vertex;
import org.apache.streampark.console.plugin.entity.ApplicationOfJob;
import org.apache.streampark.console.plugin.mapper.ApplicationOfJobMapper;

import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.vixtel.insight.node.config.ResourceCommonProperties;
import com.vixtel.insight.node.log.MgmtJobHelper;
import com.vixtel.insight.node.log.content.FlinkRunningLog;
import com.vixtel.insight.node.log.content.RunningLog;
import com.vixtel.insight.node.log.content.RunningLogContent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class FlinkJobService {

  @Autowired ResourceCommonProperties resourceCommonProperties;

  private Map<Long, Integer> countMap = new HashMap<>();

  private final Integer queryFlinkJobStatusInterval = 60;

  private static String getShortName(String jobName) {
    return jobName.substring(0, jobName.lastIndexOf("-"));
  }

  public void process(Application application) {
    if (!resourceCommonProperties.isEnable()) {
      return;
    }

    Long applicationId = application.getId();

    if (!countMap.containsKey(applicationId)) {
      countMap.put(applicationId, 0);
    }
    Integer currentCount = countMap.get(applicationId);
    if (currentCount % queryFlinkJobStatusInterval > 0) {
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

        long readBytes = 0L;
        long readRecords = 0L;
        long writeBytes = 0L;
        long writeRecords = 0L;
        StringBuilder windowSituation = new StringBuilder();

        try {
          JobDetails jobDetails = httpJobDetail(application);
          if (isHaveWatermarks(jobDetails)) {
            List<String> watermarkVertexList = getWatermarkVertextList(jobDetails);
            for (String watermarkVertex : watermarkVertexList) {
              IdAndValue[] idAndValues = httpJobWatermarks(application, watermarkVertex);
              if (idAndValues != null) {
                for (IdAndValue idAndValue : idAndValues) {
                  Date date = new Date();
                  String value = idAndValue.getValue();
                  long time = Long.parseLong(value);
                  if (time < 0) {
                    continue;
                  }
                  date.setTime(time);
                  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                  windowSituation.append(sdf.format(date));
                  windowSituation.append(" ");
                }
              }
            }
          }

          // 获取起始节点
          String startNodeId = getStartNodeId(jobDetails);

          if (!StringUtils.isEmpty(startNodeId)) {
            Vertex vertex = getStartNodeVertex(jobDetails, startNodeId);
            if (vertex != null) {
              Metrics metrics = vertex.getMetrics();
              if (metrics != null) {
                readRecords = metrics.getReadRecords();
                readBytes = metrics.getReadBytes();

                writeRecords = metrics.getWriteRecords();
                writeBytes = metrics.getWriteBytes();
              }
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        RunningLogContent runningLogContent = new RunningLogContent();
        ArrayList<RunningLog> details = new ArrayList<>();
        FlinkRunningLog runningLog = new FlinkRunningLog();
        runningLog.setName(getShortName(application.getJobName()));
        runningLog.setTotalRecordProcesses(writeRecords);
        runningLog.setFailedProcesses(0L);
        runningLog.setWatermark(windowSituation.toString());
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

  private Vertex getStartNodeVertex(JobDetails jobDetails, String startNodeId) {
    if (jobDetails == null || StringUtils.isEmpty(startNodeId)) {
      return null;
    }

    List<Vertex> vertices = jobDetails.getVertices();
    if (CollectionUtils.isEmpty(vertices)) {
      return null;
    }

    for (Vertex vertex : vertices) {
      if (vertex.getId().equals(startNodeId)) {
        return vertex;
      }
    }

    return null;
  }

  private String getStartNodeId(JobDetails jobDetails) {
    if (jobDetails == null) {
      return null;
    }
    Plan plan = jobDetails.getPlan();
    List<Node> nodes = plan.getNodes();
    if (CollectionUtils.isEmpty(nodes)) {
      return null;
    }
    for (Node node : nodes) {
      if (CollectionUtils.isEmpty(node.getInputs())) {
        return node.getId();
      }
    }

    return null;
  }

  @Autowired ApplicationOfJobMapper applicationOfJobMapper;

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

  private JobDetails httpJobDetail(Application application) throws Exception {
    ExecutionMode execMode = application.getExecutionModeEnum();
    if (ExecutionMode.isYarnMode(execMode)) {
      String reqURL;
      if (StringUtils.isEmpty(application.getJobManagerUrl())) {
        reqURL =
            String.format(
                "%s/proxy/%s/jobs/%s",
                YarnUtils.getRMWebAppProxyURL(), application.getAppId(), application.getJobId());
      } else {
        reqURL =
            String.format("%s/jobs/%s", application.getJobManagerUrl(), application.getJobId());
      }
      return yarnRestRequest(reqURL, JobDetails.class);
    }

    return null;
  }

  private IdAndValue[] httpJobWatermarks(Application application, String vertexId)
      throws Exception {
    ExecutionMode execMode = application.getExecutionModeEnum();
    if (ExecutionMode.isYarnMode(execMode)) {
      String reqURL;
      if (StringUtils.isEmpty(application.getJobManagerUrl())) {
        reqURL =
            String.format(
                "%s/proxy/%s/jobs/%s/vertices/%s/watermarks",
                YarnUtils.getRMWebAppProxyURL(),
                application.getAppId(),
                application.getJobId(),
                vertexId);
      } else {
        reqURL =
            String.format(
                "%s/jobs/%s/vertices/%s/watermarks",
                application.getJobManagerUrl(), application.getJobId(), vertexId);
      }
      return yarnRestRequest(reqURL, IdAndValue[].class);
    }

    return null;
  }

  public static void main(String[] args) throws IOException {
    JobDetails jobDetails =
        yarnRestRequest(
            "http://node10:8083/proxy/application_1712469332797_0097/jobs/5cbac8efd09729cdf5d95cf558aae8bb",
            JobDetails.class);
    //    IdAndValue[] idAndValues =
    //        yarnRestRequest(
    //
    // "http://node10:8083/proxy/application_1708658153067_0014/jobs/248792f08fb2ac3717c32011f0026db9/vertices/90bea66de1c231edf33913ecd54406c1/watermarks",
    //            IdAndValue[].class);
    //      System.out.println(idAndValues);

    System.out.println(jobDetails);
  }

  private static void specialLog(String content) {
    log.error("specialLog:::::" + (content.length() > 120 ? content.substring(0, 120) : content));
  }

  private static <T> T yarnRestRequest(String url, Class<T> clazz) throws IOException {

    specialLog("发起URL请求:" + url);
    String result = YarnUtils.restRequest(url);
    specialLog("返回结果:" + result);

    return JacksonUtils.read(result, clazz);
  }

  private List<String> getWatermarkVertextList(JobDetails jobDetails) {
    if (jobDetails == null) {
      return new ArrayList<>(0);
    }

    List<String> list = new ArrayList<>();
    for (Vertex vertex : jobDetails.getVertices()) {
      if (isHaveWatermark(vertex)) {
        list.add(vertex.getId());
      }
    }

    return list;
  }

  private static boolean isHaveWatermark(Vertex vertex) {
    return vertex.getName().toLowerCase().contains("windowaggregate");
  }

  private boolean isHaveWatermarks(JobDetails jobDetails) {
    if (jobDetails == null) {
      return false;
    }

    for (Vertex vertex : jobDetails.getVertices()) {
      if (isHaveWatermark(vertex)) {
        return true;
      }
    }

    return false;
  }
}
