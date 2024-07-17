package org.apache.streampark.console.plugin;

import org.apache.streampark.common.enums.ExecutionMode;
import org.apache.streampark.common.util.HttpClientUtils;
import org.apache.streampark.common.util.ThreadUtils;
import org.apache.streampark.common.util.YarnUtils;
import org.apache.streampark.console.base.util.JacksonUtils;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.FlinkCluster;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.metrics.flink.JobsOverview;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.FlinkClusterService;
import org.apache.streampark.console.core.task.FlinkRESTAPIWatcher;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
public class FlinkLostAppRecoveryWatcher {

  @Autowired private ApplicationService applicationService;

  @Autowired private FlinkClusterService flinkClusterService;

  @Value("${resource.flinkLostAppRecovery:false}")
  private boolean flinkLostAppRecovery;

  private static final Map<Long, FlinkCluster> FLINK_CLUSTER_MAP = new ConcurrentHashMap<>(0);

  private static final ExecutorService EXECUTOR =
      new ThreadPoolExecutor(
          Runtime.getRuntime().availableProcessors() * 5,
          Runtime.getRuntime().availableProcessors() * 10,
          60L,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(1024),
          ThreadUtils.threadFactory("flink-lost-app-recovery-watcher"));

  @Scheduled(fixedDelay = 10 * 1000)
  public void start() {

    if (!flinkLostAppRecovery) {
      return;
    }

    List<Application> applications =
        applicationService.list(
            new LambdaQueryWrapper<Application>()
                .eq(Application::getState, FlinkAppState.LOST.getValue())
                .in(Application::getExecutionMode, ExecutionMode.REMOTE.getMode()));

    for (Application application : applications) {
      watch(application);
    }
  }

  private void watch(Application application) {
    EXECUTOR.execute(
        () -> {
          try {
            // query status from flink rest api
            getFromFlinkRestApi(application);
          } catch (Exception flinkException) {

          }
        });
  }

  private void getFromFlinkRestApi(Application application) throws Exception {
    JobsOverview jobsOverview = httpJobsOverview(application);
    Optional<JobsOverview.Job> optional;
    ExecutionMode execMode = application.getExecutionModeEnum();
    optional =
        jobsOverview.getJobs().stream()
            .filter(x -> x.getId().equals(application.getJobId()))
            .findFirst();
    if (optional.isPresent()) {
      FlinkRESTAPIWatcher.doWatching(application);
    }
  }

  private JobsOverview httpJobsOverview(Application application) throws Exception {
    final String flinkUrl = "jobs/overview";
    ExecutionMode execMode = application.getExecutionModeEnum();
    if (ExecutionMode.isYarnMode(execMode)) {
      String reqURL;
      if (StringUtils.isEmpty(application.getJobManagerUrl())) {
        String format = "proxy/%s/" + flinkUrl;
        reqURL = String.format(format, application.getAppId());
      } else {
        String format = "%s/" + flinkUrl;
        reqURL = String.format(format, application.getJobManagerUrl());
      }
      return yarnRestRequest(reqURL, JobsOverview.class);
    }

    if (application.getJobId() != null && ExecutionMode.isRemoteMode(execMode)) {
      return httpRemoteCluster(
          application.getFlinkClusterId(),
          cluster -> {
            String remoteUrl = cluster.getAddress() + "/" + flinkUrl;
            JobsOverview jobsOverview = httpRestRequest(remoteUrl, JobsOverview.class);
            if (jobsOverview != null) {
              List<JobsOverview.Job> jobs =
                  jobsOverview.getJobs().stream()
                      .filter(x -> x.getId().equals(application.getJobId()))
                      .collect(Collectors.toList());
              jobsOverview.setJobs(jobs);
            }
            return jobsOverview;
          });
    }
    return null;
  }

  private <T> T yarnRestRequest(String url, Class<T> clazz) throws IOException {
    String result = YarnUtils.restRequest(url);
    return JacksonUtils.read(result, clazz);
  }

  private <T> T httpRestRequest(String url, Class<T> clazz) throws IOException {
    String result =
        HttpClientUtils.httpGetRequest(url, RequestConfig.custom().setConnectTimeout(5000).build());
    if (null == result) {
      return null;
    }
    return JacksonUtils.read(result, clazz);
  }

  private <T> T httpRemoteCluster(Long clusterId, Callback<FlinkCluster, T> function)
      throws Exception {
    FlinkCluster flinkCluster = getFlinkRemoteCluster(clusterId, false);
    try {
      return function.call(flinkCluster);
    } catch (Exception e) {
      flinkCluster = getFlinkRemoteCluster(clusterId, true);
      return function.call(flinkCluster);
    }
  }

  private FlinkCluster getFlinkRemoteCluster(Long clusterId, boolean flush) {
    FlinkCluster flinkCluster = FLINK_CLUSTER_MAP.get(clusterId);
    if (flinkCluster == null || flush) {
      flinkCluster = flinkClusterService.getById(clusterId);
      FLINK_CLUSTER_MAP.put(clusterId, flinkCluster);
    }
    return flinkCluster;
  }

  interface Callback<T, R> {
    R call(T e) throws Exception;
  }
}
