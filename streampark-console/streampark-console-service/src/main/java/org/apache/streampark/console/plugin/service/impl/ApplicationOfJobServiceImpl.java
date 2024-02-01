package org.apache.streampark.console.plugin.service.impl;

import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.enums.ReleaseState;
import org.apache.streampark.console.core.service.AppBuildPipeService;
import org.apache.streampark.console.core.service.ApplicationLogService;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.FlinkEnvService;
import org.apache.streampark.console.core.service.FlinkSqlService;
import org.apache.streampark.console.plugin.entity.ApplicationOfJob;
import org.apache.streampark.console.plugin.mapper.ApplicationOfJobMapper;
import org.apache.streampark.console.plugin.service.ApplicationOfJobService;
import org.apache.streampark.flink.core.FlinkSqlValidationResult;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.vixtel.insight.core.domain.NameAndValue;
import com.vixtel.insight.core.domain.job.flink.FlinkSQLJob;
import com.vixtel.insight.core.domain.job.flink.SQLWrapper;
import com.vixtel.insight.core.dto.job.FlinkSQLJobDto;
import com.vixtel.insight.core.enums.ActivelyResourceJobPublishStatus;
import com.vixtel.insight.core.enums.FlinkJobPropertyType;
import com.vixtel.insight.node.log.MgmtJobHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@Slf4j
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class ApplicationOfJobServiceImpl
    extends ServiceImpl<ApplicationOfJobMapper, ApplicationOfJob>
    implements ApplicationOfJobService {

  public static final long VERSION_ID = 100000L;
  @Autowired ApplicationOfJobMapper applicationOfJobMapper;

  @Autowired ApplicationService applicationService;

  @Autowired FlinkSqlService flinkSqlService;

  @Value("${resource.originApplicationId}")
  private Long originApplicationId;

  @Override
  public void process(FlinkSQLJobDto flinkSQLJobDto) {
    String jobId = flinkSQLJobDto.getJobId();
    List<FlinkSQLJob> jobs = flinkSQLJobDto.getJobs();

    try {
      String status = flinkSQLJobDto.getStatus();
      ActivelyResourceJobPublishStatus activelyResourceJobPublishStatus =
          ActivelyResourceJobPublishStatus.getByName(status);

      // 删除整个JOB中涉及到的所有任务
      if (ActivelyResourceJobPublishStatus.DISABLED.equals(activelyResourceJobPublishStatus)) {
        List<ApplicationOfJob> applicationOfJobs = applicationOfJobMapper.getByJobId(jobId);

        for (ApplicationOfJob applicationOfJob : applicationOfJobs) {
          Application application = applicationService.getById(applicationOfJob.getAppId());

          stopIfRunning(application);

          applicationService.delete(application);
          applicationOfJobMapper.deleteById(applicationOfJob.getId());

          MgmtJobHelper.success(jobId, "删除name为[%s]的job成功");
        }
        return;
      }

      // 校验
      checkNotEmpty(jobs);

      // 校验SQL
      checkSql(jobs);

      // 盘点当前jobId已经存在的任务,并清除无效任务
      Map<String, Application> alreadyExistsApplicationNameMap = new HashMap<>();
      Map<Long, Application> alreadyExistsApplicationIdMap = new HashMap<>();
      List<ApplicationOfJob> applicationOfJobs = applicationOfJobMapper.getByJobId(jobId);
      if (!CollectionUtils.isEmpty(applicationOfJobs)) {
        for (ApplicationOfJob applicationOfJob : applicationOfJobs) {
          Application application = applicationService.getById(applicationOfJob.getAppId());

          if (application == null) {
            applicationOfJobMapper.deleteById(applicationOfJob.getId());
            continue;
          }

          alreadyExistsApplicationNameMap.put(application.getJobName(), application);
          alreadyExistsApplicationIdMap.put(application.getId(), application);
        }
      }

      // 更新或创建任务
      List<Application> needBuildAndRunApplications = new ArrayList<>();
      Set<Long> validApplicationIds = new HashSet<>();
      for (FlinkSQLJob job : jobs) {
        Application alreadyExistsApplication =
            alreadyExistsApplicationNameMap.get(job.getName() + "-" + jobId);
        String jobJson = JSONUtil.toJsonStr(job);
        if (alreadyExistsApplication == null) {
          // 创建application
          Application newApplication = new Application();
          newApplication.setJobName(job.getName() + "-" + jobId);
          newApplication.setId(originApplicationId);
          Long copiedApplicationId = applicationService.copy(newApplication);

          Application saved = applicationService.getById(copiedApplicationId);
          saved.setFlinkSql(buildSql(job.getSql()));
          fillOptionsByJob(saved, job);
          applicationService.update(saved);

          Application app = new Application();
          app.setId(copiedApplicationId);
          needBuildAndRunApplications.add(applicationService.getApp(app));

          // 创建关联关系
          ApplicationOfJob applicationOfJob = new ApplicationOfJob();
          applicationOfJob.setAppId(copiedApplicationId);
          applicationOfJob.setJobId(jobId);
          applicationOfJob.setContent(jobJson);
          applicationOfJobMapper.insert(applicationOfJob);
        } else {
          // 更新
          validApplicationIds.add(alreadyExistsApplication.getId());

          // 未发生变更,且job正在运行的.不需要变更
          FlinkAppState flinkAppState = FlinkAppState.of(alreadyExistsApplication.getState());
          ApplicationOfJob applicationOfJob =
              applicationOfJobMapper.getByAppId(alreadyExistsApplication.getId());
          if (jobJson.equals(applicationOfJob.getContent())
              && FlinkAppState.RUNNING.equals(flinkAppState)) {
            continue;
          }

          // 更新前停止运行
          stopIfRunning(alreadyExistsApplication);

          alreadyExistsApplication.setFlinkSql(buildSql(job.getSql()));
          fillOptionsByJob(alreadyExistsApplication, job);

          FlinkSql effective =
              flinkSqlService.getEffective(alreadyExistsApplication.getId(), false);
          if (effective == null) {
            if (appBuildPipeService.allowToBuildNow(alreadyExistsApplication.getId())) {
              this.buildUtilSuccessOrFail(alreadyExistsApplication.getId(), false);
            }
            effective = flinkSqlService.getEffective(alreadyExistsApplication.getId(), false);
          }
          alreadyExistsApplication.setSqlId(effective.getId());

          applicationService.update(alreadyExistsApplication);
          applicationOfJob.setContent(jobJson);
          applicationOfJobMapper.updateById(applicationOfJob);

          needBuildAndRunApplications.add(alreadyExistsApplication);
        }
      }

      // 删除本jobId下未提及的任务
      for (Map.Entry<Long, Application> entry : alreadyExistsApplicationIdMap.entrySet()) {
        Long key = entry.getKey();
        Application value = entry.getValue();
        if (!validApplicationIds.contains(key)) {

          Application application = applicationService.getById(value.getId());

          stopIfRunning(application);
          applicationService.delete(value);
        }
      }

      // 编译并启动本次更新的任务
      buildAndRun(jobId, needBuildAndRunApplications);

    } catch (Exception ex) {
      ex.printStackTrace();
      MgmtJobHelper.failed(jobId, "任务失败,详细信息:" + ex.getMessage());
    }
  }

  private static String buildSql(SQLWrapper sql) {
    return sql.getInput() + "\n" + sql.getOutput() + "\n" + sql.getCalc();
  }

  private static String buildSql(String sql) {
    return sql;
  }

  private void stopIfRunning(Application alreadyExistsApplication) throws Exception {
    FlinkAppState flinkAppState = FlinkAppState.of(alreadyExistsApplication.getState());

    if (FlinkAppState.RUNNING.equals(flinkAppState)) {
      cancelApplication(alreadyExistsApplication);
    }
  }

  private void buildAndRun(String jobId, List<Application> needBuildAndRunApplications) {
    for (Application application : needBuildAndRunApplications) {
      if (appBuildPipeService.allowToBuildNow(application.getId())) {
        this.buildUtilSuccessOrFail(application.getId(), false);
      }
      new Thread(
              () -> {
                while (true) {

                  try {
                    Thread.sleep(5000);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }

                  Application app = applicationService.getById(application.getId());

                  ReleaseState releaseState = app.getReleaseState();

                  if (ReleaseState.RELEASING.equals(releaseState)) {
                    // TODO
                  } else if (ReleaseState.DONE.equals(releaseState)) {
                    MgmtJobHelper.success(jobId, String.format("job[%s]发布完成", app.getJobName()));
                    try {
                      applicationService.start(app, false);
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                    break;
                  } else {
                    MgmtJobHelper.failed(jobId, String.format("job[%s]发布出错", app.getJobName()));
                    break;
                  }
                }
              })
          .start();
    }
  }

  private void cancelApplication(Application application) throws Exception {
    Application appParam = new Application();
    appParam.setId(application.getId());
    appParam.setDrain(false);
    appParam.setSavePointed(false);
    appParam.setTeamId(application.getTeamId());
    applicationService.cancel(appParam);
  }

  private void checkSql(List<FlinkSQLJob> jobs) {
    List<FlinkSQLJob> verifiedSuccessJobList = new ArrayList<>();
    StringBuilder buffer = new StringBuilder();
    for (FlinkSQLJob job : jobs) {
      FlinkSqlValidationResult flinkSqlValidationResult =
          flinkSqlService.verifySql(buildSql(job.getSql()), VERSION_ID);
      if (flinkSqlValidationResult.success()) {
        verifiedSuccessJobList.add(job);
      } else {
        buffer.append(
            String.format(
                "name为[%s]的flink sql未通过校验,错误日志:%s",
                job.getName(), flinkSqlValidationResult.exception()));
      }
    }
    if (verifiedSuccessJobList.size() < jobs.size()) {
      throw new RuntimeException(buffer.toString());
    }
  }

  private static void checkNotEmpty(List<FlinkSQLJob> jobs) {
    if (CollectionUtils.isEmpty(jobs)) {
      throw new RuntimeException("无有效job信息");
    }

    // sql非空校验
    StringBuilder stringBuilder = new StringBuilder();
    boolean passNotEmptySqlCheck = true;
    for (FlinkSQLJob job : jobs) {
      if (!StringUtils.hasText(buildSql(job.getSql()))) {
        passNotEmptySqlCheck = false;
        stringBuilder.append(String.format("name为[%s]的sql语句不能为空", job.getName()));
      }
    }
    if (!passNotEmptySqlCheck) {
      throw new RuntimeException(stringBuilder.toString());
    }
  }

  @Autowired FlinkEnvService flinkEnvService;

  @Autowired AppBuildPipeService appBuildPipeService;

  @Autowired ApplicationLogService applicationLogService;

  private boolean buildUtilSuccessOrFail(Long appId, boolean forceBuild) {
    Application app = applicationService.getById(appId);

    // 1) check flink version
    FlinkEnv env = flinkEnvService.getById(app.getVersionId());
    boolean checkVersion = env.getFlinkVersion().checkVersion(false);
    if (!checkVersion) {
      throw new ApiAlertException("Unsupported flink version: " + env.getFlinkVersion().version());
    }

    // 2) check env
    boolean envOk = applicationService.checkEnv(app);
    if (!envOk) {
      throw new ApiAlertException(
          "Check flink env failed, please check the flink version of this job");
    }

    if (!forceBuild && !appBuildPipeService.allowToBuildNow(appId)) {
      throw new ApiAlertException(
          "The job is invalid, or the job cannot be built while it is running");
    }
    // check if you need to go through the build process (if the jar and pom have changed,
    // you need to go through the build process, if other common parameters are modified,
    // you don't need to go through the build process)

    ApplicationLog applicationLog = new ApplicationLog();
    applicationLog.setOptionName(
        org.apache.streampark.console.core.enums.Operation.RELEASE.getValue());
    applicationLog.setAppId(app.getId());
    applicationLog.setOptionTime(new Date());

    boolean needBuild = applicationService.checkBuildAndUpdate(app);
    if (!needBuild) {
      applicationLog.setSuccess(true);
      applicationLogService.save(applicationLog);
    }

    // rollback
    if (app.isNeedRollback() && app.isFlinkSqlJob()) {
      flinkSqlService.rollback(app);
    }

    try {
      do {
        boolean success = appBuildPipeService.buildApplication(app, applicationLog);
        if (success) {
          return true;
        } else {
          System.out.println(app.getId() + "编译失败,重新编译");
        }
        try {
          Thread.sleep(20 * 1000L);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      } while (true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void fillOptionsByJob(Application application, FlinkSQLJob job) {
    if (application == null || job == null || CollectionUtils.isEmpty(job.getProperties())) {
      return;
    }

    JSONObject jsonObject = new JSONObject();
    for (NameAndValue property : job.getProperties()) {
      String name = property.getName();
      String value = property.getValue();
      if (FlinkJobPropertyType.PARALLELISM.getName().equals(name)) {
        jsonObject.set(FlinkJobPropertyType.PARALLELISM.getName(), value);
      } else if (FlinkJobPropertyType.TASK_SLOTS.getName().equals(name)) {
        jsonObject.set(FlinkJobPropertyType.TASK_SLOTS.getName(), value);
      } else if (FlinkJobPropertyType.TASK_MANAGER_MEMORY_SIZE.getName().equals(name)) {
        jsonObject.set(FlinkJobPropertyType.TASK_MANAGER_MEMORY_SIZE.getName(), value + "mb");
      } else if (FlinkJobPropertyType.JOB_MANAGER_MEMORY_SIZE.getName().equals(name)) {
        jsonObject.set(FlinkJobPropertyType.JOB_MANAGER_MEMORY_SIZE.getName(), value + "mb");
      }
    }

    application.setOptions(jsonObject.toString());
  }
}
