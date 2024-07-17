package org.apache.streampark.console.plugin.service.impl;

import org.apache.streampark.common.util.DeflaterUtils;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.ApplicationConfig;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.enums.ReleaseState;
import org.apache.streampark.console.core.service.AppBuildPipeService;
import org.apache.streampark.console.core.service.ApplicationConfigService;
import org.apache.streampark.console.core.service.ApplicationLogService;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.FlinkEnvService;
import org.apache.streampark.console.core.service.FlinkSqlService;
import org.apache.streampark.console.plugin.entity.ApplicationOfJob;
import org.apache.streampark.console.plugin.mapper.ApplicationOfJobMapper;
import org.apache.streampark.console.plugin.service.ApplicationOfJobService;
import org.apache.streampark.flink.core.FlinkSqlValidationResult;

import org.apache.commons.lang3.builder.ToStringBuilder;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class ApplicationOfJobServiceImpl
    extends ServiceImpl<ApplicationOfJobMapper, ApplicationOfJob>
    implements ApplicationOfJobService {

  private static final Logger log = LoggerFactory.getLogger(ApplicationOfJobServiceImpl.class);

  public static final long VERSION_ID = 100000L;

  @Autowired ApplicationOfJobMapper applicationOfJobMapper;

  @Autowired ApplicationService applicationService;

  @Autowired FlinkSqlService flinkSqlService;

  @Value("${resource.originApplicationId}")
  private Long originApplicationId;

  @Autowired ApplicationConfigService configService;

  private void specialLog(String content) {
    log.error("specialLog:::::" + (content.length() > 120 ? content.substring(0, 120) : content));
  }

  @Override
  public void process(FlinkSQLJobDto flinkSQLJobDto) {
    String jobId = flinkSQLJobDto.getJobId();
    List<FlinkSQLJob> jobs = flinkSQLJobDto.getJobs();

    specialLog("开始处理:" + JSONUtil.toJsonStr(flinkSQLJobDto));

    try {
      String status = flinkSQLJobDto.getStatus();
      ActivelyResourceJobPublishStatus activelyResourceJobPublishStatus =
          ActivelyResourceJobPublishStatus.getByName(status);

      if (ActivelyResourceJobPublishStatus.DISABLED.equals(activelyResourceJobPublishStatus)) {
        specialLog("状态为disabled,删除JOB相关的JOB.");
        // 删除整个JOB中涉及到的所有任务
        deleteAllApplicationByJobId(jobId);
        return;
      }

      // 校验SQL
      specialLog("开始校验sql");
      checkSql(jobs);
      specialLog("校验sql完成");

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

      specialLog("开始更新或创建任务");

      // 更新或创建任务
      List<Application> needBuildAndRunApplications = new ArrayList<>();
      Set<Long> validApplicationIds = new HashSet<>();
      for (FlinkSQLJob job : jobs) {
        Application currentApplication = null;
        Application alreadyExistsApplication =
            alreadyExistsApplicationNameMap.get(job.getName() + "-" + jobId);
        String jobJson = JSONUtil.toJsonStr(job);
        if (alreadyExistsApplication == null) {

          specialLog("开始创建:" + job.getName());

          // 创建application
          Application newApplication = new Application();
          newApplication.setJobName(job.getName() + "-" + jobId);
          newApplication.setId(originApplicationId);
          Long copiedApplicationId = applicationService.copy(newApplication);

          Application saved = applicationService.getById(copiedApplicationId);
          saved.setFlinkSql(buildSql(job.getSql()));
          fillOptionsByJob(saved, job);

          ApplicationConfig config = configService.getEffective(saved.getId());
          config = config == null ? configService.getLatest(saved.getId()) : config;

          String updatedFlinkConfigString =
              getUpdatedFlinkConfigString(
                  (config == null ? "" : config.getContent()), job.getProperties());
          String encoded =
              new String(Base64.getEncoder().encode(updatedFlinkConfigString.getBytes()));
          saved.setConfig(encoded);
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

          currentApplication = saved;
        } else {

          specialLog("开始更新:" + job.getName());

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
          specialLog("更新前停止运行:" + alreadyExistsApplication.getJobName());
          stopAndWaitIfRunning(alreadyExistsApplication);
          specialLog("停止完毕:" + alreadyExistsApplication.getJobName());

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

          ApplicationConfig config = configService.getEffective(alreadyExistsApplication.getId());
          config =
              config == null ? configService.getLatest(alreadyExistsApplication.getId()) : config;

          String updatedFlinkConfigString =
              getUpdatedFlinkConfigString(
                  (config == null ? "" : config.getContent()), job.getProperties());
          String encoded =
              new String(Base64.getEncoder().encode(updatedFlinkConfigString.getBytes()));
          alreadyExistsApplication.setConfig(encoded);
          applicationService.update(alreadyExistsApplication);
          applicationOfJob.setContent(jobJson);
          applicationOfJobMapper.updateById(applicationOfJob);

          needBuildAndRunApplications.add(alreadyExistsApplication);
          currentApplication = alreadyExistsApplication;
        }
      }

      // 删除本jobId下未提及的任务
      for (Map.Entry<Long, Application> entry : alreadyExistsApplicationIdMap.entrySet()) {
        Long key = entry.getKey();
        Application value = entry.getValue();
        if (!validApplicationIds.contains(key)) {

          Application application = applicationService.getById(value.getId());

          stopAndWaitIfRunning(application);

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

  private String getUpdatedFlinkConfigString(
      String oldZippedConfigString, List<NameAndValue> properties) {

    String zippedConfigString = oldZippedConfigString;

    // 如果没有,则创建
    if (!StringUtils.hasLength(oldZippedConfigString)) {
      ApplicationConfig origin = configService.getEffective(originApplicationId);
      origin = origin == null ? configService.getLatest(originApplicationId) : origin;
      zippedConfigString = origin.getContent();
    }

    specialLog("未解压前:" + zippedConfigString);

    String unzippedConfigString = DeflaterUtils.unzipString(zippedConfigString);

    specialLog("解压后:" + unzippedConfigString);

    DumperOptions dumperOptions = new DumperOptions();
    dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    dumperOptions.setPrettyFlow(false);
    Yaml yaml = new Yaml(dumperOptions);

    Map<String, Object> map =
        yaml.load(PropertiesUtil.castToYaml(PropertiesUtil.castToProperties(unzippedConfigString)));

    updateYamlByProperties(map, properties);

    return yaml.dump(map);
  }

  private void updateYamlByProperties(Map<String, Object> map, List<NameAndValue> properties) {
    if (CollectionUtils.isEmpty(map) || CollectionUtils.isEmpty(properties)) {
      return;
    }

    for (NameAndValue property : properties) {
      if (FlinkJobPropertyType.STATE_BACKEND_TYPE.getName().equals(property.getName())) {
        setValue(map, getKey(FlinkJobPropertyType.STATE_BACKEND_TYPE), property.getValue());
      } else if (FlinkJobPropertyType.CHECK_POINT_INTERVAL.getName().equals(property.getName())) {
        setValue(
            map,
            getKey(FlinkJobPropertyType.CHECK_POINT_INTERVAL),
            getSimpleTimeString(Integer.parseInt(property.getValue())));
      } else if (FlinkJobPropertyType.CHECK_POINT_TIMEOUT.getName().equals(property.getName())) {
        setValue(
            map,
            getKey(FlinkJobPropertyType.CHECK_POINT_TIMEOUT),
            getSimpleTimeString(Integer.parseInt(property.getValue())));
      } else if (FlinkJobPropertyType.PARALLELISM.getName().equals(property.getName())) {
        setValue(
            map, getKey(FlinkJobPropertyType.PARALLELISM), Integer.parseInt(property.getValue()));
      } else if (FlinkJobPropertyType.TASK_SLOTS.getName().equals(property.getName())) {
        setValue(
            map, getKey(FlinkJobPropertyType.TASK_SLOTS), Integer.parseInt(property.getValue()));
      } else if (FlinkJobPropertyType.TASK_MANAGER_MEMORY_SIZE
          .getName()
          .equals(property.getName())) {
        setValue(
            map, getKey(FlinkJobPropertyType.TASK_MANAGER_MEMORY_SIZE), property.getValue() + "MB");
      } else if (FlinkJobPropertyType.JOB_MANAGER_MEMORY_SIZE
          .getName()
          .equals(property.getName())) {
        setValue(
            map, getKey(FlinkJobPropertyType.JOB_MANAGER_MEMORY_SIZE), property.getValue() + "MB");
      }
    }
  }

  private String getSimpleTimeString(int ms) {
    int second = 1000;
    int minute = 60 * second;
    int hour = minute * 60;
    int day = hour * 24;

    StringBuilder result = new StringBuilder();
    if (ms / day > 0) {
      result.append(ms /= day).append("d");
    }
    if (ms / hour > 0) {
      result.append(ms /= hour).append("h");
    }
    if (ms / minute > 0) {
      result.append(ms /= minute).append("min");
    }
    if (ms / second > 0) {
      result.append(ms /= second).append("s");
    }
    return result.toString();
  }

  private static String getKey(FlinkJobPropertyType flinkJobPropertyType) {
    return "flink.property." + flinkJobPropertyType.getName();
  }

  public static Map<String, Object> setValue(Map<String, Object> map, String key, Object value) {
    String[] keys = key.split("\\.");

    int len = keys.length;
    Map temp = map;
    if (len == 1) {
      temp.put(keys[0], value);
    } else {
      for (int i = 0; i < len - 1; i++) {
        if (temp.containsKey(keys[i])) {
          temp = (Map) temp.get(keys[i]);
        } else {
          return null;
        }
        if (i == len - 2) {
          temp.put(keys[i + 1], value);
        }
      }
      for (int j = 0; j < len - 1; j++) {
        if (j == len - 1) {
          map.put(keys[j], temp);
        }
      }
    }
    return map;
  }

  private static void buildFlattenedMap(
      Map<String, Object> result, Map<String, Object> source, @Nullable String path) {
    source.forEach(
        (key, value) -> {
          if (StringUtils.hasText(path)) {
            if (key.startsWith("[")) {
              key = path + key;
            } else {
              key = path + '.' + key;
            }
          }
          if (value instanceof String) {
            result.put(key, value);
          } else if (value instanceof Map) {
            // Need a compound key
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            buildFlattenedMap(result, map, key);
          } else if (value instanceof Collection) {
            // Need a compound key
            @SuppressWarnings("unchecked")
            Collection<Object> collection = (Collection<Object>) value;
            if (collection.isEmpty()) {
              result.put(key, "");
            } else {
              int count = 0;
              for (Object object : collection) {
                buildFlattenedMap(
                    result, Collections.singletonMap("[" + (count++) + "]", object), key);
              }
            }
          } else {
            result.put(key, (value != null ? value : ""));
          }
        });
  }

  public static void main(String[] args) {
    String a =
        "eNqtVttu20YQfddXDCwDtlGTUpzmIepLVcdGhRoSYClNjaIIVuRQ2ojcZXaXlhTk43uWF5GqnAYF6gfbmtk5c+aufq9PDzJiZTkmp8mtmca5iPBnrhO3FYbpXhcqFk5qRZfj+f0V4SMb0opJG8q0YYBEWjkjl4WDKK0ASawMc8bK2ZBozlyiT2eLye0dJTJliqWtjOB8K90aOG4tLW212VACJBHH0jsWKUkFQVbRMLwSJpZqBbf53sjV2pHeKjZ2LfMQKAsfxvy+YWIr2NIngnzSRR1DJ9w6C9f0O2C8k5twCKRL/+SsVp5d/UR7GGdiT0o7Kix3kHkXce5AFKyyPJVCRdyGdfCAXDzVGHrpBJ6LMgzSSfcZCdfrwxI/a+dyOxoMttttKEq2oTarQRPc4AEJnc7vgpIxTN6rlK1Flj4X0iC1yz2JHIQisQTNVGx93crilDUHg61BmtXqmmxddKB0i9Nmq2GHoLsPkC+h6Gw8p8n8jH4Zzyfza2B8mCx+nb1f0Ifx4+N4upjczWn2SLez6bvJYjKb4tM9jadP9Ntk+u6aGLmCG97lxvMHSenzyLEvadM/DQHfHv6zzTmSiYwQl1oVYsW00s9slO+OnE0mra+mBb0YKKnMpCubyJ4GBTe9XpJKtRn1iHKjYe/2I+r/bJlHhzIo32+gZbvFKM2q30GsIxsYTllYDl6Fr94MvGQQc57qvZ+HAaYlkatBz1f3XCoHviIN6yJ5dmGGxhiV+lzmDFgOlci4Eu2FUUevPxdc1Don7CYTCokwoSqyJZtZsoBsnmpnR/SqwhRGpClgbRbGnIgidY3qk1425hljuPcVLFEZW2jlF24kaxb5keDTcxZkDAbIC59ofFlgEiO2XSPXSRKcwCDzERqgI+sG9T1WiUGe/AYJT4Bb1Yt+Kw/xd7hUbE7BS+mLuP89LcfyphNahggk8oUf0TD88ahLmpcCmzjYCnRWJswmKHvsWaQjuhkOM1s+wtJec7TJNZSlgHccFSVsDdLqMU2NECx0jHG4+2N8u3h4+jib3t4dVK2f10N7kDqZsS58iw0RzEFcoOnlSnE8okSklg8K3lUTgUzFQUsCM+UwPWXcj3eL8WQK5x9vxyDw8DD2C6X1qFM2ft0FicBu7qJgCN6+rRNgsQuYliLaMNaDl5WSJtRagdEXdp2JHBZzv25wjrD+kYOy+zDhN6Tz+k5Vo12Y+mBetPN0cU0X/lDYvXWc+U/orY2NlxdX1y3SD9+GqlkcWx5TDaWKTHlzfQ2cKfiklIHFjQafEbVk6kdWPHOVozCWpnqAhTdwWT6I1hvITtD+/WUfRwgZNQ55RRi82pfiWhg0Qm+/Q5FiTnEVYfX4Dyv687Lz4quvaWE48MqvCl9ErnBDnUbM/oi+bhPn712NgX3914vOww7yYXockpL7VnldSyo1vRkOh99A6XBqx3UX1HIb4Ji0Y9gspI7RifLACF8ffCQ9qv42KxH/hqmORBr4+Qq+IA8jqrc5bPzR+h9u1vOg9HR0sqoFgNBZZP7O9i+XwkXrrwfJVa+H84TL6b8imQtbHhysQPZt0T/fMFJ5jlAL/htOLWuU";

    String x = DeflaterUtils.unzipString(a);
    //      String properties = PropertiesUtil.castToProperties(x);
    //    System.out.println(properties);
    //      String yaml = PropertiesUtil.castToYaml(properties);
    //    System.out.println(yaml);

    System.out.println(x);

    DumperOptions dumperOptions = new DumperOptions();
    dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    dumperOptions.setPrettyFlow(false);
    Yaml yaml = new Yaml(dumperOptions);

    Map<String, Object> map =
        yaml.load(PropertiesUtil.castToYaml(PropertiesUtil.castToProperties(x)));

    HashMap<String, Object> result = new LinkedHashMap<>();

    setValue(map, "flink.property.pipeline.name", "10");

    String dump = yaml.dump(map);
    System.out.println(dump);

    System.out.println(map);
  }

  private void deleteAllApplicationByJobId(String jobId) throws Exception {
    List<ApplicationOfJob> applicationOfJobs = applicationOfJobMapper.getByJobId(jobId);

    for (ApplicationOfJob applicationOfJob : applicationOfJobs) {
      Application application = applicationService.getById(applicationOfJob.getAppId());

      stopAndWaitIfRunning(application);

      applicationService.delete(application);
      applicationOfJobMapper.deleteById(applicationOfJob.getId());

      MgmtJobHelper.success(jobId, "删除name为[%s]的job成功");
    }
  }

  private static String buildSql(SQLWrapper sql) {
    return sql.getInput() + "\n" + sql.getOutput() + "\n" + sql.getCalc();
  }

  private static String buildSql(String sql) {
    return sql;
  }

  private void stopAndWaitIfRunning(Application application) throws Exception {
    FlinkAppState flinkAppState = FlinkAppState.of(application.getState());

    if (FlinkAppState.RUNNING.equals(flinkAppState)) {
      cancelApplication(application);
    }

    int waitCount = 0;
    final int maxWaitCount = 5;
    while (waitCount <= maxWaitCount) {

      try {
        Thread.sleep((waitCount + 1) * 1000L);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      Application app = applicationService.getById(application.getId());

      Integer state = app.getState();

      if (FlinkAppState.RUNNING.getValue() != state) {
        break;
      }
      waitCount++;
    }
  }

  private void buildAndRun(String jobId, List<Application> needBuildAndRunApplications) {

    specialLog("开始编译和运行:" + jobId);

    for (Application application : needBuildAndRunApplications) {

      specialLog(
          String.format(
              "开始编译和运行:%s,是否允许编译:%s,",
              application.getJobName(), appBuildPipeService.allowToBuildNow(application.getId())));

      if (appBuildPipeService.allowToBuildNow(application.getId())) {
        specialLog("开始编译:" + application.getJobName());
        this.buildUtilSuccessOrFail(application.getId(), true);
        specialLog("编译完成:" + application.getJobName());
      }

      specialLog("开始启动:" + application.getJobName());

      while (true) {
        {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }

          Application app = applicationService.getById(application.getId());

          ReleaseState releaseState = app.getReleaseState();

          specialLog("发布状态:" + app.getJobName() + "," + releaseState.get());
          specialLog("app:" + ToStringBuilder.reflectionToString(app));

          if (ReleaseState.RELEASING.equals(releaseState)) {
            specialLog("判断到发布中,继续等待:" + app.getJobName());
            continue;
          } else if (ReleaseState.DONE.equals(releaseState)) {

            specialLog("判断到发布完成,准备启动:" + app.getJobName());

            MgmtJobHelper.success(jobId, String.format("job[%s]发布完成", app.getJobName()));
            try {
              applicationService.start(app, false);

              specialLog("启动完成" + app.getJobName());

            } catch (Exception e) {
              specialLog("启动报错" + e.getMessage());
              throw new RuntimeException(e);
            }
            break;
          } else {
            specialLog("不是发布中和发布完成,退出" + application.getJobName());

            MgmtJobHelper.failed(jobId, String.format("job[%s]发布出错", app.getJobName()));
            break;
          }
        }
      }
      //              })
      //          .start();
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
    checkNotEmpty(jobs);

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
          specialLog("编译失败,重新编译");
        }
        try {
          specialLog("等待20秒");
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
        jsonObject.set(FlinkJobPropertyType.TASK_MANAGER_MEMORY_SIZE.getName(), value + "MB");
      } else if (FlinkJobPropertyType.JOB_MANAGER_MEMORY_SIZE.getName().equals(name)) {
        jsonObject.set(FlinkJobPropertyType.JOB_MANAGER_MEMORY_SIZE.getName(), value + "MB");
      }
    }

    application.setOptions(jsonObject.toString());
  }
}

@Slf4j
class PropertiesUtil {

  /**
   * yaml 转 Properties
   *
   * @param input
   * @return
   */
  public static String castToProperties(String input) {
    Map<String, Object> propertiesMap = new LinkedHashMap<>();
    Map<String, Object> yamlMap = new Yaml().load(input);
    flattenMap("", yamlMap, propertiesMap);
    StringBuffer strBuff = new StringBuffer();
    propertiesMap.forEach(
        (key, value) -> strBuff.append(key).append("=").append(value).append(StrUtil.LF));
    return strBuff.toString();
  }

  /**
   * Properties 转 Yaml
   *
   * @param input
   * @return
   */
  public static String castToYaml(String input) {
    try {
      Map<String, Object> properties = readProperties(input);
      return properties2Yaml(properties);
    } catch (Exception e) {
      log.error("property 转 Yaml 转换失败", e);
    }
    return null;
  }

  private static Map<String, Object> readProperties(String input) throws IOException {
    Map<String, Object> propertiesMap = new LinkedHashMap<>(); // 使用 LinkedHashMap 保证顺序
    for (String line : input.split(StrUtil.LF)) {
      if (StrUtil.isNotBlank(line)) {
        // 使用正则表达式解析每一行中的键值对
        Pattern pattern = Pattern.compile("\\s*([^=\\s]*)\\s*=\\s*(.*)\\s*");
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
          String key = matcher.group(1);
          String value = matcher.group(2);
          propertiesMap.put(key, value);
        }
      }
    }
    return propertiesMap;
  }

  /**
   * 递归 Map 集合，转为 Properties集合
   *
   * @param prefix
   * @param yamlMap
   * @param treeMap
   */
  private static void flattenMap(
      String prefix, Map<String, Object> yamlMap, Map<String, Object> treeMap) {
    yamlMap.forEach(
        (key, value) -> {
          String fullKey = prefix + key;
          if (value instanceof LinkedHashMap) {
            flattenMap(fullKey + ".", (LinkedHashMap) value, treeMap);
          } else if (value instanceof ArrayList) {
            List values = (ArrayList) value;
            for (int i = 0; i < values.size(); i++) {
              String itemKey = String.format("%s[%d]", fullKey, i);
              Object itemValue = values.get(i);
              if (itemValue instanceof String) {
                treeMap.put(itemKey, itemValue);
              } else {
                flattenMap(itemKey + ".", (LinkedHashMap) itemValue, treeMap);
              }
            }
          } else {
            treeMap.put(fullKey, String.valueOf(value));
          }
        });
  }

  /**
   * properties 格式转化为 yaml 格式字符串
   *
   * @param properties
   * @return
   */
  private static String properties2Yaml(Map<String, Object> properties) {
    if (CollUtil.isEmpty(properties)) {
      return null;
    }
    Map<String, Object> map = parseToMap(properties);
    StringBuffer stringBuffer = map2Yaml(map);
    return stringBuffer.toString();
  }

  /**
   * 递归解析为 LinkedHashMap
   *
   * @param propMap
   * @return
   */
  private static Map<String, Object> parseToMap(Map<String, Object> propMap) {
    Map<String, Object> resultMap = new LinkedHashMap<>();
    try {
      if (CollectionUtils.isEmpty(propMap)) {
        return resultMap;
      }
      propMap.forEach(
          (key, value) -> {
            if (key.contains(".")) {
              String currentKey = key.substring(0, key.indexOf("."));
              if (resultMap.get(currentKey) != null) {
                return;
              }
              Map<String, Object> childMap = getChildMap(propMap, currentKey);
              Map<String, Object> map = parseToMap(childMap);
              resultMap.put(currentKey, map);
            } else {
              resultMap.put(key, value);
            }
          });
    } catch (Exception e) {
      e.printStackTrace();
    }
    return resultMap;
  }

  /**
   * 获取拥有相同父级节点的子节点
   *
   * @param propMap
   * @param currentKey
   * @return
   */
  private static Map<String, Object> getChildMap(Map<String, Object> propMap, String currentKey) {
    Map<String, Object> childMap = new LinkedHashMap<>();
    try {
      propMap.forEach(
          (key, value) -> {
            if (key.contains(currentKey + ".")) {
              key = key.substring(key.indexOf(".") + 1);
              childMap.put(key, value);
            }
          });
    } catch (Exception e) {
      e.printStackTrace();
    }
    return childMap;
  }

  /**
   * map集合转化为yaml格式字符串
   *
   * @param map
   * @return
   */
  public static StringBuffer map2Yaml(Map<String, Object> map) {
    // 默认deep 为零，表示不空格，deep 每加一层，缩进两个空格
    return map2Yaml(map, 0);
  }

  /**
   * 把Map集合转化为yaml格式 String字符串
   *
   * @param propMap map格式配置文件
   * @param deep 树的层级，默认deep 为零，表示不空格，deep 每加一层，缩进两个空格
   * @return
   */
  private static StringBuffer map2Yaml(Map<String, Object> propMap, int deep) {
    StringBuffer yamlBuffer = new StringBuffer();
    try {
      if (CollectionUtils.isEmpty(propMap)) {
        return yamlBuffer;
      }
      String space = getSpace(deep);
      for (Map.Entry<String, Object> entry : propMap.entrySet()) {
        Object valObj = entry.getValue();
        if (entry.getKey().contains("[") && entry.getKey().contains("]")) {
          String key = entry.getKey().substring(0, entry.getKey().indexOf("[")) + ":";
          yamlBuffer.append(space + key + "\n");
          propMap.forEach(
              (itemKey, itemValue) -> {
                if (itemKey.startsWith(key.substring(0, entry.getKey().indexOf("[")))) {
                  yamlBuffer.append(getSpace(deep + 1) + "- ");
                  if (itemValue instanceof Map) {
                    StringBuffer valStr = map2Yaml((Map<String, Object>) itemValue, 0);
                    String[] split = valStr.toString().split(StrUtil.LF);
                    for (int i = 0; i < split.length; i++) {
                      if (i > 0) {
                        yamlBuffer.append(getSpace(deep + 2));
                      }
                      yamlBuffer.append(split[i]).append(StrUtil.LF);
                    }
                  } else {
                    yamlBuffer.append(itemValue + "\n");
                  }
                }
              });
          break;
        } else {
          String key = space + entry.getKey() + ":";
          if (valObj instanceof String) { // 值为value 类型，不用再继续遍历
            yamlBuffer.append(key + " " + valObj + "\n");
          } else if (valObj instanceof List) { // yaml List 集合格式
            yamlBuffer.append(key + "\n");
            List<String> list = (List<String>) entry.getValue();
            String lSpace = getSpace(deep + 1);
            for (String str : list) {
              yamlBuffer.append(lSpace + "- " + str + "\n");
            }
          } else if (valObj instanceof Map) { // 继续递归遍历
            Map<String, Object> valMap = (Map<String, Object>) valObj;
            yamlBuffer.append(key + "\n");
            StringBuffer valStr = map2Yaml(valMap, deep + 1);
            yamlBuffer.append(valStr.toString());
          } else {
            yamlBuffer.append(key + " " + valObj + "\n");
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return yamlBuffer;
  }

  /**
   * 获取缩进空格
   *
   * @param deep
   * @return
   */
  private static String getSpace(int deep) {
    StringBuffer buffer = new StringBuffer();
    if (deep == 0) {
      return "";
    }
    for (int i = 0; i < deep; i++) {
      buffer.append("  ");
    }
    return buffer.toString();
  }
}
