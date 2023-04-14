package org.apache.streampark.console.core.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.exception.InternalException;
import org.apache.streampark.console.core.annotation.ApiAccess;
import org.apache.streampark.console.core.bean.AppControl;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.enums.AppExistsState;
import org.apache.streampark.console.core.service.*;
import org.apache.streampark.flink.core.FlinkSqlValidationResult;
import org.apache.streampark.flink.packer.pipeline.PipelineStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("adapter/flink/app")
public class ApplicationAdapterController {

  @Autowired private ApplicationService applicationService;

  @Autowired private ApplicationBackUpService backUpService;

  @Autowired private ApplicationLogService applicationLogService;

  @Autowired private AppBuildPipeService appBuildPipeService;

  @Autowired private FlinkSqlService flinkSqlService;

  @ApiAccess
  @RequestMapping("get")
  public RestResponse get(Application app) {
    Application application = applicationService.getApp(app);
    application.setFlinkSql(new String(Base64.getDecoder().decode(application.getFlinkSql())));
    String config = application.getConfig();
    if (StringUtils.hasText(config)) {
      application.setConfig(new String(Base64.getDecoder().decode(config)));
    }

    Map<Long, PipelineStatus> pipeStates =
        appBuildPipeService.listPipelineStatus(Lists.newArrayList(application.getId()));

    if (pipeStates.containsKey(application.getId())) {
      application.setBuildStatus(pipeStates.get(application.getId()).getCode());
    }

    AppControl appControl =
        new AppControl()
            .setAllowBuild(
                application.getBuildStatus() == null
                    || !PipelineStatus.running.getCode().equals(application.getBuildStatus()))
            .setAllowStart(
                !application.shouldBeTrack()
                    && PipelineStatus.success.getCode().equals(application.getBuildStatus()))
            .setAllowStop(application.isRunning());
    application.setAppControl(appControl);

    return RestResponse.success(application);
  }

  @ApiAccess
  @RequestMapping("create")
  public RestResponse create(Application app) throws IOException {
    boolean saved = applicationService.create(app);
    return RestResponse.success(saved);
  }

  @ApiAccess
  @RequestMapping(value = "operation/build")
  public RestResponse buildApplication(Long appId, boolean forceBuild) {
    try {
      Application app = applicationService.getById(appId);
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
      boolean needBuild = applicationService.checkBuildAndUpdate(app);
      if (!needBuild) {
        return RestResponse.success(true);
      }

      // rollback
      if (app.isNeedRollback() && app.isFlinkSqlJob()) {
        flinkSqlService.rollback(app);
      }

      boolean actionResult = appBuildPipeService.buildApplication(app);
      return RestResponse.success(actionResult);
    } catch (Exception e) {
      return RestResponse.success(false).message(e.getMessage());
    }
  }

  @ApiAccess
  @RequestMapping(value = "copy")
  public RestResponse copy(@ApiIgnore Application app) throws IOException {
    Long id = applicationService.copy(app);
    Map<String, String> data = new HashMap<>();
    data.put("id", Long.toString(id));
    return id.equals(0L)
        ? RestResponse.success(false).data(data)
        : RestResponse.success(true).data(data);
  }

  @Autowired
  VariableService variableService;

  @ApiAccess
  @PostMapping("checkSql")
  public RestResponse checkSql(
      @RequestParam String sql,
      @RequestParam Long teamId,
      @RequestParam(required = false, defaultValue = "100000") Long versionId) {
    sql = variableService.replaceVariable(teamId, sql);
    FlinkSqlValidationResult flinkSqlValidationResult = flinkSqlService.verifySql(sql, versionId);
    if (!flinkSqlValidationResult.success()) {
      // record error type, such as error sql, reason and error start/end line
      String exception = flinkSqlValidationResult.exception();
      RestResponse response =
          RestResponse.success()
              .data(false)
              .message(exception)
              .put("type", flinkSqlValidationResult.failedType().getValue())
              .put("start", flinkSqlValidationResult.lineStart())
              .put("end", flinkSqlValidationResult.lineEnd());

      if (flinkSqlValidationResult.errorLine() > 0) {
        response
            .put("start", flinkSqlValidationResult.errorLine())
            .put("end", flinkSqlValidationResult.errorLine() + 1);
      }
      return response;
    } else {
      return RestResponse.success(true);
    }
  }

  @ApiAccess
  @RequestMapping("update")
  public RestResponse update(Application app) {
    String encode = Base64.getEncoder().encodeToString(app.getConfig().getBytes());
    app.setConfig(encode);
    applicationService.update(app);
    return RestResponse.success(true);
  }

  @ApiAccess
  @PostMapping("list")
  public RestResponse list(Application app, RestRequest request) {
    IPage<Application> applicationList = applicationService.page(app, request);
    List<Application> appRecords = applicationList.getRecords();
    List<Long> appIds = appRecords.stream().map(Application::getId).collect(Collectors.toList());
    Map<Long, PipelineStatus> pipeStates = appBuildPipeService.listPipelineStatus(appIds);

    // add building pipeline status info and app control info
    appRecords =
        appRecords.stream()
            .peek(
                e -> {
                  if (pipeStates.containsKey(e.getId())) {
                    e.setBuildStatus(pipeStates.get(e.getId()).getCode());
                  }
                })
            .peek(
                e -> {
                  AppControl appControl =
                      new AppControl()
                          .setAllowBuild(
                              e.getBuildStatus() == null
                                  || !PipelineStatus.running.getCode().equals(e.getBuildStatus()))
                          .setAllowStart(
                              !e.shouldBeTrack()
                                  && PipelineStatus.success.getCode().equals(e.getBuildStatus()))
                          .setAllowStop(e.isRunning());
                  e.setAppControl(appControl);
                })
            .collect(Collectors.toList());
    applicationList.setRecords(appRecords);
    return RestResponse.success(applicationList);
  }

  @ApiAccess
  @PostMapping(value = "operation/start", consumes = "application/x-www-form-urlencoded")
  public RestResponse start(@ApiIgnore Application app) {
    try {
      applicationService.checkEnv(app);
      applicationService.starting(app);
      applicationService.start(app, false);
      return RestResponse.success(true);
    } catch (Exception e) {
      return RestResponse.success(false).message(e.getMessage());
    }
  }

  @ApiAccess
  @PostMapping(value = "operation/cancel", consumes = "application/x-www-form-urlencoded")
  public RestResponse cancel(@ApiIgnore Application app) throws Exception {
    applicationService.cancel(app);
    return RestResponse.success();
  }

  @ApiAccess
  @PostMapping("checkName")
  public RestResponse checkName(Application app) {
    AppExistsState exists = applicationService.checkExists(app);
    return RestResponse.success(exists.get());
  }

  @ApiAccess
  @PostMapping("operation/log")
  public RestResponse operationLog(ApplicationLog applicationLog, RestRequest request) {
    IPage<ApplicationLog> applicationList = applicationLogService.page(applicationLog, request);
    return RestResponse.success(applicationList);
  }

  @ApiAccess
  @PostMapping("delete")
  public RestResponse delete(Application app) throws InternalException {
    Boolean deleted = applicationService.delete(app);
    return RestResponse.success(deleted);
  }
}
