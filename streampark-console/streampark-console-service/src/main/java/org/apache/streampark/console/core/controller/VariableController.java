/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.console.core.controller;

import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.ApplicationConfig;
import org.apache.streampark.console.core.entity.CreateTableVariable;
import org.apache.streampark.console.core.entity.Variable;
import org.apache.streampark.console.core.enums.ApplicationVariable;
import org.apache.streampark.console.core.service.*;

import com.baomidou.mybatisplus.core.metadata.IPage;
import lombok.extern.slf4j.Slf4j;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Validated
@RestController
@RequestMapping("variable")
public class VariableController {

  @Autowired private VariableService variableService;

  @Autowired private CreateTableVariableService createTableVariableService;

  @Autowired private ApplicationVariableService applicationVariableService;

  @Autowired private ApplicationConfigService applicationConfigService;

  /**
   * Get variable list by page.
   *
   * @param restRequest
   * @param variable
   * @return
   */
  @PostMapping("page")
  @RequiresPermissions("variable:view")
  public RestResponse page(RestRequest restRequest, Variable variable) {
    IPage<Variable> page = variableService.page(variable, restRequest);
    for (Variable v : page.getRecords()) {
      v.dataMasking();
    }
    return RestResponse.success(page);
  }

  /**
   * Get variables through team and search keywords.
   *
   * @param teamId
   * @param keyword Fuzzy search keywords through variable code or description, Nullable.
   * @return
   */
  @PostMapping("list")
  public RestResponse variableList(
      @RequestParam Long teamId, @RequestParam(required = false) String appId, String keyword) {
    List<Variable> variableList = variableService.findByTeamId(teamId, keyword);
    for (Variable v : variableList) {
      v.dataMasking();
    }
    List<CreateTableVariable> createTableVariableList =
        createTableVariableService.findByTeamId(teamId, keyword);

    List<Object> result = new ArrayList<>(variableList);

    ApplicationConfig applicationConfig = applicationConfigService.getById(appId);

    for (CreateTableVariable createTableVariable : createTableVariableList) {
      String originVariableValue = createTableVariable.getVariableValue();
      String replacedVariableValue = variableService.replaceVariable(teamId, originVariableValue);
      replacedVariableValue =
          applicationVariableService.replaceVariable(replacedVariableValue, applicationConfig);
      createTableVariable.setVariableValue(replacedVariableValue);
    }

    result.addAll(createTableVariableList);

    for (ApplicationVariable value : ApplicationVariable.values()) {
      Variable variable = new Variable();
      if (ApplicationVariable.APP_ID.equals(value) && applicationConfig != null) {
        variable.setVariableValue(applicationConfig.getAppId().toString());
      } else {
        variable.setVariableValue(value.getCode());
      }
      variable.setVariableCode(value.getCode());
      variable.setDescription("");
      variable.setTeamId(teamId);
      variable.setDesensitization(false);
      result.add(variable);
    }

    return RestResponse.success(result);
  }

  @PostMapping("dependApps")
  @RequiresPermissions("variable:depend_apps")
  public RestResponse dependApps(RestRequest restRequest, Variable variable) {
    IPage<Application> dependApps = variableService.dependAppsPage(variable, restRequest);
    return RestResponse.success(dependApps);
  }

  @PostMapping("post")
  @RequiresPermissions("variable:add")
  public RestResponse addVariable(@Valid Variable variable) {
    this.variableService.createVariable(variable);
    return RestResponse.success();
  }

  @PutMapping("update")
  @RequiresPermissions("variable:update")
  public RestResponse updateVariable(@Valid Variable variable) {
    if (variable.getId() == null) {
      throw new ApiAlertException("Sorry, the variable id cannot be null.");
    }
    Variable findVariable = this.variableService.getById(variable.getId());
    if (findVariable == null) {
      throw new ApiAlertException("Sorry, the variable does not exist.");
    }
    if (!findVariable.getVariableCode().equals(variable.getVariableCode())) {
      throw new ApiAlertException("Sorry, the variable code cannot be updated.");
    }
    this.variableService.updateById(variable);
    return RestResponse.success();
  }

  @PostMapping("showOriginal")
  @RequiresPermissions("variable:show_original")
  public RestResponse showOriginal(@RequestParam Long id) {
    Variable v = this.variableService.getById(id);
    return RestResponse.success(v);
  }

  @DeleteMapping("delete")
  @RequiresPermissions("variable:delete")
  public RestResponse deleteVariable(@Valid Variable variable) {
    this.variableService.deleteVariable(variable);
    return RestResponse.success();
  }

  @PostMapping("check/code")
  public RestResponse checkVariableCode(
      @RequestParam Long teamId, @NotBlank(message = "{required}") String variableCode) {
    boolean result = this.variableService.findByVariableCode(teamId, variableCode) == null;
    return RestResponse.success(result);
  }
}
