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

import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.core.annotation.ApiAccess;
import org.apache.streampark.console.system.entity.Team;
import org.apache.streampark.console.system.service.TeamService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

@Slf4j
@RestController
@RequestMapping("adapter/team")
public class TeamAdapterController {

  @Autowired private TeamService teamService;

  @ApiAccess
  @PostMapping("check/name")
  public RestResponse checkTeamName(@NotBlank(message = "{required}") String teamName) {
    Team result = this.teamService.findByName(teamName);
    return RestResponse.success(result == null);
  }

  @ApiAccess
  @PostMapping("create")
  public RestResponse addTeam(@Valid Team team) {
    return RestResponse.success(this.teamService.createTeam(team));
  }
}
