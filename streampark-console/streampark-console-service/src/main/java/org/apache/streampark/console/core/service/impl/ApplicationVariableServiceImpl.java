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

package org.apache.streampark.console.core.service.impl;

import org.apache.streampark.console.core.entity.ApplicationConfig;
import org.apache.streampark.console.core.enums.ApplicationVariable;
import org.apache.streampark.console.core.service.ApplicationVariableService;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

@Service
public class ApplicationVariableServiceImpl implements ApplicationVariableService {
  @Override
  public String replaceVariable(String mixed, ApplicationConfig applicationConfig) {
    if (StringUtils.isEmpty(mixed)) {
      return mixed;
    }

    if (applicationConfig == null) {
      return mixed;
    }

    for (ApplicationVariable value : ApplicationVariable.values()) {
      if (!mixed.contains(value.getPlaceHolder())) continue;
      if (ApplicationVariable.APP_ID.equals(value)) {
        mixed = mixed.replace(value.getPlaceHolder(), applicationConfig.getAppId().toString());
      }
    }
    return mixed;
  }
}
