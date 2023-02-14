package org.apache.streampark.console.core.service;

import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.CreateTableVariable;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

public interface CreateTableVariableService extends IService<CreateTableVariable> {

  /**
   * find variable
   *
   * @param variable variable
   * @param restRequest queryRequest
   * @return IPage
   */
  IPage<CreateTableVariable> page(CreateTableVariable variable, RestRequest restRequest);

  /**
   * get variables through team
   *
   * @param teamId
   * @return
   */
  List<CreateTableVariable> findByTeamId(Long teamId);

  /**
   * Get variables through team and search keywords.
   *
   * @param teamId
   * @param keyword Fuzzy search keywords through variable code or description, Nullable.
   * @return
   */
  List<CreateTableVariable> findByTeamId(Long teamId, String keyword);

  boolean existsByTeamId(Long teamId);

  /**
   * create variable
   *
   * @param variable variable
   */
  void createVariable(CreateTableVariable variable);

  void deleteVariable(CreateTableVariable variable);

  CreateTableVariable findByVariableCode(Long teamId, String variableCode);

  String replaceVariable(Long teamId, String mixed);

  IPage<Application> dependAppsPage(CreateTableVariable variable, RestRequest request);
}
