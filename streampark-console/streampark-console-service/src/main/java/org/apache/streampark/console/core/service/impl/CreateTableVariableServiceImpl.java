package org.apache.streampark.console.core.service.impl;

import org.apache.streampark.common.util.DeflaterUtils;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.base.mybatis.pager.MybatisPager;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.CreateTableVariable;
import org.apache.streampark.console.core.entity.FlinkSql;
import org.apache.streampark.console.core.mapper.CreateTableVariableMapper;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.CommonService;
import org.apache.streampark.console.core.service.CreateTableVariableService;
import org.apache.streampark.console.core.service.FlinkSqlService;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class CreateTableVariableServiceImpl
    extends ServiceImpl<CreateTableVariableMapper, CreateTableVariable>
    implements CreateTableVariableService {

  private static final Pattern PLACEHOLDER_PATTERN =
      Pattern.compile("\\$\\{([A-Za-z])+([A-Za-z0-9._-])+\\}");

  private static final String PLACEHOLDER_START = "${";

  private static final String PLACEHOLDER_END = "}";

  @Autowired private ApplicationService applicationService;

  @Autowired private FlinkSqlService flinkSqlService;

  @Autowired private CommonService commonService;

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void createVariable(CreateTableVariable variable) {
    if (this.findByVariableCode(variable.getTeamId(), variable.getVariableCode()) != null) {
      throw new ApiAlertException("Sorry, the variable code already exists.");
    }
    variable.setCreatorId(commonService.getUserId());
    this.save(variable);
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void deleteVariable(CreateTableVariable variable) {
    if (isDependByApplications(variable)) {
      throw new ApiAlertException("Sorry, the variable is actually used.");
    }
    this.removeById(variable);
  }

  @Override
  public IPage<CreateTableVariable> page(CreateTableVariable variable, RestRequest request) {
    if (variable.getTeamId() == null) {
      return null;
    }
    Page<CreateTableVariable> page =
        new MybatisPager<CreateTableVariable>().getDefaultPage(request);
    return this.baseMapper.page(page, variable);
  }

  @Override
  public IPage<Application> dependAppsPage(CreateTableVariable variable, RestRequest request) {
    List<Application> applications = getDependApplicationsByCode(variable);

    IPage<Application> page = new Page<>();
    if (CollectionUtils.isEmpty(applications)) {
      return page;
    }
    page.setCurrent(request.getPageNum());
    page.setSize(request.getPageSize());
    page.setTotal(applications.size());
    int fromIndex = (request.getPageNum() - 1) * request.getPageSize();
    int toIndex = request.getPageNum() * request.getPageSize();
    toIndex = toIndex > applications.size() ? applications.size() : toIndex;
    page.setRecords(applications.subList(fromIndex, toIndex));
    return page;
  }

  @Override
  public CreateTableVariable findByVariableCode(Long teamId, String variableCode) {
    LambdaQueryWrapper<CreateTableVariable> queryWrapper =
        new LambdaQueryWrapper<CreateTableVariable>()
            .eq(CreateTableVariable::getVariableCode, variableCode)
            .eq(CreateTableVariable::getTeamId, teamId);
    return baseMapper.selectOne(queryWrapper);
  }

  /**
   * get variables through team
   *
   * @param teamId
   * @return
   */
  @Override
  public List<CreateTableVariable> findByTeamId(Long teamId) {
    return findByTeamId(teamId, null);
  }

  /**
   * Get variables through team and search keywords.
   *
   * @param teamId
   * @param keyword Fuzzy search keywords through variable code or description, Nullable.
   * @return
   */
  @Override
  public List<CreateTableVariable> findByTeamId(Long teamId, String keyword) {
    return baseMapper.selectByTeamId(teamId, keyword);
  }

  /**
   * Replace variable with defined variable codes.
   *
   * @param teamId
   * @param mixed Text with placeholders, e.g. "--cluster ${kafka.cluster}"
   * @return
   */
  @Override
  public String replaceVariable(Long teamId, String mixed) {
    if (StringUtils.isEmpty(mixed)) {
      return mixed;
    }
    List<CreateTableVariable> variables = findByTeamId(teamId);
    if (CollectionUtils.isEmpty(variables)) {
      return mixed;
    }
    Map<String, String> variableMap =
        variables.stream()
            .collect(
                Collectors.toMap(
                    CreateTableVariable::getVariableCode, CreateTableVariable::getVariableValue));
    String restore = mixed;
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(restore);
    while (matcher.find()) {
      String placeholder = matcher.group();
      String variableCode = getCodeFromPlaceholder(placeholder);
      String variableValue = variableMap.get(variableCode);
      if (StringUtils.isNotEmpty(variableValue)) {
        restore = restore.replace(placeholder, variableValue);
      }
    }
    return restore;
  }

  private boolean isDependByApplications(CreateTableVariable variable) {
    return CollectionUtils.isNotEmpty(getDependApplicationsByCode(variable));
  }

  private List<Application> getDependApplicationsByCode(CreateTableVariable variable) {
    List<Application> dependApplications = new ArrayList<>();
    List<Application> applications = applicationService.getByTeamId(variable.getTeamId());
    Map<Long, Application> applicationMap =
        applications.stream()
            .collect(Collectors.toMap(Application::getId, application -> application));

    // Get applications that depend on this variable in application args
    if (applications != null) {
      for (Application app : applications) {
        if (isDepend(variable.getVariableCode(), app.getArgs())) {
          dependApplications.add(app);
        }
      }
    }
    // Get the application that depends on this variable in flink sql
    List<FlinkSql> flinkSqls = flinkSqlService.getByTeamId(variable.getTeamId());
    if (flinkSqls != null) {
      for (FlinkSql flinkSql : flinkSqls) {
        if (isDepend(variable.getVariableCode(), DeflaterUtils.unzipString(flinkSql.getSql()))) {
          Application app = applicationMap.get(flinkSql.getAppId());
          if (!dependApplications.contains(app)) {
            dependApplications.add(applicationMap.get(flinkSql.getAppId()));
          }
        }
      }
    }
    return dependApplications;
  }

  /**
   * Determine whether variableCode is dependent on mixed.
   *
   * @param variableCode Variable code, e.g. "kafka.cluster"
   * @param mixed Text with placeholders, e.g. "--cluster ${kafka.cluster}"
   * @return If mixed can match the variableCode, return true, otherwise return false
   */
  private boolean isDepend(String variableCode, String mixed) {
    if (StringUtils.isEmpty(mixed)) {
      return false;
    }
    String placeholder = String.format("%s%s%s", PLACEHOLDER_START, variableCode, PLACEHOLDER_END);
    return mixed.contains(placeholder);
  }

  private String getCodeFromPlaceholder(String placeholder) {
    return placeholder.substring(
        PLACEHOLDER_START.length(), placeholder.length() - PLACEHOLDER_END.length());
  }

  @Override
  public boolean existsByTeamId(Long teamId) {
    return this.baseMapper.existsByTeamId(teamId);
  }
}
