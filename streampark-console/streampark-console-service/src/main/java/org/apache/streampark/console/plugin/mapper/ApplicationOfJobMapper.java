package org.apache.streampark.console.plugin.mapper;

import org.apache.streampark.console.plugin.entity.ApplicationOfJob;

import org.apache.ibatis.annotations.Param;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;

import java.util.List;

public interface ApplicationOfJobMapper extends BaseMapper<ApplicationOfJob> {

  List<ApplicationOfJob> getByJobId(@Param("jobId") String jobId);

  ApplicationOfJob getByAppId(@Param("appId") Long appId);
}
