package org.apache.streampark.console.plugin.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Date;

@Data
@TableName("t_application_of_job")
@Slf4j
public class ApplicationOfJob implements Serializable {

  @TableId(type = IdType.AUTO)
  private Long id;

  private String jobId;

  private Long appId;

  private Date createTime;
}
