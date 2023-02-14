package org.apache.streampark.console.core.entity;

import org.apache.streampark.common.conf.ConfigConst;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.io.Serializable;
import java.util.Date;

@Data
@TableName("t_create_table_variable")
public class CreateTableVariable implements Serializable {

  private static final long serialVersionUID = -7720746591258904369L;

  @TableId(type = IdType.AUTO)
  private Long id;

  @NotBlank(message = "{required}")
  private String variableCode;

  @NotBlank(message = "{required}")
  private String variableValue;

  @Size(max = 100, message = "{noMoreThan}")
  private String description;

  /** user id of creator */
  private Long creatorId;

  /** user name of creator */
  private transient String creatorName;

  @NotNull(message = "{required}")
  private Long teamId;

  private Boolean desensitization;

  private transient Date createTime;

  private transient Date modifyTime;

  private transient String sortField;

  private transient String sortOrder;

  public void dataMasking() {
    if (desensitization) {
      this.setVariableValue(ConfigConst.DEFAULT_DATAMASK_STRING());
    }
  }
}
