use streampark;

ALTER TABLE `t_flink_app`
    MODIFY COLUMN `cluster_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL AFTER `version_id`;


-- ----------------------------
-- Table structure for t_application_of_job
-- ----------------------------
DROP TABLE IF EXISTS `t_application_of_job`;
CREATE TABLE `t_application_of_job` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `app_id` bigint(20) NOT NULL,
  `job_id` varchar(64) NOT NULL DEFAULT '0',
  `content` longtext ,
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=100000 DEFAULT CHARSET=utf8mb4;

set foreign_key_checks = 1;


