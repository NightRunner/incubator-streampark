DROP TABLE IF EXISTS `t_create_table_variable`;
CREATE TABLE `t_create_table_variable` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `variable_code` varchar(100) NOT NULL COMMENT 'Variable code is used for parameter names passed to the program or as placeholders',
  `variable_value` text NOT NULL COMMENT 'The specific value corresponding to the variable',
  `description` text COMMENT 'More detailed description of variables',
  `creator_id` bigint(20) NOT NULL COMMENT 'user id of creator',
  `team_id` bigint(20) NOT NULL COMMENT 'team id',
  `desensitization` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0 is no desensitization, 1 is desensitization, if set to desensitization, it will be replaced by * when displayed',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `modify_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'modify time',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `un_team_vcode_inx` (`team_id`,`variable_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=100007 DEFAULT CHARSET=utf8mb4;

INSERT INTO `t_menu` VALUES (900001, 100013, 'menu.createTableVariable', '/flink/variable/create/table', 'flink/variable/create/table/View', NULL, 'code', '0', 1, 4, now(), now());
INSERT INTO `t_menu` VALUES (900002, 900001, 'add', NULL, NULL, 'variable:create:table:add', NULL, '1', 1, NULL, now(), now());
INSERT INTO `t_menu` VALUES (900003, 900001, 'update', NULL, NULL, 'variable:create:table:update', NULL, '1', 1, NULL, now(), now());
INSERT INTO `t_menu` VALUES (900004, 900001, 'delete', NULL, NULL, 'variable:create:table:delete', NULL, '1', 1, NULL, now(), now());
INSERT INTO `t_menu` VALUES (900005, 900001, 'depend apps', '/flink/variable/create/table/depend_apps', 'flink/variable/create/table/DependApps', 'variable:create:table:depend_apps', NULL, '0', 0, NULL, now(), now());
INSERT INTO `t_menu` VALUES (900006, 900001, 'show original', NULL, NULL, 'variable:create:table:show_original', NULL, '1', 1, NULL, now(), now());
INSERT INTO `t_menu` VALUES (900007, 900001, 'view', NULL, NULL, 'variable:create:table:view', NULL, '1', 1, NULL, now(), now());
INSERT INTO `t_menu` VALUES (900008, 900001, 'depend view', NULL, NULL, 'variable:create:table:depend_apps', NULL, '1', 1, NULL, now(), now());

insert into `t_role_menu` values (900001, 100001, 900001);
insert into `t_role_menu` values (900002, 100001, 900002);
insert into `t_role_menu` values (900003, 100001, 900003);
insert into `t_role_menu` values (900004, 100001, 900004);
insert into `t_role_menu` values (900005, 100001, 900005);
insert into `t_role_menu` values (900006, 100001, 900006);
insert into `t_role_menu` values (900007, 100001, 900007);
insert into `t_role_menu` values (900008, 100001, 900008);
insert into `t_role_menu` values (900009, 100001, 900009);
