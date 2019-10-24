/*
 Navicat Premium Data Transfer

 Source Server         : LocalhostMysql
 Source Server Type    : MySQL
 Source Server Version : 80017
 Source Host           : localhost:3306
 Source Schema         : ucs_manage

 Target Server Type    : MySQL
 Target Server Version : 80017
 File Encoding         : 65001

 Date: 24/10/2019 19:47:23
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for partitions
-- ----------------------------
DROP TABLE IF EXISTS `partitions`;
CREATE TABLE `partitions` (
  `table_schema` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '库名',
  `table_name` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '表名',
  `partition_column` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '分区键名',
  `keep_data_days` int(11) NOT NULL DEFAULT '0' COMMENT '保留天数,0为永久保留',
  `hourly_interval` int(11) NOT NULL DEFAULT '0' COMMENT '分区间隔',
  `create_next_intervals` int(11) NOT NULL DEFAULT '15' COMMENT '提前创建多少天',
  `status` tinyint(1) NOT NULL DEFAULT '0' COMMENT '状态 0-Init/Disable 初始化/停用 1-Succses/Enable 成功/启用 2-Delete 删除 3-Processing 处理中 4-FAIL 失败 5-Exception 异常',
  `clear_expired_partition` tinyint(1) NOT NULL DEFAULT '0' COMMENT '清除过期分区 0不清除 1清除',
  PRIMARY KEY (`table_schema`,`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='分区管理表';

-- ----------------------------
-- Records of partitions
-- ----------------------------
BEGIN;
INSERT INTO `partitions` VALUES ('creativity', 'user_log_8', 'record_date', 1, 0, 2, 1, 1);
INSERT INTO `partitions` VALUES ('creativity', 'user_log_9', 'record_date2', 13, 0, 150, 0, 1);
COMMIT;

-- ----------------------------
-- Procedure structure for partition_create
-- ----------------------------
DROP PROCEDURE IF EXISTS `partition_create`;
delimiter ;;
CREATE PROCEDURE `ucs_manage`.`partition_create`(p_schema_name VARCHAR(64), p_table_name VARCHAR(64), p_partition_name VARCHAR(64), p_clock INT)
  COMMENT '分区表创建新分区'
BEGIN
        DECLARE v_data_rows INT;
				
        SELECT COUNT(1) INTO v_data_rows
        FROM information_schema.partitions
        WHERE table_schema = p_schema_name AND table_name = p_table_name AND partition_description >= p_clock;
				
				/*
				For security, avoid duplicate creation, 
				increase query judgment on partition table data.
				*/
        IF v_data_rows = 0 THEN
           SELECT CONCAT( "partition_create(", p_schema_name, ",", p_table_name, ",", p_partition_name, ",", p_clock, ")" ) AS msg;
           SET @execute_sql = CONCAT( 'ALTER TABLE ', p_schema_name, '.', p_table_name, ' ADD PARTITION (PARTITION ', p_partition_name, ' VALUES LESS THAN (', p_clock, '));' );
           PREPARE STMT FROM @execute_sql;
           EXECUTE STMT;
           DEALLOCATE PREPARE STMT;
        END IF;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for partition_drop
-- ----------------------------
DROP PROCEDURE IF EXISTS `partition_drop`;
delimiter ;;
CREATE PROCEDURE `ucs_manage`.`partition_drop`(p_schema_name VARCHAR(64), p_table_name VARCHAR(64), p_delete_below_partition_date BIGINT)
  COMMENT '分区表删除指定分区'
BEGIN
        /*
           SCHEMANAME = The DB schema in which to make changes
           TABLENAME = The table with partitions to potentially delete
           DELETE_BELOW_PARTITION_DATE = Delete any partitions with names that are dates older than this one (yyyy-mm-dd)
        */
        DECLARE v_done INT DEFAULT FALSE;
        DECLARE v_drop_partition_name VARCHAR(16);

        /*
           Get a list of all the partitions that are older than the date
           in DELETE_BELOW_PARTITION_DATE.  All partitions are prefixed with
           a "p", so use SUBSTRING TO get rid of that character.
        */
        DECLARE v_row_cursor CURSOR FOR
                SELECT partition_name
                FROM information_schema.partitions
                WHERE table_schema = p_schema_name AND table_name = p_table_name AND CAST(SUBSTRING(partition_name FROM 2) AS UNSIGNED) < p_delete_below_partition_date;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;

        /*
           Create the basics for when we need to drop the partition.  Also, create
           @DROP_PARTITIONS to hold a comma-delimited list of all partitions that
           should be deleted.
        */
        SET @alter_header = CONCAT("ALTER TABLE ", p_schema_name, ".", p_table_name, " DROP PARTITION ");
        SET @drop_partitions = "";

        /*
           Start looping through all the partitions that are too old.
        */
        OPEN v_row_cursor;
        read_loop: LOOP
                FETCH v_row_cursor INTO v_drop_partition_name;
                IF v_done THEN
                        LEAVE read_loop;
                END IF;
                SET @drop_partitions = IF(@drop_partitions = "", v_drop_partition_name, CONCAT(@drop_partitions, ",", v_drop_partition_name));
        END LOOP;
				CLOSE v_row_cursor;
        IF @drop_partitions != "" THEN
                /*
                   1. Build the SQL to drop all the necessary partitions.
                   2. Run the SQL to drop the partitions.
                   3. Print out the table partitions that were deleted.
                */
                SET @execute_sql = CONCAT(@alter_header, @drop_partitions, ";");
                PREPARE STMT FROM @execute_sql;
                EXECUTE STMT;
                DEALLOCATE PREPARE STMT;

                SELECT CONCAT(p_schema_name, ".", p_table_name) AS `table`, @drop_partitions AS `partitions_deleted`;
        ELSE
                /*
                   No partitions are being deleted, so print out "N/A" (Not applicable) to indicate
                   that no changes were made.
                */
                SELECT CONCAT(p_schema_name, ".", p_table_name) AS `table`, "N/A" AS `partitions_deleted`;
        END IF;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for partition_maintenance
-- ----------------------------
DROP PROCEDURE IF EXISTS `partition_maintenance`;
delimiter ;;
CREATE PROCEDURE `ucs_manage`.`partition_maintenance`(p_schema_name VARCHAR(32), p_table_name VARCHAR(32), p_keep_data_days INT, p_create_next_intervals INT,p_partition_column VARCHAR(64),p_clear_expired_partition INT)
  COMMENT '分区表维护管理'
proc_runtime:BEGIN
        DECLARE v_older_than_partition_date VARCHAR(16);
        DECLARE v_partition_name VARCHAR(16);
        DECLARE v_old_partition_name VARCHAR(16);
        DECLARE v_less_than_timestamp DATETIME;
        DECLARE v_current_time VARCHAR(64); 
				
				-- DECLARE table_not_found CONDITION FOR SQLSTATE '42S02';
				-- DECLARE EXIT HANDLER FOR table_not_found SELECT CONCAT("PLEASE CREATE TABLE ", p_schema_name, ".", p_table_name);
				
				IF NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = p_schema_name AND table_name = p_table_name AND column_name = p_partition_column AND data_type = 'datetime' LIMIT 1) THEN
				    SELECT CONCAT("Please check correct for table: ", p_schema_name, ".", p_table_name, " column: ", p_partition_column) AS msg;
				    LEAVE proc_runtime;
				END IF;
				
				/*
				Verify that the data table is a partitioned table. 
				If not, you need to convert the data table to a partitioned table.
				*/
        CALL partition_verify(p_schema_name, p_table_name, p_keep_data_days,p_partition_column);
        SET v_current_time = DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL p_keep_data_days DAY) , '%Y-%m-%d 00:00:00');
        SET @day_num = 1;
        create_loop: LOOP
                IF @day_num > p_create_next_intervals+p_keep_data_days THEN
                        LEAVE create_loop;
                END IF;
								
                SET v_less_than_timestamp = DATE_ADD(v_current_time, INTERVAL @day_num DAY);  
                SET v_partition_name = DATE_FORMAT(DATE_ADD(v_current_time, INTERVAL @day_num-1 DAY), 'p%Y%m%d'); -- 获取当前分区表名称
                IF(v_partition_name != v_old_partition_name) THEN
                        CALL partition_create(p_schema_name, p_table_name, v_partition_name, TO_DAYS(v_less_than_timestamp)); -- 创建当前分区表
                END IF;
                SET @day_num=@day_num+1;
                SET v_old_partition_name = v_partition_name;
        END LOOP;
				
				/*
           Determine the number of days the data is retained. 
					 If it is greater than 0, the partition delete operation will be performed, 
					 otherwise the data partition will remain forever.
        */
				IF p_clear_expired_partition = 1 THEN
								SET v_older_than_partition_date=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL p_keep_data_days DAY), '%Y%m%d'); -- 201608150000 获取最小分区时间
								CALL partition_drop(p_schema_name, p_table_name, v_older_than_partition_date); -- 删除过期分区
				END IF;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for partition_maintenance_all
-- ----------------------------
DROP PROCEDURE IF EXISTS `partition_maintenance_all`;
delimiter ;;
CREATE PROCEDURE `ucs_manage`.`partition_maintenance_all`()
  COMMENT '分区表维护管理所有表'
BEGIN
  DECLARE v_done INT DEFAULT FALSE;
  DECLARE v_table_name VARCHAR(32) DEFAULT NULL;
  DECLARE v_partition_column VARCHAR(32) DEFAULT NULL;
  DECLARE v_keep_data_days INT(11) DEFAULT NULL;
  -- DECLARE v_hourly_interval int(11) DEFAULT NULL;
  DECLARE v_create_next_intervals INT(11) DEFAULT NULL;
  DECLARE v_table_schema VARCHAR(255) DEFAULT NULL;
	DECLARE v_clear_expired_partition TINYINT(1) DEFAULT 0;
	
	DECLARE v_mysql_error_code CHAR(10) DEFAULT '00000';
  DECLARE v_mysql_error_msg VARCHAR(1000);
	
  DECLARE v_row_cursor CURSOR FOR
  SELECT table_schema,table_name,partition_column,keep_data_days,create_next_intervals,clear_expired_partition FROM partitions WHERE status = 1;
  
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;
	DECLARE EXIT HANDLER FOR SQLWARNING,SQLEXCEPTION
	BEGIN
	      ROLLBACK;
				
				GET STACKED DIAGNOSTICS CONDITION 1
				v_mysql_error_code = RETURNED_SQLSTATE, v_mysql_error_msg = MESSAGE_TEXT;

				SELECT v_mysql_error_code AS `mysql_error_code`, v_mysql_error_msg AS `mysql_error_msg`;
	END;
	
	OPEN v_row_cursor;
			read_loop: LOOP
							FETCH v_row_cursor INTO v_table_schema,v_table_name,v_partition_column,v_keep_data_days,v_create_next_intervals,v_clear_expired_partition;
						 	IF v_done THEN
										LEAVE read_loop;
							END IF;

							CALL partition_maintenance(v_table_schema,v_table_name,v_keep_data_days,v_create_next_intervals, v_partition_column,v_clear_expired_partition);
	    END LOOP;
	CLOSE v_row_cursor;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for partition_verify
-- ----------------------------
DROP PROCEDURE IF EXISTS `partition_verify`;
delimiter ;;
CREATE PROCEDURE `ucs_manage`.`partition_verify`(p_schema_name VARCHAR(64), p_table_name VARCHAR(64),p_keep_data_days INT, p_partition_column VARCHAR(64))
  COMMENT '分区表验证并开通'
BEGIN
        DECLARE v_partition_name VARCHAR(16); 
        DECLARE v_data_rows INT(11);
        DECLARE v_future_timestamp TIMESTAMP;
				
				/*
				Check if the data table is a partition table, 
				and judge that the partition name is empty.
				*/
        SELECT COUNT(1) INTO v_data_rows
        FROM information_schema.partitions
        WHERE table_schema = p_schema_name AND table_name = p_table_name AND v_partition_name IS NULL;
        IF v_data_rows = 1 THEN		 
								 /*
								 Calculate time based on data retention days
								 */
								 SET v_future_timestamp = TIMESTAMPADD(HOUR, 24, CONCAT(DATE_SUB(CURDATE(),INTERVAL p_keep_data_days DAY), " ", '00:00:00')); 
								 SET v_partition_name = DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL p_keep_data_days DAY), 'p%Y%m%d');

                 /*
								 Generate data table structure change database statement, 
								 convert to partition table
								 */
								 SET @execute_sql = CONCAT("ALTER TABLE ", p_schema_name, ".", p_table_name, CONCAT(" PARTITION BY RANGE(TO_DAYS(",p_partition_column,"))")); 
								 SET @execute_sql = CONCAT(@execute_sql, "(PARTITION ", v_partition_name, " VALUES LESS THAN (TO_DAYS('", v_future_timestamp, "')));");
								 PREPARE STMT FROM @execute_sql;
								 EXECUTE STMT;
								 DEALLOCATE PREPARE STMT;
        END IF;
END
;;
delimiter ;

-- ----------------------------
-- Event structure for job_for_manage_partition_table
-- ----------------------------
DROP EVENT IF EXISTS `job_for_manage_partition_table`;
delimiter ;;
CREATE EVENT `ucs_manage`.`job_for_manage_partition_table`
ON SCHEDULE
EVERY '1' DAY STARTS '2019-01-01 03:00:00'
ON COMPLETION PRESERVE
COMMENT '分区表管理任务'
DO CALL `partition_maintenance_all`()
;;
delimiter ;

SET FOREIGN_KEY_CHECKS = 1;
