#!/usr/bin/env python
# coding=utf-8

"""
CREATE DATABASE test;
use test;

CREATE TABLE `student` (
  `id` int(11) DEFAULT NULL,
  `name` varchar(100) DEFAULT NULL,
  `sex` varchar(100) DEFAULT NULL,
  `birthday` varchar(100) DEFAULT NULL,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
INSERT INTO `student` VALUES (1, '小明', '男', '1996-05-06', '2021-05-13 14:30:45');
INSERT INTO `student` VALUES (2, '小花', '女', '1995-12-23', '2021-05-13 14:30:45');
INSERT INTO `student` VALUES (3, '张三', '男', '1995-04-05', '2021-05-13 14:30:45');
INSERT INTO `student` VALUES (4, '李四', '男', '1996-01-01', '2021-05-13 14:30:45');
INSERT INTO `student` VALUES (5, 'xiaoming', 'unknown', NULL, '2021-05-13 14:30:45');
INSERT INTO `student` VALUES (6, 'xiaohua', 'femail', NULL, '2021-05-13 14:30:45');
INSERT INTO `student` (id,name,sex,create_time) VALUES (99, 'banana', '-1', '0000-00-00 00:00:00');
INSERT INTO `student` (id,name,sex) VALUES (100,'apple','-1');

CREATE TABLE test4 (id int NOT NULL AUTO_INCREMENT, data VARCHAR(255), data2 VARCHAR(255), PRIMARY KEY(id));
INSERT INTO test4 (data,data2) VALUES ("Hello", "World");
INSERT INTO test4 (data,data2) VALUES ("Java", "python");
INSERT INTO test4 (data) VALUES ("Shell");
UPDATE test4 SET data = "World", data2="Hello" WHERE id = 1;
DELETE FROM test4 WHERE id = 1;
INSERT INTO test4 (data, data2) SELECT 'double','id1' UNION ALL SELECT 'double','id2';
UPDATE test4 SET data = "update double" WHERE data = 'double';
"""

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from pymysqlreplication.event import RotateEvent, QueryEvent

from datetime import datetime, date
import json
import sys

from ComplexJsonEncoder import ComplexJsonEncoder
from kafka_producer import create_producer, send_message

# 如何初始化指定表的数据，和binlog的offset结合，保证数据的完整性？？？
