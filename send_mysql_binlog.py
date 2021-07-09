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
from pymysqlreplication.event import RotateEvent

from datetime import datetime, date
import json
import sys

from ComplexJsonEncoder import ComplexJsonEncoder
from kafka_producer import create_producer, send_message

MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": "root"
}
SERVER_ID = 100
BINLOG_OFFSET_RECORD = "./records/binlog_offset.json"
PREFIX = "data_sync"


def parsing_mysql_binlog(p_log_file=None, p_log_pos=4):
    # 如果binlog日志被清理或缺失，需要从数据库直接读取导入到kafka，然后通过时间戳等过滤之前的数据，比较麻烦
    # 所以实时还是推荐只计算最近的数据，kafka也存在默认的只保留7天数据，历史数据建议不管

    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS,
        server_id=SERVER_ID,
        blocking=True,  # 阻塞等待后续事件，会一直执行
        only_schemas=["test"],
        # ignored_schemas：包含要跳过的模式的数组
        only_tables=["test4", "student"],
        # ignored_tables：包含要跳过的表的数组

        # freeze_schema=True,  # 如果为True，则不支持ALTER TABLE。速度更快
        resume_stream=True,  # 只有设置为True，才可以配合log_file和log_pos参数进行增量解析
        log_file=p_log_file,  # 设置复制开始日志文件（不设置默认读取最后一个文件）
        log_pos=p_log_pos,  # 设置复制开始日志pos（resume_stream应该为True，增量方案，且两参数必须都指定）
        # skip_to_timestamp=0,  # 在达到指定的时间戳之前忽略所有事件
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, RotateEvent]
    )

    log_file = stream.log_file

    for binlog_event in stream:
        # print(binlog_event.dump())  # 打印所有信息

        if isinstance(binlog_event, RotateEvent):
            log_file = binlog_event.next_binlog
            continue

        # 增加log_file, log_pos, log_time
        log_pos = binlog_event.packet.log_pos
        table_pk_list = [binlog_event.primary_key] if isinstance(binlog_event.primary_key, str) else binlog_event.primary_key
        event = {
            "log_file": log_file,  # 为了效率可以去掉，binlog_offset已经记录
            "log_pos": log_pos,  # 为了效率可以去掉，binlog_offset已经记录
            "log_time": str(datetime.fromtimestamp(binlog_event.timestamp)),  # binlog_event.packet.timestamp  # 冗余字段
            "log_timestamp": binlog_event.timestamp,
            "schema": binlog_event.schema,
            "table": binlog_event.table,
            "table_pk": table_pk_list,
        }

        # QueryEvent负责DDL语句(Query!="BEGIN")，WriteRowsEvent、UpdateRowsEvent、DeleteRowsEvent负责DML语句
        if isinstance(binlog_event, DeleteRowsEvent):
            event["action"] = "delete"
        elif isinstance(binlog_event, UpdateRowsEvent):
            event["action"] = "update"
            # event["before_values"] = row["before_values"]
            # event["after_values"] = row["after_values"]
        elif isinstance(binlog_event, WriteRowsEvent):
            event["action"] = "insert"

        event["data"] = binlog_event.rows

        # # 一行行遍历和发送，效率太低，推荐以批次形式发送，同mysql的批次
        # for row in binlog_event.rows:
        #     event["schema"] = binlog_event.schema
        #     event["table"] = binlog_event.table
        #     event["table_pk"] = binlog_event.primary_key
        #
        #     if isinstance(binlog_event, DeleteRowsEvent):
        #         event["action"] = "delete"
        #         event["values"] = row["values"]
        #     elif isinstance(binlog_event, UpdateRowsEvent):
        #         event["action"] = "update"
        #         event["before_values"] = row["before_values"]
        #         event["after_values"] = row["after_values"]
        #     elif isinstance(binlog_event, WriteRowsEvent):
        #         event["action"] = "insert"
        #         event["values"] = row["values"]
        #
        #     print(event)
        #     # sys.stdout.flush()

        # print(event)

        # 发送到kafka
        # 必须要解决时间类型等序列化的问题，因为kafka需要发送json格式的数据
        # 终止或异常结束程序，如何关闭producer，何时执行producer.close()
        # print(json.dumps(event, cls=ComplexJsonEncoder))
        # print(json.dumps(event, indent=4, cls=CJsonEncoder, ensure_ascii=False))
        # data = json.dumps(event).encode()
        data = json.dumps(event, cls=ComplexJsonEncoder)
        data = json.loads(data)  # 成功序列化成json后再转成字典，存入kafka的数据会更直观
        topic = "{}.{}.{}".format(PREFIX, event["schema"], event["table"])
        key = event["log_timestamp"]
        send_message(producer=create_producer(), topic=topic, key=key, value=data, partition=None)

        # 记录处理过的时间戳或log file及log position等等，为了程序失败后继续增量执行
        # 写入redis固化或写入本地文件
        content = {
            "log_file": log_file,
            "log_pos": log_pos,
            "log_time": str(datetime.fromtimestamp(binlog_event.timestamp)),
            "log_timestamp": binlog_event.timestamp,
        }
        write_offset(json.dumps(content))

    # 如果使用阻塞模式blocking=True就不用关闭流了，会一直堵塞等待
    # stream.close()


def read_offset():
    content = {
        "log_file": None,  # 设置show master status为默认值？
        "log_pos": None  # 设置4为默认值？
    }
    try:
        with open(BINLOG_OFFSET_RECORD, "r") as f:
            content = json.loads(f.read())
    except FileNotFoundError as e:
        print(e)
    return content


def write_offset(content):
    with open(BINLOG_OFFSET_RECORD, "w") as f:
        f.write(content)


if __name__ == "__main__":
    config = read_offset()
    print(config)
    log_file = config["log_file"]
    log_pos = config["log_pos"]
    parsing_mysql_binlog(log_file, log_pos)
