# coding:utf-8
# python3

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from datetime import datetime
import time

mysql_settings = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': 'root'}

stream = BinLogStreamReader(
    connection_settings=mysql_settings,
    server_id=100,
    # 只能设置开始文件和位置，log_pos必须>=4，后面的会一直解析。不过后面的数据可以通过时间戳判断将数据过滤
    # log_pos=4为RotateEvent事件，一个文件只有一次，可以获取log_file和log_pos
    # 三个参数必须都设置，程序停止后的增量方案
    # resume_stream=True,
    log_file="mysql-bin.000185",
    log_pos=0
)

log_file = stream.log_file

for binlog_event in stream:
    if isinstance(binlog_event, RotateEvent):
        log_file = binlog_event.next_binlog

    binlog_event.dump()
    print(log_file, binlog_event.packet.log_pos)
    print(binlog_event.packet.timestamp,
          time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(binlog_event.packet.timestamp)),
          datetime.fromtimestamp(binlog_event.packet.timestamp),
          binlog_event.timestamp)

stream.close()