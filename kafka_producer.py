#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import traceback
import json
import random

from loguru import logger
from datetime import datetime


def create_producer():
    # 假设生产的消息为键值对（不是一定要键值对），且序列化方式为json
    producer = KafkaProducer(
        bootstrap_servers=['hadoop-prod03:9092'],
        key_serializer=lambda k: json.dumps(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode()
    )
    return producer
    # producer.close()


def send_message(producer, topic, key, value, partition=None):
    future = producer.send(
        topic,
        key=key,  # 同一个key值，会被送至同一个分区，类似哈希key
        value=value,
        partition=partition  # 向分区发送消息
    )
    logger.info("send {}".format(value))
    try:
        future.get(timeout=10)  # 监控是否发送成功
    except kafka_errors:  # 发送失败抛出kafka_errors
        traceback.format_exc()


if __name__ == "__main__":
    producer = create_producer()
    data = {
        "id": 1,
        "name": "小ming",
        "sex": "male",
        "org_id": "ABC001",
        "org_name": "北京xx有限公司",
        "address": "",
        "create_time": str(datetime.now())
    }
    send_message(producer, topic="kafka_demo", key="demo", value=data, partition=0)
    producer.close()
