#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import traceback
import json
import random

from loguru import logger
from datetime import datetime


def producer_demo():
    # 假设生产的消息为键值对（不是一定要键值对），且序列化方式为json
    producer = KafkaProducer(
        bootstrap_servers=['hadoop-prod03:9092'],
        acks="all",  # [0, 1, "all"] 默认1，推荐使用all。 0为只管发送，不管是否成功接收。1为只管发送搭配leader，不管follower是否同步。all代表leader接收同时follower同步。
        key_serializer=lambda k: json.dumps(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode()
    )
    # 发送三条消息
    for i in range(0, 3):
        data = {
            "id": i,
            "name": "user" + str(i),
            "sex": "male",
            "orgid": "ABC" + str(i),
            "orgname": "北京",
            "address": "",
            "create_time": str(datetime.now())
        }
        send_message(producer, topic="kafka_demo", key="src_master", value=data, partition=2)

    # 发送三条json消息
    for i in range(0, 5):
        data = {
            "tid": i,
            "id": random.randint(0, 2),  # 0~2
            "item_name": "banana" + str(i),
            "qty": int(random.random() * 100),
            "msg": "done",
            "skill": ["java", "python"],
            "create_time": str(datetime.now())
        }
        send_message(producer, topic="kafka_demo", key="src_detail", value=data, partition=3)
    producer.close()


def send_message(producer, topic, key, value, partition=None):
    future = producer.send(
        topic,
        key=key,  # 同一个key值，会被送至同一个分区
        value=value,
        partition=partition  # 向分区发送消息
    )
    logger.info("send {}".format(value))
    try:
        future.get(timeout=10)  # 监控是否发送成功
    except kafka_errors:  # 发送失败抛出kafka_errors
        traceback.format_exc()


if __name__ == "__main__":
    producer_demo()
