#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kafka import KafkaConsumer, TopicPartition
from loguru import logger
import json


def create_consumer():
    consumer = KafkaConsumer(
        bootstrap_servers=["hadoop-prod03:9092"],
        group_id="test",
        enable_auto_commit=False,
        auto_offset_reset="earliest",  # 默认值是latest
    )
    # 获取主题列表
    print("All Topic:", consumer.topics())
    # 获取当前消费者订阅的主题
    print("Subscription Topic:", consumer.subscription())
    # 获取指定topic的partition信息
    print("Partitions Information:", consumer.partitions_for_topic("kafka_demo"))
    # 指派监控的topic和partition
    consumer.assign([
        TopicPartition(topic="kafka_demo", partition=0),
        TopicPartition(topic="kafka_demo", partition=1),
        TopicPartition(topic="kafka_demo", partition=2),
        TopicPartition(topic="kafka_demo", partition=3),
    ])
    # 获取当前消费者topic、partition信息
    print(consumer.assignment())
    # 获取当前消费者topic、partition、position的信息
    print(consumer.beginning_offsets(list(consumer.assignment())))
    # 获取当前主题的最新偏移量，也就是该partition下一个offset（还未生成的）
    print(consumer.position(TopicPartition(topic="kafka_demo", partition=0)))
    # 指定指派过的partition和指定offset消费
    consumer.seek(TopicPartition(topic="kafka_demo", partition=0), offset=25)
    return consumer


def receive_message(consumer):
    for msg in consumer:
        topic = msg.topic
        partition = msg.partition
        receive = "{topic}:{partition}:{offset}: key={key} value={value}".format(
            topic=topic,
            partition=partition,
            offset=msg.offset,
            key=msg.key,
            value=msg.value
        )
        logger.info(receive)
        # do something
        content = {
            "topic": topic,
            "partition": partition,
            "offset": msg.offset
        }
        # 多个topic推荐分成多个文件，存在写文件失败重复接收数据的问题。优化为异步操作？
        with open(f"./records/consumer_{topic}_partition{partition}_offset.json", "w") as f:
            f.write(json.dumps(content))
        consumer.commit()


if __name__ == "__main__":
    consumer = create_consumer()
    receive_message(consumer)
