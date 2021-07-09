#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from loguru import logger


def consumer_demo():
    consumer = KafkaConsumer(
        'kafka_demo',
        bootstrap_servers=['hadoop-prod03:9092'],
        group_id='test',
        enable_auto_commit=False
    )
    for msg in consumer:
        receive = "{topic}:{partition}:{offset}: key={key} value={value}".format(
            topic=msg.topic,
            partition=msg.partition,
            offset=msg.offset,
            key=msg.key,
            value=msg.value
        )
        logger.info(receive)
        consumer.commit()


def consumer_sub_demo():
    consumer = KafkaConsumer(
        bootstrap_servers=['hadoop-prod03:9092'],
        group_id='test',
        enable_auto_commit=False,
        auto_offset_reset="earliest",  # 默认值是latest
    )

    print("Partitions Information: ", consumer.partitions_for_topic("kafka_demo"))  # 获取test主题的分区信息

    consumer.assign([
        TopicPartition(topic="kafka_demo", partition=0),
        TopicPartition(topic="kafka_demo", partition=1),
        TopicPartition(topic="kafka_demo", partition=2),
        TopicPartition(topic="kafka_demo", partition=3),
    ])
    # print(consumer.assignment())
    # print(consumer.beginning_offsets(consumer.assignment()))
    # consumer.seek_to_beginning();
    start = 25
    consumer.seek(TopicPartition(topic="kafka_demo", partition=0), offset=start)

    # # 手动拉取消息
    # while True:
    #     msg = consumer.poll(timeout_ms=5)  # 从kafka获取消息
    #     print(msg)
    #     time.sleep(1)

    for msg in consumer:
        receive = "{topic}:{partition}:{offset}: key={key} value={value}".format(
            topic=msg.topic,
            partition=msg.partition,
            offset=msg.offset,
            key=msg.key,
            value=msg.value
        )
        logger.info(receive)
        consumer.commit()


if __name__ == "__main__":
    # consumer_demo()
    consumer_sub_demo()