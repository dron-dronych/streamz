import boto
from boto.regioninfo import RegionInfo
from boto.kinesis.exceptions import ResourceNotFoundException
import os
from configparser import ConfigParser
from kafka import KafkaProducer


class StreamProducer:
    """
    a client stream producer class supporting the following stream
    types:
    ['kinesis', 'kafka']
    default stream_type is 'kinesis'
    """
    def __init__(self, conn, stream_name, part_key, stream_platform='kinesis'):
        self.type = stream_platform
        self._producer = self._get_producer(stream_platform)(
            conn, stream_name, part_key)

    def put_records(self, messages):
        producer = self._producer
        producer.put_records(messages)
        print('DONE!')

    def put_record(self, message):
        producer = self._producer
        producer.put_record(message)

    def _get_producer(self, stream_type):
        if self.type == 'kinesis':
            return KinesisProducer
        elif self.type == 'kafka':
            return KafkaProducerWrapper
        else:
            raise ValueError('! unknown stream type: {}'.format(self.type))

    def get_conn(self):
        # TODO implement
        pass


class KinesisProducer:
    """
    a Kinesis Stream producer class responsible for pushing
    messages into an AWS Kinesis Stream
    """

    def __init__(self, kinesis_con, stream_name, part_key):
        self.stream_name = stream_name
        self.part_key = part_key
        self.kinesis_con = kinesis_con

    def put_record(self, msg):

        self.kinesis_con.put_record(self.stream_name, msg, self.part_key)

    def put_records(self, msgs):
        for m in msgs:
            self.put_record(m)


class KinesisStreamHealthCheck:
    """
    a Kinesis stream health checker to get information on
    a given stream's operability
    """
    def __init__(self, stream_conn, stream_name):
        self._stream_connection = stream_conn
        self.stream_name = stream_name

    def check_active(self):
        return self._check_status() == 'ACTIVE'

    def check_deleting(self):
        return self._check_status() == 'DELETING'

    def _check_status(self):
        description_map = self._stream_connection.describe_stream(self.stream_name)
        description = description_map.get('StreamDescription')
        return description.get('StreamStatus')


class KafkaProducerWrapper:
    """
    Kafka stream producer
    """
    def __init__(self, hosts):
        self.producer = KafkaProducer(bootsrap_servers=hosts,
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    def put_record(self, topic, msg):
        self.producer.send(topic, msg)

    def put_records(self, topic, msgs):
        for msg in msgs:
            self.put_record(topic, msg)