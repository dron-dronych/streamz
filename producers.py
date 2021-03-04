import boto
from boto.regioninfo import RegionInfo
from boto.kinesis.exceptions import ResourceNotFoundException
import os
from configparser import ConfigParser
from kafka import KafkaProducer
import json


class StreamProducer:
    """
    a client stream producer class supporting the following stream
    types:
    ['kinesis', 'kafka']
    default stream_type is 'kinesis'
    """
    def __init__(self, conn, stream_name, stream_platform='kinesis', hosts=None):
        """
        :param conn: for kinesis
        :param stream_name:
        :param part_key:
        :param stream_platform:
        :param hosts: for kafka
        """
        ConnectParameterValidation.validate(stream_platform, conn, hosts)
        self.type = stream_platform

        # if conn and hosts:
        #     raise ValueError('either a conn object OR hosts should be used. Not both!')

        # can we make this resolution elsewhere?
        # take out to separate method?
        if stream_platform == 'kinesis':
            conn_args = conn, stream_name
        elif stream_platform == 'kafka':
            conn_args = (hosts, stream_name)
        else:
            raise ValueError('unknown platform!')

        self._producer = self._get_producer()(*conn_args)

    def put_records(self, messages, part_key=None):
        producer = self._producer
        if self.type == 'kinesis':
            assert part_key is not None, "For kinesis app the part_key arg should not be None"

        producer.put_records(messages, part_key)
        print('DONE!')

    def put_record(self, message, part_key=None):
        if self.type == 'kinesis':
            assert part_key is not None, "For kinesis app the part_key arg should not be None"

        producer = self._producer
        producer.put_record(message)

    def _get_producer(self):
        if self.type == 'kinesis':
            return KinesisProducer
        elif self.type == 'kafka':
            return KafkaProducerWrapper
        else:
            raise ValueError('! unknown stream type: {}'.format(self.type))


class KinesisProducer:
    """
    a Kinesis Stream producer class responsible for pushing
    messages into an AWS Kinesis Stream
    """

    def __init__(self, kinesis_con, stream_name):
        self.stream_name = stream_name
        self.kinesis_con = kinesis_con

    def put_record(self, msg, part_key):

        self.kinesis_con.put_record(self.stream_name, msg, part_key)

    def put_records(self, msgs, part_key):
        for m in msgs:
            self.put_record(m, part_key)


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
        self.producer = KafkaProducer(bootstrap_servers=hosts,
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    def put_record(self, msg, topic):
        self.producer.send(topic, msg)

    def put_records(self, msgs, topic):
        for msg in msgs:
            self.put_record(msg, topic)


class ConnectionConfig:
    def __init__(self, platform):
        self.platform = platform

    def get(self):
        if self.platform == 'kinesis':
            return 'conn'
        elif self.platform == 'kafka':
            return 'hosts'
        else:
            raise ValueError('platform {} not supported!'.format(
                self.platform
            ))


class ConnectParameterValidation:
    @staticmethod
    def validate(platform, conn, hosts):
        _allowed_platforms = ('kinesis', 'kafka')

        if platform not in _allowed_platforms:
            raise ValueError("unknown platform '{}'! supported: {}".format(
                platform,
                _allowed_platforms))

        if platform == 'kinesis' and not conn:
            raise ValueError('conn argument must be specified for a {} app'.format(
                platform
            ))

        elif platform == 'kafka' and not hosts:
            raise ValueError('conn argument must be specified for a {} app'.format(
                platform
            ))

        else:
            return

