# streamz
High level streams API for fast prototyping and experiments.

Streamz allows you to connect to streaming platforms such as Apache Kafka without getting into the details of a specific platform. **Current roadmap includes the support of Apache Kafka and AWS Kinesis.** 

**Important.** This is still a very early version and the API may change from time to time. Please use with consideration of this fact.

## Usage

```python3
from streamz.producers import StreamProducer
import boto

messages = [
        'message 1',
        'message 2'
    ]

# Kafka producer
kafka_topic = 'my_topic'
producer = StreamProducer(stream_platform='kafka',
                        hosts=['localhost:9092'])
producer.put_records(messages)

# now Kinesis producer
aws_access_key = ''
aws_secret_key = ''
kinesis_region_name = 'my_region'
kinesis_region_host = 'https://region.host'

kinesis_region = RegionInfo(name=kinesis_region_name, endpoint=kinesis_region_host)
kinesis = boto.connect_kinesis(aws_access_key,
                               aws_secret_key,
                               region=kinesis_region)
kinesis_stream_name = 'mystream'

sp = StreamProducer(kinesis_stream_name, conn=kinesis)
sp.put_records(messages, part_key='night')

```

