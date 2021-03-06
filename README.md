# streamz
High level streams API for fast prototyping and experiments.

Streamz allows you to connect to streaming platforms such as Apache Kafka without getting into the details of a specific platform. **Current roadmap includes the support of Apache Kafka and AWS Kinesis.** 

**Important.** This is still a very early version and the API may change from time to time. Please use with consideration of this fact.

##Usage

```python3
messages = [
        'message 1',
        'message 2'
    ]

kafka_topic = 'my_topic'
producer = StreamProducer(stream_platform='kafka',
                        hosts=['localhost:9092'])
producer.put_records(messages)

```

