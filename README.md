# The Offset in a partition is being reset incorrectly

We were seeing lag in our application and figured it was do to low partitions and high volumes of data. But upon further investigation we found that the consumer was "moving backwards" and re-processing a series of messages on a partition. 

When a message has an exception, the `breakOnFirstError` will cause the partition/offset to be replayed again after the consumer is removed and readded to the consumer group. The 2nd time it occurs, it is typically followed by a seek to -1. This allows the consumer to move forward through the rest of the messages in that partition. However, it appears that sometimes the offset will be set to the value from another partition. That value can then result in the consumer reading from the partition where the error occurred in the wrong place. 

What makes this particular observation tough to catch and debug is that it appears to be intermittent. 

Under the `src/logs` are 3 text files representing the logs for 3 separate runs of the provided test. Two of these logs capture the scenario where the offset is reset incorrectly. One of the logs captures what is expected to occur. The bottom of this README annotates the issue from the logs. 

## The Environment
The issue was first observed with the consumer running:

- Rocky Linux 8.7
- Open JDK 11.0.8
- Camel 3.21.0
- Spring Boot 2.7.14
- Strimzi Kafka 0.28.0/3.0.0

## The Kafka configuration
The basic Kafka settings:

- autoCommitEnable = false
- allowManualCommit = true
- autoOffsetReset = earliest
- maxPollRecords = 1
- breakOnFirstError = true
- consumerCount = 3 (same as partitions on the topic)

## The Route 
The Camel route is consuming messages from the topic. It will perform basic validation and then do a series of inserts and updates into the database. 

Depending on various errors they are considered retryable or non-retryable. An example of the former might be a failure to connect to the database. An example of the latter might be missing data in the database. 

When a retryable problem is encountered, the exception is thrown and is unhandled in order to force Camel to roll back any database activity that has already been performed. The Kafka offset is not committed. This allows the message to be re-consumed and processed once the problem is corrected.

When a non-retryable problem is encountered, the exceptions is also thrown and is unhandled. This is also to force Camel to roll back any database activity that has already been performed. In this case the Kafka offset is committed so that the message is not seen again. 

## Using breakOnFirstError and the subsequent behavior 

From the Camel docs (`breakOnFirstError`)

> This option controls what happens when a consumer is processing an exchange and it fails. If the option is false then the consumer continues to the next message and processes it. If the option is true then the consumer breaks out, and will seek back to offset of the message that caused a failure, and then re-attempt to process this message. However this can lead to endless processing of the same message if its bound to fail every time, eg a poison message. Therefore it is recommended to deal with that for example by using Camel’s error handler.

In the sample application we are publishing 13 messages. Each is a simple number or the text "NORETRY-ERROR". Each number is only published to the topic one time. The "NORETRY-ERROR" is published twice.

Based on the way we have the route written and the expected behavior in Camel we would expect the following to occur when a non-retryable error occurs:

- consume the message at the partition:offset (2:3)
- throw an exception that is unhandled
- commit the offset manually (2:3)
- the undhandled exception is handled by Camel (rollback) 
- the `KafkaRecordProcessor` will log > Will seek consumer to offset 2 and start polling again.

then:

- consume the message at the partition:offset (2:3) for a second time
- throw an exception that is unhandled
- commit the offset manually (2:3)
- the undhandled exception is handled by Camel (rollback) 
- the `KafkaRecordProcessor` will log > Will seek consumer to offset -1 and start polling again.

then:

- consume the message at the partition:offset (2:4)

At the end of the rest run, this is the expected result 

| Payload Body         | Times Processed  | 
|----------------------|------------------|
| NORETRY-ERROR        | 4 times 
| 1			           | 1 times
| 2				       | 1 times 
| 3			           | 1 times
| 4			           | 1 times
| 5			           | 1 times
| 6			           | 1 times
| 7			           | 1 times
| 8			           | 1 times
| 9			           | 1 times
| 10			           | 1 times
| 11			           | 1 times

If the test is re-run several times, it will (eventually) have an issue where the offset is set incorrectly.
The attached log (src/logs) has the following (as it started to replay messages)

| Payload Body         | Times Processed  | 
|----------------------|------------------|
| NORETRY-ERROR        | 4 times 
| 3  		           | 2 times
| 11				       | 1 times 
| 1			           | 1 times
| 2			           | 1 times
| 4			           | 1 times
| 5			           | 1 times
| 6			           | 1 times
| 7			           | 1 times
| 8			           | 1 times
| 10			           | 1 times

The NORETRY-ERROR was written to partition 0 with an offset of 1.
It was consumed:

```
2023-10-24 | 09:52:19.405 | INFO  | [Camel (camel-1) thread #2 - KafkaConsumer[foobarTopic]] | codesmell.test.CamelKafkaOffsetTest (CamelKafkaOffsetTest.java:147) | Message consumed from Kafka
Message consumed from foobarTopic
The Partion:Offset is 0:1
The Key is null
NORETRY-ERROR
```

When it resulted in an exception the offset was committed.

```
2023-10-24 | 09:52:19.510 | INFO  | [Camel (camel-1) thread #2 - KafkaConsumer[foobarTopic]] | c.c.k.KafkaOffsetManagerProcessor (KafkaOffsetManagerProcessor.java:49) | manually committing the offset for batch
Message consumed from foobarTopic
The Partion:Offset is 0:1
The Key is null
NORETRY-ERROR
```

Then the unhandled exception was handed over to Camel

```
2023-10-24 | 09:52:19.530 | ERROR | [Camel (camel-1) thread #2 - KafkaConsumer[foobarTopic]] | o.a.c.p.e.DefaultErrorHandler (CamelLogger.java:205) | Failed delivery for (MessageId: 6561DE1EA878C39-0000000000000000 on ExchangeId: 6561DE1EA878C39-0000000000000000). Exhausted after delivery attempt: 1 caught: codesmell.exception.NonRetryException: NON RETRY ERROR TRIGGERED BY TEST. Processed by failure processor: FatalFallbackErrorHandler[null]
```

It then seeks to offset 0 (which is correct) and removes itself from the consumer group

```
2023-10-24 | 09:52:19.531 | WARN  | [Camel (camel-1) thread #2 - KafkaConsumer[foobarTopic]] | o.a.c.c.k.c.s.KafkaRecordProcessor (KafkaRecordProcessor.java:132) | Will seek consumer to offset 0 and start polling again.
2023-10-24 | 09:52:19.537 | INFO  | [Camel (camel-1) thread #2 - KafkaConsumer[foobarTopic]] | o.a.k.c.c.i.ConsumerCoordinator (ConsumerCoordinator.java:311) | [Consumer clientId=consumer-test_group_id-1, groupId=test_group_id] Revoke previously assigned partitions foobarTopic-0
```

When it rejoins partition 1 is set to offset 5

```
2023-10-24 | 09:52:22.205 | INFO  | [Camel (camel-1) thread #3 - KafkaConsumer[foobarTopic]] | o.a.k.c.c.i.ConsumerCoordinator (ConsumerCoordinator.java:851) | [Consumer clientId=consumer-test_group_id-3, groupId=test_group_id] Setting offset for partition foobarTopic-1 to the committed offset FetchPosition{offset=5, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:65036 (id: 0 rack: null)], epoch=0}}
```

Partition 0 is set to offset 1

```
2023-10-24 | 09:52:22.205 | INFO  | [Camel (camel-1) thread #1 - KafkaConsumer[foobarTopic]] | o.a.k.c.c.i.ConsumerCoordinator (ConsumerCoordinator.java:851) | [Consumer clientId=consumer-test_group_id-2, groupId=test_group_id] Setting offset for partition foobarTopic-0 to the committed offset FetchPosition{offset=1, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:65036 (id: 0 rack: null)], epoch=0}}
```

Partition 2 is set to offset 4

```
2023-10-24 | 09:52:22.205 | INFO  | [Camel (camel-1) thread #2 - KafkaConsumer[foobarTopic]] | o.a.k.c.c.i.ConsumerCoordinator (ConsumerCoordinator.java:851) | [Consumer clientId=consumer-test_group_id-4, groupId=test_group_id] Setting offset for partition foobarTopic-2 to the committed offset FetchPosition{offset=4, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:65036 (id: 0 rack: null)], epoch=0}}
```

The message at partition 0 and offset 1 is reconsumed based on the behavior for `breakOnFirstError`

```
2023-10-24 | 09:52:22.691 | INFO  | [Camel (camel-1) thread #1 - KafkaConsumer[foobarTopic]] | codesmell.test.CamelKafkaOffsetTest (CamelKafkaOffsetTest.java:147) | Message consumed from Kafka
Message consumed from foobarTopic
The Partion:Offset is 0:1
The Key is null
NORETRY-ERROR
```

The message offset is committed and Camel gets the unhandled exception

```
2023-10-24 | 09:52:22.697 | INFO  | [Camel (camel-1) thread #1 - KafkaConsumer[foobarTopic]] | c.c.k.KafkaOffsetManagerProcessor (KafkaOffsetManagerProcessor.java:49) | manually committing the offset for batch
Message consumed from foobarTopic
The Partion:Offset is 0:1
The Key is null
NORETRY-ERROR
2023-10-24 | 09:52:22.714 | ERROR | [Camel (camel-1) thread #1 - KafkaConsumer[foobarTopic]] | o.a.c.p.e.DefaultErrorHandler (CamelLogger.java:205) | Failed delivery for (MessageId: 6561DE1EA878C39-0000000000000001 on ExchangeId: 6561DE1EA878C39-0000000000000001). Exhausted after delivery attempt: 1 caught: codesmell.exception.NonRetryException: NON RETRY ERROR TRIGGERED BY TEST. Processed by failure processor: FatalFallbackErrorHandler[null]
```

This time when the consumer is removed from the consumer group the seek should use -1 so that it moves forward. However, it seeks to offset 4 instead. This seems to be the offset assigned to partition 2. Observations suggest it is always grabbing the current offset from another partition. 

```
2023-10-24 | 09:52:22.715 | WARN  | [Camel (camel-1) thread #1 - KafkaConsumer[foobarTopic]] | o.a.c.c.k.c.s.KafkaRecordProcessor (KafkaRecordProcessor.java:132) | Will seek consumer to offset 4 and start polling again.
2023-10-24 | 09:52:22.720 | INFO  | [Camel (camel-1) thread #1 - KafkaConsumer[foobarTopic]] | o.a.k.c.c.i.ConsumerCoordinator (ConsumerCoordinator.java:311) | [Consumer clientId=consumer-test_group_id-2, groupId=test_group_id] Revoke previously assigned partitions foobarTopic-0
```

When it rejoins it now starts to set the offsets and ends up getting an out of range.

Parition 1 is set to offset 5

```
2023-10-24 | 09:52:25.238 | INFO  | [Camel (camel-1) thread #2 - KafkaConsumer[foobarTopic]] | o.a.k.c.c.i.ConsumerCoordinator (ConsumerCoordinator.java:851) | [Consumer clientId=consumer-test_group_id-4, groupId=test_group_id] Setting offset for partition foobarTopic-1 to the committed offset FetchPosition{offset=5, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:65036 (id: 0 rack: null)], epoch=0}}
```

Partition 0 is set to offset 5. This should have been set to 2.

```
2023-10-24 | 09:52:25.238 | INFO  | [Camel (camel-1) thread #3 - KafkaConsumer[foobarTopic]] | o.a.k.c.c.i.ConsumerCoordinator (ConsumerCoordinator.java:851) | [Consumer clientId=consumer-test_group_id-3, groupId=test_group_id] Setting offset for partition foobarTopic-0 to the committed offset FetchPosition{offset=5, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:65036 (id: 0 rack: null)], epoch=0}}
```

Partition 2 is set to offset 4

```
2023-10-24 | 09:52:25.238 | INFO  | [Camel (camel-1) thread #1 - KafkaConsumer[foobarTopic]] | o.a.k.c.c.i.ConsumerCoordinator (ConsumerCoordinator.java:851) | [Consumer clientId=consumer-test_group_id-5, groupId=test_group_id] Setting offset for partition foobarTopic-2 to the committed offset FetchPosition{offset=4, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:65036 (id: 0 rack: null)], epoch=0}}
```

This is where things get weird. Since partition 0 does not have an offset 5 it gets an out of range error.

```
2023-10-24 | 09:52:25.261 | INFO  | [Camel (camel-1) thread #3 - KafkaConsumer[foobarTopic]] | o.a.k.c.consumer.internals.Fetcher (Fetcher.java:1413) | [Consumer clientId=consumer-test_group_id-3, groupId=test_group_id] Fetch position FetchPosition{offset=5, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:65036 (id: 0 rack: null)], epoch=0}} is out of range for partition foobarTopic-0, resetting offset
```

This then sets the offset to earliest (offset 0) and starts to replay the messages. 

```
2023-10-24 | 09:52:25.264 | INFO  | [Camel (camel-1) thread #3 - KafkaConsumer[foobarTopic]] | o.a.k.c.c.i.SubscriptionState (SubscriptionState.java:398) | [Consumer clientId=consumer-test_group_id-3, groupId=test_group_id] Resetting offset for partition foobarTopic-0 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:65036 (id: 0 rack: null)], epoch=0}}.
2023-10-24 | 09:52:25.267 | INFO  | [Camel (camel-1) thread #3 - KafkaConsumer[foobarTopic]] | codesmell.test.CamelKafkaOffsetTest (CamelKafkaOffsetTest.java:147) | Message consumed from Kafka
Message consumed from foobarTopic
The Partion:Offset is 0:0
The Key is null
3
```
