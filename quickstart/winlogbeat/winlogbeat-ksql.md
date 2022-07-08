# ksqlDB for Winlogbeat

Once Winlogbeat events are in Confluent, they can be analyzed in real time using ksqlDB.

The following ksql query can be used to create a Stream from Winlogbeat events.  Note extensive use of the `STRUCT` keyword to read and parse nested JSON.

A tutorial on working with nested JSON in ksqlDB is available at [developer.confluent.io](https://developer.confluent.io/tutorials/working-with-nested-json/ksql.html "Working with nested JSON").

```sql
CREATE STREAM WINLOGBEAT (
    "@timestamp" VARCHAR,
    "@metadata" STRUCT<
        beat VARCHAR,
        type VARCHAR,
        version VARCHAR>,
    agent STRUCT<
        ephemeral_id VARCHAR,
        id VARCHAR,
        name VARCHAR,
        type VARCHAR,
        version VARCHAR>,
    winlog STRUCT<
        process STRUCT<
            pid INT,
            thread STRUCT<
                id INT>>,
        api VARCHAR,
        keywords ARRAY<STRING>,
        channel VARCHAR,
        record_id INT,
        task VARCHAR,
        opcode VARCHAR,
        user STRUCT<
            type VARCHAR,
            identifier VARCHAR,
            domain VARCHAR,
            name VARCHAR>,
        event_id VARCHAR,
        event_data VARCHAR,
        computer_name VARCHAR,
        provider_guid VARCHAR,
        provider_name VARCHAR>,
    event STRUCT<
        created VARCHAR,
        code VARCHAR,
        kind VARCHAR,
        provider VARCHAR,
        action VARCHAR>,
    log STRUCT<
        level VARCHAR>,
    message VARCHAR,
    host STRUCT<
        name VARCHAR,
        hostname VARCHAR,
        architecture VARCHAR,
        os STRUCT<
            platform VARCHAR,
            version VARCHAR,
            family VARCHAR,
            name VARCHAR,
            kernel VARCHAR,
            build VARCHAR,
            type VARCHAR>,
        id VARCHAR,
        ip ARRAY<VARCHAR>,
        mac ARRAY<VARCHAR>>,
    ecs STRUCT<
        version VARCHAR>) 
WITH (KAFKA_TOPIC='winlogbeat', VALUE_FORMAT='JSON');
```
Now that a Stream has been created from the topic, it can be queried using ksqlDB.

Looking at a [sample record](https://github.com/confluentinc/cyber/blob/bhayes-elastic/quickstart/winlogbeat/winlogbeat_ksql_event.json) in this Stream and you can see that there are scalar values in nested JSON, as well as JSON objects in nested JSON.

Since the `EVENT_DATA` field will always have different data in it, the original ksql query leaves this field as a `VARCHAR` data type instead of further trying to extract fields using `STRUCT`.

The JSON objects that are the values for this nested field can be queried using a combination of the `->` operator and the `EXTRACTJSONFIELD` function in ksqlDB.

For example, to filter out all Windows Event Logs with user events for user bert:


```sql
SELECT 
*
FROM WINLOGBEAT 
WHERE EXTRACTJSONFIELD(WINLOG->EVENT_DATA, '$.SubjectUserName') = 'bert'
EMIT CHANGES;

```

