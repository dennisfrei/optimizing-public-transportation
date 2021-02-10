# Cheatsheet

## Kafka Topics

### list
```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

### Delete
- Single Topic
```bash
kafka-topics --bootstrap-server localhost:9092 --delete --topic myTopic
```
- Wildcard
```bash
kafka-topics --bootstrap-server localhost:9092 --delete --topic "myTopic.*"
```

k
## Kafka Console Consumer

### Consume from topic
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic myTopic
```

## KSQL

### Show Tables
```sql
SHOW TABLES;
```

### Show Queries
```sql
SHOW QUERIES;
```

### Terminate Query
```sql
TERMINATE QUERY <query>;
```

### DROP TABLE
```sql
DROP TABLE <name>;
```
