# Public Transit Status with Apache Kafka

Streaming event pipeline with Apache Kafka and its ecosystem, using public data from the [Chicago Transit Authority](https://www.transitchicago.com/data/).


## Prerequisites

* Docker
* Python 3.7^

![Project Architecture](docs/images/diagram.png)

## Running and Testing

### Set Up

```bash
git clone https://github.com/dennisfrei/optimizing-public-transportation.git
cd optimizing-public-transportation
```

### Start Kafka and ecosystem

```bash
cd etc
docker-compose up
```

Docker compose will take a 3-5 minutes to start, depending on your hardware. Please be patient and wait for the docker-compose logs to slow down or stop before beginning the simulation.

### Producers

- Switch to producer directory
	```bash
	cd src/producers
	```
- Set up the database credentials
	```bash
	cp .env.example .env
	```

- Change env and install producer requirements
	```bash
	pip install -r requirements.txt
	```

- Start the simulation 
	```
	python simulation.py
	```

You can check if everything works as expected by checking the topics with the UI at `localhost:8085`

### Consumers

- Switch to producer directory
	```bash
	cd src/consumers
	```

- Change env and install consumer requirements
	```bash
	pip install -r requirements.txt
	```

- Start the Faust Streaming Application
	```bash
	faust -A faust_stream worker -l info
	```

- Start KSQL Query
	```bash
	python ksql.py
	```

- Start the server application
	```bash
	python server.py
	```


### Overview of service ports

| Service | Host URL | Docker URL | Username | Password |
| --- | --- | --- | --- | --- |
| Public Transit Status | [http://localhost:8888](http://localhost:8888) | n/a | ||
| Landoop Kafka Connect UI | [http://localhost:8084](http://localhost:8084) | http://connect-ui:8084 |
| Landoop Kafka Topics UI | [http://localhost:8085](http://localhost:8085) | http://topics-ui:8085 |
| Landoop Schema Registry UI | [http://localhost:8086](http://localhost:8086) | http://schema-registry-ui:8086 |
| Kafka | PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094 | PLAINTEXT://kafka0:9092,PLAINTEXT://kafka1:9093,PLAINTEXT://kafka2:9094 |
| REST Proxy | [http://localhost:8082](http://localhost:8082/) | http://rest-proxy:8082/ |
| Schema Registry | [http://localhost:8081](http://localhost:8081/ ) | http://schema-registry:8081/ |
| Kafka Connect | [http://localhost:8083](http://localhost:8083) | http://kafka-connect:8083 |
| KSQL | [http://localhost:8088](http://localhost:8088) | http://ksql:8088 |
| PostgreSQL | `jdbc:postgresql://localhost:5432/cta` | `jdbc:postgresql://postgres:5432/cta` | `cta_admin` | `chicago` |
