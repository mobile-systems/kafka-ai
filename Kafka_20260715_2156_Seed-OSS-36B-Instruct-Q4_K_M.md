<pre>
alexa@DellPro MINGW64 ~/workspace/kafka-ai (master)
$ llama-cli -ngl auto -t 20 -c 64000 -m ~/.lmstudio/models/lmstudio-community/Seed-OSS-36B-Instruct-GGUF/Seed-OSS-36B-Instruct-Q4_K_M.gguf

Loading model...


▄▄ ▄▄
██ ██
██ ██  ▀▀█▄ ███▄███▄  ▀▀█▄    ▄████ ████▄ ████▄
██ ██ ▄█▀██ ██ ██ ██ ▄█▀██    ██    ██ ██ ██ ██
██ ██ ▀█▄██ ██ ██ ██ ▀█▄██ ██ ▀████ ████▀ ████▀
                                    ██    ██
                                    ▀▀    ▀▀

build      : b9893-6f8895fee
model      : C:/Users/alexa/.lmstudio/models/lmstudio-community/Seed-OSS-36B-Instruct-GGUF/Seed-OSS-36B-Instruct-Q4_K_M.gguf
ftype      : Q4_K - Medium
modalities : text

available commands:
  /exit or Ctrl+C     stop or exit
  /regen              regenerate the last response
  /clear              clear the chat history
  /read <file>        add a text file
  /glob <pattern>     add text files using globbing pattern
</pre>


> Подготовь пожалуйста конфигурационные файлы отказоустойчивого кластера Kafka для нескольких нод на отдельных серверах работающих в Docker и с использованием Zookeeper

[Start thinking]

Got it, let's tackle this problem: the user wants configuration files for a fault-tolerant Kafka cluster with multiple nodes in Docker, using Zookeeper. First, I need to remember how Kafka and Zookeeper work together—Kafka relies on Zookeeper for metadata, so we need a Zookeeper ensemble too, right? Wait, the user mentioned "several nodes" for Kafka, but Zookeeper should also be a cluster for fault tolerance, not a single node. Because a single Zookeeper is a single point of failure.

First, let's outline the components:

1. **Zookeeper Ensemble**: Typically 3, 5, or 7 nodes for odd number to handle quorum. Let's go with 3 nodes for simplicity—easy to set up, fault-tolerant (can lose 1 node).

2. **Kafka Brokers**: Each broker is a node. Let's say 3 brokers too, for replication (each topic will have replicas across brokers). Replication factor should be at least 3 if we have 3 brokers to ensure no data loss on any single broker failure.

Next, Docker setup: We'll need Docker Compose since it's for multiple containers across servers? Wait, no—Docker Compose is for a single host, but the user said "на отдельных серверах" (on separate servers). Oh, right, so maybe they want a way to deploy across multiple servers, but Docker Compose is local. Alternatively, maybe they're okay with a Compose file as a template, and then scaling across servers by adjusting the configuration. Or maybe using Docker Swarm? But the user didn't mention Swarm, just Docker. Let's assume they might be using multiple Docker hosts, so we'll provide per-node configurations (Zookeeper and Kafka) that can be adapted for each server, plus a Docker Compose example for local testing (since that's common), but note that for production across servers, they need to use hostnames/IPs of each server.

First, Zookeeper configuration: Each Zookeeper node needs a `zoo.cfg` with:

- `clientPort`: The port Zookeeper listens on (2181 by default).
- `dataDir`: Directory for data.
- `tickTime`: Basic time unit in ms (2000 by default).
- `initLimit`: Time to connect to peers (tickTime * 5).
- `syncLimit`: Time to sync with leader (tickTime * 2).
- `server.X`: List of ensemble members, where X is the server ID (1,2,3 for 3 nodes). Each entry is `server.X=host:port1:port2`, where port1 is for leader election, port2 for quorum communication.

For Kafka brokers: Each broker needs a `server.properties` with:

- `broker.id`: Unique ID for each broker (0,1,2 for 3 brokers).
- `listeners`: List of interfaces Kafka listens on—important to use the correct hostname/IP for the server (e.g., `PLAINTEXT://kafka1:9092` for broker 0 on server kafka1).
- `advertised.listeners`: The address brokers advertise to clients—must be the public/network IP/clients can reach (critical, otherwise clients can't connect).
- `zookeeper.connect`: Comma-separated list of Zookeeper ensemble nodes (e.g., `zoo1:2181,zoo2:2181,zoo3:2181`).
- `num.partitions`: Default number of partitions per topic (e.g., 3).
- `default.replication.factor`: Default replication factor (should be <= number of brokers, e.g., 3).
- `offsets.topic.replication.factor`: Replication factor for the internal offsets topic (should be same as default, 3).
- `transaction.state.log.replication.factor`: For transactions, same as above.
- `transaction.state.log.min.isr`: Minimum in-sync replicas for transaction logs (2 is safe).
- `log.dirs`: Directory for Kafka logs (data storage).
- `delete.topic.enable`: Whether to allow topic deletion (true for development, maybe false for production, but common to enable).
- `num.network.threads`: Network threads (e.g., 3).
- `num.io.threads`: IO threads (e.g., 8).
- `socket.send.buffer.bytes`: Send buffer (102400).
- `socket.receive.buffer.bytes`: Receive buffer (102400).
- `socket.request.max.bytes`: Max request size (104857600).
- `log.retention.hours`: How long to keep logs (e.g., 168 for a week).
- `log.segment.bytes`: Size of each log segment (1073741824, 1GB).
- `log.retention.check.interval.ms`: Check interval for retention (300000, 5 mins).
- `num.recovery.threads.per.data.dir`: Threads for log recovery (e.g., 1).

Now, for Docker: Each Zookeeper and Kafka node will be a container. For multiple servers, each server will run either a Zookeeper node or a Kafka broker (or both, but best practice to separate for resource isolation). Let's assume 3 servers:

- **Server 1**: Zookeeper 1 + Kafka Broker 0
- **Server 2**: Zookeeper 2 + Kafka Broker 1
- **Server 3**: Zookeeper 3 + Kafka Broker 2

This way, each server has one Zookeeper and one Kafka node, which is a common setup for small-to-medium clusters.

First, let's create Zookeeper configuration for each node. Let's name the Zookeeper nodes `zoo1`, `zoo2`, `zoo3` with hostnames/IPs `zoo1.example.com`, `zoo2.example.com`, `zoo3.example.com` (user should replace with their actual IPs/domain names).

For Zookeeper node 1 (`zoo1`):

`zoo1.cfg`:
```
tickTime=2000
initLimit=5
syncLimit=2
dataDir=/var/lib/zookeeper/data
dataLogDir=/var/lib/zookeeper/log
clientPort=2181
server.1=zoo1.example.com:2888:3888
server.2=zoo2.example.com:2888:3888
server.3=zoo3.example.com:2888:3888
```

Note: `dataDir` is for the myid file (which contains the server ID), `dataLogDir` is for transaction logs (separating them is better for performance).

Also, need to create a `myid` file in `dataDir` for each Zookeeper node: for zoo1, `myid` contains `1`; zoo2 contains `2`; zoo3 contains `3`.

Now, Kafka broker 0 on server 1 (`kafka1.example.com`):

`server0.properties`:
```
# Broker ID (unique per broker)
broker.id=0

# Listeners: PLAINTEXT for unencrypted, can add SSL/SASL if needed
listeners=PLAINTEXT://:9092

# Advertised listeners: the address clients and other brokers use to connect to this broker
# Use the server's public/network IP or hostname here
advertised.listeners=PLAINTEXT://kafka1.example.com:9092

# Zookeeper ensemble connection
zookeeper.connect=zoo1.example.com:2181,zoo2.example.com:2181,zoo3.example.com:2181

# Replication settings
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

# Log storage
log.dirs=/var/lib/kafka/data

# Topic deletion (optional, enable for development)
delete.topic.enable=true

# Network and I/O settings
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Log retention
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

# Recovery settings
num.recovery.threads.per.data.dir=1

# Optional: Enable Kafka metrics (JMX)
# jmx.port=9999
# jmx.hostname=kafka1.example.com
```

Similarly, for Kafka broker 1 on server 2 (`kafka2.example.com`), change `broker.id=1`, `advertised.listeners=PLAINTEXT://kafka2.example.com:9092`.

For Kafka broker 2 on server 3 (`kafka3.example.com`), `broker.id=2`, `advertised.listeners=PLAINTEXT://kafka3.example.com:9092`.

Now, Dockerfiles: Do we need Dockerfiles? Or can we use official images with volume mounts for configs? The official Kafka and Zookeeper images (confluentinc/cp-zookeeper, confluentinc/cp-kafka) are good because they have all dependencies and are commonly used.

So for Zookeeper, using `confluentinc/cp-zookeeper:7.3.0` (latest as of now), we can mount the `zoo.cfg` and `myid` file.

For Kafka, `confluentinc/cp-kafka:7.3.0`, mount the `server.properties`.

Now, Docker Compose example for local testing (if all nodes are on one host, but user said separate servers—so this is for development, but useful as a template). For local, we can use container names as hostnames, but in production, replace with real IPs.

`docker-compose.yml` (local test cluster with 3 Zookeeper and 3 Kafka nodes):

```yaml
version: '3.8'

services:
  # Zookeeper Ensemble
  zookeeper-1:
    image: confluentinc/cp-zookeeper:7.3.0
    container_name: zookeeper-1
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SERVER_ID: 1
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888
    volumes:
      - zookeeper-1-data:/var/lib/zookeeper/data
      - zookeeper-1-log:/var/lib/zookeeper/log

  zookeeper-2:
    image: confluentinc/cp-zookeeper:7.3.0
    container_name: zookeeper-2
    ports:
      - "2182:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SERVER_ID: 2
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888
    volumes:
      - zookeeper-2-data:/var/lib/zookeeper/data
      - zookeeper-2-log:/var/lib/zookeeper/log

  zookeeper-3:
    image: confluentinc/cp-zookeeper:7.3.0
    container_name: zookeeper-3
    ports:
      - "2183:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SERVER_ID: 3
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888
    volumes:
      - zookeeper-3-data:/var/lib/zookeeper/data
      - zookeeper-3-log:/var/lib/zookeeper/log

  # Kafka Brokers
  kafka-1:
    image: confluentinc/cp-kafka:7.3.0
    container_name: kafka-1
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 0
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:9092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
    volumes:
      - kafka-1-data:/var/lib/kafka/data
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3

  kafka-2:
    image: confluentinc/cp-kafka:7.3.0
    container_name: kafka-2
    ports:
      - "9093:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:9092,PLAINTEXT_HOST://localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
    volumes:
      - kafka-2-data:/var/lib/kafka/data
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3

  kafka-3:
    image: confluentinc/cp-kafka:7.3.0
    container_name: kafka-3
    ports:
      - "9094:9092"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:9092,PLAINTEXT_HOST://localhost:9094
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
    volumes:
      - kafka-3-data:/var/lib/kafka/data
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3

volumes:
  zookeeper-1-data:
  zookeeper-1-log:
  zookeeper-2-data:
  zookeeper-2-log:
  zookeeper-3-data:
  zookeeper-3-log:
  kafka-1-data:
  kafka-2-data:
  kafka-3-data:
```

Notes on the Docker Compose:

- Uses Confluent images which are based on Apache Kafka but have more tools (like Kafka CLI).
- For each Zookeeper, we set `ZOOKEEPER_SERVER_ID` and `ZOOKEEPER_SERVERS` (the ensemble members). The `ZOOKEEPER_CLIENT_PORT` is 2181 for all.
- For each Kafka broker, `KAFKA_BROKER_ID` is unique. `KAFKA_ZOOKEEPER_CONNECT` points to the Zookeeper ensemble. `KAFKA_ADVERTISED_LISTENERS` is important: in local, we have two listeners—one for container-to-container communication (`PLAINTEXT://kafka-1:9092`) and one for host access (`PLAINTEXT_HOST://localhost:9092`). The port mapping (`9092:9092`) maps the host port to the container's 9092.
- `depends_on` ensures Zookeeper starts before Kafka.
- Volumes persist data between container restarts.

Now, for production across separate servers:

Each server will need to run either Zookeeper or Kafka (or both). Let's take Server 1 (zoo1, kafka0):

On Server 1, create a directory for Zookeeper configs: `/etc/zookeeper/` with `zoo.cfg` and `myid` (content 1). Then run Zookeeper container:

```bash
docker run -d \
  --name zookeeper-1 \
  -p 2181:2181 \
  -p 2888:2888 \
  -p 3888:3888 \
  -v /etc/zookeeper/zoo.cfg:/etc/zookeeper/zoo.cfg \
  -v /etc/zookeeper/myid:/var/lib/zookeeper/data/myid \
  -v /var/lib/zookeeper/data:/var/lib/zookeeper/data \
  -v /var/lib/zookeeper/log:/var/lib/zookeeper/log \
  confluentinc/cp-zookeeper:7.3.0
```

Then run Kafka broker 0 on Server 1:

Create `/etc/kafka/server.properties` with the config we had earlier (broker.id=0, advertised.listeners=kafka1.example.com:9092, etc.). Then:

```bash
docker run -d \
  --name kafka-0 \
  -p 9092:9092 \
  -v /etc/kafka/server.properties:/etc/kafka/server.properties \
  -v /var/lib/kafka/data:/var/lib/kafka/data \
  --link zookeeper-1:zookeeper-1 \ # Optional, if using old networking, else use --network
  confluentinc/cp-kafka:7.3.0
```

Repeat this on Server 2 (zoo2, kafka1) and Server 3 (zoo3, kafka2), changing the `myid` (2,3), `broker.id` (1,2), and `advertised.listeners` (kafka2.example.com:9092, kafka3.example.com:9092).

Important considerations for fault tolerance:

- **Zookeeper Ensemble Size**: Odd number (3,5,7) to avoid split-brain. 3 is minimum for fault tolerance (can lose 1 node).
- **Kafka Replication**: `default.replication.factor` = number of brokers (3) so that each partition has a replica on every broker—no data loss if any broker fails. `min.insync.replicas` = 2 means at least 2 replicas must acknowledge writes, ensuring durability.
- **Networking**: Ensure all servers can reach each other on the required ports (2181 for Zookeeper, 9092 for Kafka, 2888/3888 for Zookeeper quorum). Firewalls should allow these ports between servers.
- **Hostnames/IPs**: Use stable hostnames or IPs (not dynamic) for Zookeeper and Kafka nodes—otherwise, the cluster will break if IPs change.
- **Persistence**: Use volumes to persist Zookeeper and Kafka data—otherwise, data is lost when containers restart.
- **Monitoring**: Add monitoring (e.g., Prometheus + Grafana, Confluent Control Center) to track broker health, replication lag, etc.
- **Security**: In production, enable encryption (SSL/TLS) for inter-broker and client communication, and authentication (SASL) to prevent unauthorized access. The example uses PLAINTEXT, which is not secure for production.

Optional: Confluent Control Center for monitoring. Add a service to Docker Compose:

```yaml
  control-center:
    image: confluentinc/cp-enterprise-control-center:7.3.0
    container_name: control-center
    ports:
      - "9021:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9092,kafka-3:9092
      CONTROL_CENTER_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      CONTROL_CENTER_REPLICATION_FACTOR: 3
      CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 3
      CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS: 3
      CONFLUENT_METRICS_TOPIC_REPLICATION: 3
      PORT: 9021
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
```

This runs Control Center on port 9021, which provides a web UI to monitor the cluster.

Testing the cluster:

1. Start Zookeeper ensemble first, then Kafka brokers.
2. Create a topic: `kafka-topics --create --topic test-topic --bootstrap-server kafka1.example.com:9092 --replication-factor 3 --partitions 3`.
3. Produce messages: `kafka-console-producer --topic test-topic --bootstrap-server kafka1.example.com:9092`.
4. Consume messages: `kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server kafka1.example.com:9092`.
5. Test fault tolerance: Stop one Kafka broker (e.g., `docker stop kafka-1`), then produce/consume—should still work because replicas are on other brokers. Stop one Zookeeper node—ensemble still works (quorum is 2 out of 3).

Troubleshooting tips:

- If Kafka can't connect to Zookeeper, check `zookeeper.connect` in server.properties—ensure it matches the Zookeeper ensemble addresses.
- If clients can't connect to Kafka, check `advertised.listeners`—it must be the address clients can reach (e.g., public IP, not localhost if clients are remote).
- Check logs: `docker logs kafka-0` to see if there are connection errors.
- Use `kafka-topics --describe --topic test-topic` to verify replication status—each partition should have replicas on different brokers.

Final notes:

- The user should replace placeholder hostnames (like `zoo1.example.com`, `kafka1.example.com`) with their actual server IPs or domain names.
- For production, use official Apache Kafka images or Confluent images with the latest version (check https://hub.docker.com/r/confluentinc/cp-kafka for latest tags).
- Consider using Kubernetes for orchestration across multiple servers if the cluster is large—Docker Compose is for development/testing.
- Enable JMX if needed for monitoring, but secure it properly (don't expose to public network).

[End thinking]

Чтобы настроить отказоустойчивый кластер Kafka с несколькими нодами в Docker (с использованием ZooKeeper), нам понадобятся:

1. **Энсембль ZooKeeper** (нечетное количество нод, например 3, для гарантии квора).
2. **Кластер Kafka-брокеров** (несколько нод, каждая с уникальным идентификатором).
3. **Конфигурационные файлы** для ZooKeeper и Kafka, а также скрипты запуска в Docker.


## Предварительные требования
- Доступ к 3+ серверам (или виртуальным машинам) с Docker.
- Статические IP-адреса или доменные имена для каждого сервера (например: `zoo1.kafka.local`, `kafka1.kafka.local`).
- Открытые порты:
  - ZooKeeper: 2181 (клиент), 2888 (выбор лидера), 3888 ( кворум).
  - Kafka: 9092 (обмен данными между брокерами и клиентами).


## 1. Конфигурация ZooKeeper-енсембля
Создайте для каждой ноды ZooKeeper файл `zoo.cfg` и `myid` (содержит идентификатор ноды).

### Пример для 3-х нод:
#### Нода 1 (zoo1.kafka.local)
- `zoo1.cfg`:
  ```properties
  tickTime=2000
  initLimit=5
  syncLimit=2
  dataDir=/var/lib/zookeeper/data
  dataLogDir=/var/lib/zookeeper/log
  clientPort=2181
  # Список成员 ensembles (server.ID=host:port_election:port_quorum)
  server.1=zoo1.kafka.local:2888:3888
  server.2=zoo2.kafka.local:2888:3888
  server.3=zoo3.kafka.local:2888:3888
  ```
- `myid` (в `dataDir`):
  ```
  1
  ```

#### Нода 2 (zoo2.kafka.local)
- `zoo2.cfg` (和成一样, кроме server.ID).
- `myid`: `2`.

#### Нода 3 (zoo3.kafka.local)
- `zoo3.cfg` (和成一样).
- `myid`: `3`.


## 2. Конфигурация Kafka-брокеров
Каждый брокер требует уникального `broker.id` и настройки подключения к ZooKeeper-енсembles.

### Пример для 3-х брокеров:
#### Брокер 0 (kafka1.kafka.local)
- `server0.properties`:
  ```properties
  # Уникальный ID брокера (0,1,2 для 3-х нод)
  broker.id=0
  # Порт для подключения клиентов и других брокеров
  listeners=PLAINTEXT://:9092
  # Адрес, which брокер объявляет клиентам (используйте публичный IP/домен)
  advertised.listeners=PLAINTEXT://kafka1.kafka.local:9092
  # Подключение к ZooKeeper-енсembles
  zookeeper.connect=zoo1.kafka.local:2181,zoo2.kafka.local:2181,zoo3.kafka.local:2181
  # Репликация: фактор репликации (равен количеству брокеров для отказоустойчивости)
  default.replication.factor=3
  # Минимальное количество в-sync-реплик (для гарантии durability)
  min.insync.replicas=2
  # Репликация внутренних тем Kafka (offsets, транзакций)
  offsets.topic.replication.factor=3
  transaction.state.log.replication.factor=3
  transaction.state.log.min.isr=2
  # Путь для хранения логов Kafka
  log.dirs=/var/lib/kafka/data
  # Включение удаления тем (для разработки)
  delete.topic.enable=true
  # Настройки сети и ввода-вывода
  num.network.threads=3
  num.io.threads=8
  socket.send.buffer.bytes=102400
  socket.receive.buffer.bytes=102400
  socket.request.max.bytes=104857600
  # Сохранение логов (1 неделя)
  log.retention.hours=168
  log.segment.bytes=1073741824  # 1 ГБ сегмента
  log.retention.check.interval.ms=300000  # Проверка каждые 5 минут
  ```

#### Брокер 1 (kafka2.kafka.local)
- Измените `broker.id=1` и `advertised.listeners=PLAINTEXT://kafka2.kafka.local:9092`.

#### Брокер 2 (kafka3.kafka.local)
- Измените `broker.id=2` и `advertised.listeners=PLAINTEXT://kafka3.kafka.local:9092`.


## 3. Запуск в Docker (по серверу)
На каждом сервере запускаем одну ноду ZooKeeper и один брокер Kafka (или разделите роли, если требуется лучшая изоляция ресурсов).

### Шаг 1: Создайте каталоги для данных
На сервере с ZooKeeper 1 и Kafka 0:
```bash
mkdir -p /var/lib/zookeeper/data /var/lib/zookeeper/log /var/lib/kafka/data
```
Скопируйте `zoo1.cfg` и `myid` в `/etc/zookeeper/`, а `server0.properties` в `/etc/kafka/`.

### Шаг 2: Запуск контейнеров ZooKeeper и Kafka
#### ZooKeeper 1:
```bash
docker run -d \
  --name zookeeper-1 \
  -p 2181:2181 \
  -p 2888:2888 \
  -p 3888:3888 \
  -v /etc/zookeeper/zoo1.cfg:/etc/zookeeper/zoo.cfg \
  -v /etc/zookeeper/myid:/var/lib/zookeeper/data/myid \
  -v /var/lib/zookeeper/data:/var/lib/zookeeper/data \
  -v /var/lib/zookeeper/log:/var/lib/zookeeper/log \
  confluentinc/cp-zookeeper:7.3.0
```

#### Kafka Брокер 0:
```bash
docker run -d \
  --name kafka-0 \
  -p 9092:9092 \
  -v /etc/kafka/server0.properties:/etc/kafka/server.properties \
  -v /var/lib/kafka/data:/var/lib/kafka/data \
  --network host \  # Для доступа к ZooKeeper через hostname сервера
  confluentinc/cp-kafka:7.3.0
```

### Повторяйте для других серверов:
- Сервер 2: ZooKeeper 2 + Kafka Брокер 1.
- Сервер 3: ZooKeeper 3 + Kafka Брокер 2.


## 4. Docker Compose (для локальной разработки)
Если вы тестируете кластер на одной машине, используйте `docker-compose.yml` (замените hostnames на `host.docker.internal` или локальные IP).

```yaml
version: '3.8'
services:
  # ZooKeeper Ensemble
  zookeeper-1:
    image: confluentinc/cp-zookeeper:7.3.0
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_SERVER_ID: 1
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888

  zookeeper-2:
    image: confluentinc/cp-zookeeper:7.3.0
    ports:
      - "2182:2181"
    environment:
      ZOOKEEPER_SERVER_ID: 2
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888

  zookeeper-3:
    image: confluentinc/cp-zookeeper:7.3.0
    ports:
      - "2183:2181"
    environment:
      ZOOKEEPER_SERVER_ID: 3
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888

  # Kafka Brokers
  kafka-1:
    image: confluentinc/cp-kafka:7.3.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 0
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3

  kafka-2:
    image: confluentinc/cp-kafka:7.3.0
    ports:
      - "9093:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3

  kafka-3:
    image: confluentinc/cp-kafka:7.3.0
    ports:
      - "9094:9092"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3
```


## 5. Проверка кластера
1. **Создание темы**:
   ```bash
   docker exec kafka-0 kafka-topics --create --topic test-topic --bootstrap-server kafka-1:9092 --replication-factor 3 --partitions 3
   ```

2. **Проверка репликации**:
   ```bash
   docker exec kafka-0 kafka-topics --describe --topic test-topic --bootstrap-server kafka-1:9092
   ```
   Вы должны увидеть, что каждая партиция имеет реплики на разных брокерах.

3. **Производство/소비 сообщений**:
   - Producer: `docker exec kafka-0 kafka-console-producer --topic test-topic --bootstrap-server kafka-1:9092`
   - Consumer: `docker exec kafka-0 kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server kafka-1:9092`


## Ключевые аспекты отказоустойчивости
- **ZooKeeper-енсембль**: Нечетное количество нод (3) гарантирует, что кластер выдерживает отказ одной ноды.
- **Репликация Kafka**: `default.replication.factor=3` ensures that each partition has a copy on every broker—no data loss if any broker fails.
- `min.insync.replicas=2`: Требовать подтверждения от двух реплик при записи, чтобы избежать потерь данных при сбое одного брокера.
- **Персистентное хранилище**: Volumes в Docker сохраняют данные после перезапуска контейнеров.


## Рекомендации для productions
- **Безопасность**: Включите шифрование (SSL/TLS) и аутентификацию (SASL) для предотвращения несанкционированного доступа.
- **Мониторинг**: Используйте Confluent Control Center или Prometheus+Grafana для отслеживания health кластера.
- **Обновления**: Регулярно обновляйте образы Docker (confluentinc/cp-kafka) до последних версий.
- **Дополнительные ноды**: Для крупных кластеров добавляйте больше ZooKeeper-нод (5-7) и Kafka-брокеров, чтобы распределить нагрузку.

Теперь ваш кластер Kafka готов к работе с высокой доступностью и отказоустойчивостью!

[ Prompt: 2.1 t/s | Generation: 0.6 t/s ]
