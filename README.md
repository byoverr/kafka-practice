# Kafka Practice

Здесь представлены практические работы по Kafka.

- [Практическая работа 1](https://github.com/byoverr/kafka-practice/practice1)
- [Практическая работа 2](https://github.com/byoverr/kafka-practice/practice2)
- - [Практическая работа 2](https://github.com/byoverr/kafka-practice/practice2-2)

## Практическая работа 1

### Задание

Создать 1 producer, который каждую секунду будет отправлять данные, и 2 consumer, один из них читает каждое сообщение, а другой читает батчами по 10 сообщений

### Запуск
Для запуска нужен Docker
```bash
git clone https://github.com/byoverr/kafka-practice.git

docker-compose -f ./practice1/docker-compose.yml up
```

### Структура:

- 3 брокера, настроены через KRaft
- 1 producer, который шлет каждую секунду сообщение
- 2 consumers, один из которых считывает каждое сообщение из my-topic, а другой каждые 10 сообщений

В Docker-compose создается по две реплики каждого приложения, заметил, что при одинаковых key producer кидает в одну партицию и поэтому вторая реплика consumer не будет читать сообщения пока первая реплика не выйдет из строя и не случится rebalance. При этом producer будут работать вдвоем. 

В качестве сериализатора/десериализотора использовал Protobuf


## Практическая работа 2. Часть 1

### Задание 

Создать систему обработки потоков сообщений с функциями блокировки пользователей и цензуры сообщений. Развернуть систему с использованием Docker-сompose и настройте необходимые топики Kafka.

Нужно добавить блокировку нежелательных пользователей и цензуру сообщений
### Запуск
Для запуска нужен Docker
```bash
git clone https://github.com/byoverr/kafka-practice.git

docker-compose -f ./practice2/docker-compose.yml up
```

### Структура:

- 3 брокера, настроены через KRaft
- 1 go приложение

Каждую секунду в топик `messages` кидается сообщение, каждые 20 секунд в `banned-words`, и раз в 15 секунд в `blocked-users`.
Поэтому самому можно кинуть, но необязательно, смотреть по Kafka-UI: http://localhost:8080


## Практическая работа 2. Часть 2

### Задание

Реализовать аналитику для системы обмена сообщениями с помощью ksqlDB.

Должны быть реализованы запросы:
- создание исходного потока;
- таблицы общего количества отправленных сообщений;
- таблицы с числом уникальных получателей для всех сообщений;
- таблицы с количеством сообщений, отправленных каждым пользователем;
- таблицы для агрегирования данных по каждому пользователю.

### Запуск
Для запуска нужен Docker
```bash
git clone https://github.com/byoverr/kafka-practice.git

docker-compose -f ./practice2-2/docker-compose.yml up
```

### Структура:

- 3 брокера, настроены через KRaft
- KSQLdb и KSQLdb-CLI 
- Kafka-UI

Все запросы написаны в sql файле, запустить CLI можно через команду `docker exec -it <ID контейнера> ksql http://ksqldb-server:8088
`


## Практическая работа 3

### Задание

Настроить Debezium Connector для передачи данных из базы данных PostgreSQL в Apache Kafka с использованием механизма Change Data Capture (CDC).

### Запуск
Для запуска нужен Docker
```bash
git clone https://github.com/byoverr/kafka-practice.git

docker-compose -f ./practice3/docker-compose.yml up

docker exec -it postgres psql -h 127.0.0.1 -U postgres-user -d customers // Создаем таблицы

curl localhost:8083/connector-plugins // проверим коннекторы

curl -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/pg-connector/config // конфигурируем коннектор

```

Запросы в psql:
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_name VARCHAR(100),
    quantity INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Структура:

- 3 брокера, настроены через KRaft
- Kafka Connect c Debezium
- PostgreSQL
- Grafana
- Prometheus
- Schema Registry



