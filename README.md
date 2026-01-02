# Kafka Practice

Здесь представлены практические работы по Kafka.

- [Практическая работа 1](https://github.com/byoverr/kafka-practice/practice1)


# Практическая работа 1

Для запуска нужен Docker
```bash
git clone https://github.com/byoverr/kafka-practice.git

docker-compose -f ./practice1/docker-compose.yml up
```

Структура:

- 3 брокера, настроены через KRaft
- 1 producer, который шлет каждую секунду сообщение
- 2 consumers, один из которых считывает каждое сообщение из my-topic, а другой каждые 10 сообщений

В Docker-compose создается по две реплики каждого приложения, заметил, что при одинаковых key producer кидает в одну партицию и поэтому вторая реплика consumer не будет читать сообщения пока первая реплика не выйдет из строя и не случится rebalance. При этом producer будут работать вдвоем. 

В качестве сериализатора/десериализотора использовал Protobuf
  
