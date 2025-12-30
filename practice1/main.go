package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const topicWaitingTime = 60 * time.Second

func main() {

	if len(os.Args) != 5 {
		log.Fatalf(
			"Пример использования: %s <bootstrap-servers> <topic> <partition-count> <replication-factor>\n",
			os.Args[0])
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]
	numParts, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("Ошибка в количестве партиций: %s: %v\n", os.Args[3], err)
	}
	replicationFactor, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Fatalf("Ошибка в факторе репликации : %s: %v\n", os.Args[4], err)
	}

	// Cоздаём новый админский клиент.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		log.Fatalf("Ошибка создания админского клиента: %s\n", err)
	}
	defer a.Close()
	// min.insync.replicas устанавливаем равной 2
	topicSepcConfig := make(map[string]string)
	topicSepcConfig["min.insync.replicas"] = "2"

	results, err := a.CreateTopics(
		context.Background(),
		// Можно одновременно создать несколько топиков, указав несколько структур TopicSpecification
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     numParts,
			ReplicationFactor: replicationFactor,
			Config:            topicSepcConfig,
		}},
		// Устанавливаем опцию админского клиента — ждать не более topicWaitingTime
		kafka.SetAdminOperationTimeout(topicWaitingTime))
	if err != nil {
		log.Fatalf("Ошибка создания топика: %s\n", err)
	}

	// Результат (тип TopicResult) содержит информацию о результате операции создания
	//топика. Если бы мы создавали несколько топиков, то получили бы информацию по каждому из них.
	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094",
		"acks":              "all",
		"retries":           3,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Создаём новое сообщение
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
		},
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	// Канал доставки событий (информации об отправленном сообщении)
	deliveryChan := make(chan kafka.Event)

	// Отправляем сообщение
	err = p.Produce(msg, deliveryChan)
	if err != nil {
		log.Fatal(err)
	}

	e := <-deliveryChan

	// Приводим Events к типу *kafka.Message
	m := e.(*kafka.Message)

	// Если возникла ошибка доставки сообщения
	if m.TopicPartition.Error != nil {
		fmt.Printf("Ошибка доставки сообщения: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Сообщение отправлено в топик %s [%d] офсет %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	// Закрываем продюсера
	p.Close()
	// Закрываем канал доставки событий
	close(deliveryChan)
}
