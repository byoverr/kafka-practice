package main

import (
	"log"
	"math/rand/v2"
	"time"

	protoMessage "github.com/byoverr/kafka-practice/practice1/proto"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
)

const bootstrapServers = "kafka-0:9092"

var topic = "my-topic"

func main() {

	var p *kafka.Producer
	var err error

	for i := 0; i < 10; i++ {
		p, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": bootstrapServers,
			"acks":              "all",
			"retries":           3,
		})
		if err == nil {
			break
		}
		log.Println("Kafka not ready, retrying in 2s...")
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					var msg protoMessage.MyMessage
					if err := proto.Unmarshal(ev.Value, &msg); err != nil {
						log.Printf("Failed to decode message: %v", err)
					} else {
						log.Printf("Delivered message: value=%d timestamp=%d topic=%s partition=%d offset=%d",
							msg.Value,
							msg.Timestamp,
							*ev.TopicPartition.Topic,
							ev.TopicPartition.Partition,
							ev.TopicPartition.Offset)
					}

				}
			}
		}
	}()

	for {
		myMsg := &protoMessage.MyMessage{
			Value:     int32(rand.IntN(100)),
			Timestamp: time.Now().Unix(),
		}

		payload, err := proto.Marshal(myMsg)
		if err != nil {
			log.Printf("Failed to encode protobuf: %v", err)
			continue
		}

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            nil,
			Value:          payload,
		}

		err = p.Produce(msg, nil)
		if err != nil {
			log.Printf("Produce error: %v", err)
		}

		time.Sleep(1 * time.Second)
	}

}
