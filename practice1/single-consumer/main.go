package main

import (
	"fmt"
	"log"
	"time"

	protoMessage "github.com/byoverr/kafka-practice/practice1/proto"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
)

const bootstrapServers = "kafka-0:9092"

var topic = "my-topic"

func main() {
	var c *kafka.Consumer
	var err error

	for i := 0; i < 10; i++ {
		c, err = kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  bootstrapServers,
			"group.id":           "consumer_group_single",
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": true,
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

	err = c.SubscribeTopics([]string{topic}, nil)

	if err != nil {
		log.Fatal(err)
	}

	run := true

	for run {
		ev, err := c.ReadMessage(time.Second)

		if err == nil {

			myMsg := &protoMessage.MyMessage{}
			err := proto.Unmarshal(ev.Value, myMsg)
			if err != nil {
				log.Printf("Failed to decode: %v", err)
			}

			log.Printf("Received: %d at %d, TOPIC: %s", myMsg.Value, myMsg.Timestamp, ev.TopicPartition)
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, ev)
		}
	}

	c.Close()

}
