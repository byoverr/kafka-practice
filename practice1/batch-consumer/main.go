package main

import (
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
			"group.id":           "consumer_group_batch",
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": false,
			"fetch.min.bytes":    100,
			"fetch.wait.max.ms":  500,
		})
		if err == nil {
			break
		}
		log.Println("Kafka not ready, retrying in 2s...")
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}

	err = c.SubscribeTopics([]string{topic}, nil)

	if err != nil {
		log.Fatal(err)
	}

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	batch := make([]*kafka.Message, 0, 10)

	for run {
		ev, err := c.ReadMessage(time.Second)
		if err == nil {
			batch = append(batch, ev)
		} else {
			kerr, ok := err.(kafka.Error)
			if !ok || kerr.Code() != kafka.ErrTimedOut {
				log.Printf("Consumer error: %v\n", err)
				continue
			}
		}

		if len(batch) >= 10 {
			for _, ev := range batch {
				myMsg := &protoMessage.MyMessage{}
				err := proto.Unmarshal(ev.Value, myMsg)
				if err != nil {
					log.Printf("Failed to decode: %v", err)
				}

				log.Printf("Received: %d at %d, TOPIC: %s", myMsg.Value, myMsg.Timestamp, ev.TopicPartition)

			}
			_, err = c.Commit()
			if err != nil {
				log.Printf("Commit error: %v", err)
			}
			batch = batch[:0]
		}
	}

	c.Close()

}
