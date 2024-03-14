package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)
type OrderPlacer struct{
    producer *kafka.Producer
    topic string
    deliverych chan kafka.Event
}
func NewPlaceOrder (p *kafka.Producer, topic string) *OrderPlacer{
    return &OrderPlacer{
        producer: p,
        topic: topic,
        deliverych: make(chan kafka.Event, 10000),
    }
}

func (op *OrderPlacer) placeOrder(orderMessage string, size int) error{
    message := fmt.Sprintf("%s - %d", orderMessage, size)
    payload := []byte(message)

    err := op.producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{
            Topic: &op.topic,
            Partition: kafka.PartitionAny,
        },
        Value: payload,
    },
    op.deliverych,
    
    )
    if err != nil{
        log.Fatal(err)
    }
    <-op.deliverych
    fmt.Printf("Sending Message to the topic: %s  Message: %s\n", op.topic,message)
    return nil
}


func main() {
	topic := "KAFAKASTUDY"

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "something",
		"acks":              "all",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

    op := NewPlaceOrder(p,topic) 

    for i := 0; i < 1000 ; i++{
        if err :=  op.placeOrder("Message Number", i+1); err != nil{
            log.Fatal(err)
        }
        time.Sleep(time.Second*3)
    }
}

