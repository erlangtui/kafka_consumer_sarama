package main

import (
	"context"
	"log"
	"os"

	"git.intra.weibo.com/adx/kafka_consumer_sarama"
)

func main() {
	c := &kafka_consumer_sarama.KCSConfig{
		Brokers:          []string{"10.182.29.28:19092", "10.182.29.28:29092", "10.182.29.28:39092"},
		Topics:           []string{"go_part_auto_discover_test1"},
		Group:            "go_part_auto_discover_test1_sarama",
		InitialOffset:    "oldest",
		RefreshFrequency: 10,
		LogOut:           os.Stdout,
	}
	err := kafka_consumer_sarama.Start(context.Background(), c)
	if err != nil {
		log.Printf("Start error, err: %s\n", err.Error())
		return
	}
	for {
		message, ok := <-kafka_consumer_sarama.Messages()
		if !ok {
			return
		}
		log.Printf("topic: %s, group: %s, partition: %d, msg: %s", message.Topic, c.Group, message.Partition, string(message.Value))
	}
}
