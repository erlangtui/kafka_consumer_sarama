package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"git.intra.weibo.com/adx/kafka_consumer_sarama"
)

func main() {
	c := &kafka_consumer_sarama.Config{
		Brokers:          []string{"10.182.29.28:19092", "10.182.29.28:29092", "10.182.29.28:39092"},
		Topics:           []string{"go_part_auto_discover_test1"},
		Group:            "go_part_auto_discover_test1_sarama",
		InitialOffset:    "oldest",
		RefreshFrequency: 10,
		ReturnErrors:     true,
		LogOut:           os.Stdout,
		// Assignor:         "sticky",
	}
	err := kafka_consumer_sarama.Start(context.Background(), c)
	if err != nil {
		log.Printf("Start error, err: %s\n", err.Error())
		return
	}
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	defer kafka_consumer_sarama.Close()
	cnt := 0
	for {
		select {
		case message, ok := <-kafka_consumer_sarama.Messages():
			if !ok {
				log.Println("msg chan has closed")
				return
			}
			cnt++
			log.Printf("topic: %s, group: %s, partition: %d, msg: %s", message.Topic, c.Group, message.Partition, string(message.Value))

		case err, ok := <-kafka_consumer_sarama.Errors():
			if !ok {
				log.Println("err chan has closed")
				return
			}
			log.Printf("err: %v\n", err)

		case <-sigterm:
			log.Println("terminated by signal")
			return
		}
		if cnt > 100 {
			log.Println("terminated by cnt")
			return
		}
	}
}
