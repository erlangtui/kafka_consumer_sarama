package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"git.intra.weibo.com/adx/kafka_consumer_sarama"
)

func main() {
	c := &kafka_consumer_sarama.Config{
		Brokers:          []string{"10.182.29.28:19092", "10.182.29.28:29092", "10.182.29.28:39092"},
		Topics:           []string{"go_part_auto_discover_test8"},
		Group:            "go_part_auto_discover_test1_sarama_2",
		InitialOffset:    "oldest",
		RefreshFrequency: 10,
		ReturnErrors:     true,
		LogOut:           os.Stdout,
		Version:          "0.10.2.1",
		// Assignor:         "sticky",
		// MsgChanCap:       -1,
	}
	cli, err := kafka_consumer_sarama.NewConsumer(c)
	if err != nil {
		log.Printf("Start error, err: %s\n", err.Error())
		return
	}
	go cli.Start()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	defer func() {
		cli.Close()
		log.Println("main finished")
	}()

	for {
		select {
		case message, ok := <-cli.Messages():
			if !ok {
				log.Println("msg chan has closed")
				return
			}
			log.Printf("topic: %s, group: %s, partition: %d, msg: %s", message.Topic, c.Group, message.Partition, string(message.Value))

		case err, ok := <-cli.Errors():
			if !ok {
				log.Println("err chan has closed")
				return
			}
			log.Printf("err: %v\n", err)

		case <-sigterm:
			log.Println("terminated by signal")
			return
		}
		// fmt.Println(111)
	}

}
