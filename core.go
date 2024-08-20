package kafka_consumer_sarama

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

var (
	data = &innerData{
		msgChanCap: 10000,
	}
)

// Messages 获取消息管道，调用法从其中获取消息
func Messages() <-chan *sarama.ConsumerMessage {
	// TODO 调用该方法后，调用方还是需要导入 sarama 包，又涉及到包版本问题；如果重新写 sarama.ConsumerMessage 结构则存在数据拷贝影响性能
	return data.msgChan
}

// Errors 配置中开启了 ReturnErrors 后才会有消息，默认是直接写入 LogOut 配置中
func Errors() <-chan error {
	return data.errChan
}

// Start 调用改函数后开始从kafka消费消息，调用方应该从 Messages 函数中获取消息
func Start(pCtx context.Context, c *Config) error {
	if c == nil {
		return errors.New("config is nil")
	}
	if len(c.Topics) == 0 {
		return errors.New("config topic is empty")
	}
	if c.Group == "" {
		return errors.New("config group is empty")
	}
	if c.SASLEnable && c.SASLUser == "" {
		return errors.New("config sasl user is empty")
	}
	version := sarama.V0_10_2_1
	if c.Version != "" {
		var err error
		version, err = sarama.ParseKafkaVersion(c.Version)
		if err != nil {
			return err
		}
	}

	var assignor sarama.BalanceStrategy
	switch c.Assignor {
	case "sticky":
		assignor = sarama.BalanceStrategySticky
	case "roundrobin":
		assignor = sarama.BalanceStrategyRoundRobin
	case "range", "":
		assignor = sarama.BalanceStrategyRange
	default:
		return errors.New("config assignor unknown, should be one of range, roundrobin and sticky")
	}

	var initial int64
	switch c.InitialOffset {
	case "oldest":
		initial = sarama.OffsetOldest
	case "newest", "":
		initial = sarama.OffsetNewest
	default:
		return errors.New("config initial offset unknown, should be oldest or newest")
	}

	cfg := sarama.NewConfig()
	cfg.Version = version
	cfg.Consumer.Offsets.Initial = initial
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{assignor}
	if c.SASLEnable {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = c.SASLUser
		cfg.Net.SASL.Password = c.SASLPassword
	}
	if c.RefreshFrequency > 0 {
		cfg.Metadata.RefreshFrequency = time.Duration(c.RefreshFrequency) * time.Second
	} else {
		cfg.Metadata.RefreshFrequency = time.Minute
	}
	if c.DisableAutoCommit {
		cfg.Consumer.Offsets.AutoCommit.Enable = false
	}
	if c.CommitInterval > 0 {
		cfg.Consumer.Offsets.AutoCommit.Interval = time.Duration(c.CommitInterval) * time.Second
	}
	if c.ReturnErrors {
		cfg.Consumer.Return.Errors = true
	}
	if c.LogOut != nil {
		sarama.Logger = log.New(c.LogOut, "[Kafka Sarama] ", log.LstdFlags)
	}
	cg, err := sarama.NewConsumerGroup(c.Brokers, c.Group, cfg)
	if err != nil {
		return err
	}

	if c.MsgChanCap > 0 {
		data.msgChanCap = c.MsgChanCap
	}

	if pCtx == nil {
		pCtx = context.Background()
	}
	data.saramaCfg = cfg
	data.brokers = c.Brokers
	data.topics = c.Topics
	data.group = c.Group
	data.pCtx = pCtx
	data.cg = cg

	go startConsume()
	return nil
}

func startConsume() {
	data.msgChan = make(chan *sarama.ConsumerMessage, data.msgChanCap)
	data.closeChan = make(chan bool)
	defer func() {
		if err := recover(); err != nil {
			sarama.Logger.Printf("Error: panic recover %v", err)
		}
		// 保证消费者组和消息管道被关闭
		if err := data.cg.Close(); err != nil {
			sarama.Logger.Printf("Error: closing client: %v\n", err)
		}

		sarama.Logger.Println("Debug: defer first finished")
	}()

	defer func() {
		close(data.msgChan)
		sarama.Logger.Println("Info: close msg chan")
		close(data.closeChan)
		sarama.Logger.Println("Debug: defer second finished")
	}()

	ctx, cancel := context.WithCancel(data.pCtx)
	data.cancel = cancel
	wg := &sync.WaitGroup{}

	// 消费错误信息
	if data.saramaCfg.Consumer.Return.Errors {
		data.errChan = make(chan error, 200)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(data.errChan)
			for {
				select {
				case cErr, ok := <-data.cg.Errors():
					if !ok {
						sarama.Logger.Println("Info: sarama errors chan channel was closeChan")
						return
					}
					data.errChan <- cErr
				case <-ctx.Done():
					sarama.Logger.Println("Info: stop to consume sarama error")
					return
				}
			}
		}()
	}
	consumer := myConsumer{}
	wg.Add(1)
	// 必须采用 for 循环，触发 rebalance 后能再次进行消费，消费者数量变化或是分区数量变化等
	go func() {
		defer wg.Done()
		var cnt int
		for {
			if err := data.cg.Consume(ctx, data.topics, &consumer); err != nil {
				// 此处失败，日志不容易被用户感知
				sarama.Logger.Printf("Error: myConsumer group to cunsume: %v\n", err)
				cnt++
				if cnt == 3 {
					// 失败后，重试 3 次
					cancel()
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
			sarama.Logger.Println("Debug: data.cg.Consume returned succeed")
			if ctx.Err() != nil {
				sarama.Logger.Println("Info: cancel happened")
				return
			}
			sarama.Logger.Println("Info: rebalance happened")
		}
	}()

	sarama.Logger.Println("Info: Sarama myConsumer up and running!...")
	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			sarama.Logger.Println("Info: terminated by context cancelled")
			keepRunning = false
		}
	}
	cancel()
	wg.Wait()
}

// Close 关闭消费者组和消息管道，释放资源，该函数应该被调用
func Close() {
	data.cancel()
	<-data.closeChan
	sarama.Logger.Println("Info: close finished")
}

type myConsumer struct {
}

func (consumer *myConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *myConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *myConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("Info: sarama message channel was closeChan")
				return nil
			}
			data.msgChan <- message
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
