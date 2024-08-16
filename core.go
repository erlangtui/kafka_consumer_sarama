package kafka_consumer_sarama

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

var (
	msgChan chan *sarama.ConsumerMessage
	errChan chan error
)

func Messages() <-chan *sarama.ConsumerMessage {
	// TODO 调用该方法后，调用方还是需要导入 sarama 包，又涉及到版本问题；如果重新写 sarama.ConsumerMessage 结构则存在数据拷贝影响性能
	return msgChan
}

// Errors 配置中开启了 ReturnErrors 后才会有消息，默认是直接写入 LogOut 配置中
func Errors() <-chan error {
	return errChan
}
func Start(pCtx context.Context, c *KCSConfig) error {
	sarama.Logger = log.New(io.Discard, "[Sarama] ", log.LstdFlags)
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

	msgChanCap := 10000
	if c.MsgChanCap > 0 {
		msgChanCap = c.MsgChanCap
	}
	msgChan = make(chan *sarama.ConsumerMessage, msgChanCap)

	if pCtx == nil {
		pCtx = context.Background()
	}
	ic := &innerConfig{
		saramaCfg: cfg,
		brokers:   c.Brokers,
		topics:    c.Topics,
		group:     c.Group,
		ctx:       pCtx,
		cg:        cg,
	}
	go startConsume(ic)
	return nil
}

func startConsume(ic *innerConfig) {
	defer func() {
		if err := ic.cg.Close(); err != nil {
			sarama.Logger.Printf("Error: closing client: %v\n", err)
		}
		if errChan != nil {
			close(errChan)
		}
		close(msgChan)
		sarama.Logger.Println("Info: close msg chan")
	}()
	retry := 0
	defer func() {
		// 触发 rebalance 如果错误时会panic，在此处捕获，并尝试重新构建消费者组消费，如果依然报错则 panic
		if err := recover(); err != nil {
			sarama.Logger.Printf("Error: panic recover %v", err)
			if retry <= 1 {
				cg, err1 := sarama.NewConsumerGroup(ic.brokers, ic.group, ic.saramaCfg)
				if err1 != nil {
					panic(fmt.Errorf("retry to new consumer group failed, new err: %v, origin err: %v", err1, err))
				}
				ic.cg = cg
				go startConsume(ic)
			}
		}
	}()
	ctx, cancel := context.WithCancel(ic.ctx)
	wg := &sync.WaitGroup{}
	if ic.saramaCfg.Consumer.Return.Errors {
		errChan = make(chan error, 200)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case cErr, ok := <-ic.cg.Errors():
					if !ok {
						sarama.Logger.Println("Info: errors chan closed")
						return
					}
					errChan <- cErr
				case <-ctx.Done():
					sarama.Logger.Println("Info: stop to consume error")
					return
				}
			}
		}()
	}
	consumer := consumer{
		ready: make(chan bool),
	}
	wg.Add(1)
	// 必须采用 for 循环，触发 rebalance 后能再次进行消费，消费者数量变化或是分区数量变化等
	go func() {
		defer wg.Done()
		var cnt int
		for {
			if err := ic.cg.Consume(ctx, ic.topics, &consumer); err != nil {
				// 此处失败，日志不容易被用户感知
				sarama.Logger.Printf("Error: consumer group to cunsume: %v\n", err)
				cnt++
				if cnt == 3 {
					// 失败后，重试 3 次，如果依然失败，尝试在 defer 中重新构建消费者组消费
					retry++
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
			if ctx.Err() != nil {
				sarama.Logger.Println("Info: cancel happened")
				return
			}
			consumer.ready = make(chan bool)
			sarama.Logger.Println("Info: rebalance happened")
		}
	}()

	<-consumer.ready // 阻塞等待，直到消费者组执行 Setup
	sarama.Logger.Println("Info: Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			sarama.Logger.Println("Info: terminated by context cancelled")
			keepRunning = false
		case <-sigterm:
			sarama.Logger.Println("Info: terminated by via signal")
			keepRunning = false
		}
	}
	cancel()
	wg.Wait()

}

type consumer struct {
	ready chan bool
	// Errors chan error
}

func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("Info: message channel was closed")
				return nil
			}
			msgChan <- message
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
