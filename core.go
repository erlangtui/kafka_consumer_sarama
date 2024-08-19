package kafka_consumer_sarama

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	if c.MsgChanCap > 0 {
		data.msgChanCap = c.MsgChanCap
	}
	if c.retryConsume > 0 {
		data.retryThreshold = c.retryConsume
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

		// data.retryCnt > 0 表示 data.cg.Consume(pCtx, data.topics, &consumer) 返回错误，尝试在此处重新构建消费者组消费，如果依然报错则 panic
		if data.retryCnt > 0 && data.retryCnt <= data.retryThreshold {
			cg, err := sarama.NewConsumerGroup(data.brokers, data.group, data.saramaCfg)
			if err != nil {
				// TODO to optimize
				panic(fmt.Errorf("retryCnt to new consumer group failed, err: %v", err))
			}
			data.cg = cg
			// 等待旧管道的数据被消费完毕，避免消息丢失
			for len(data.msgChan) > 0 {
				time.Sleep(10 * time.Millisecond)
			}
			go startConsume()
			// 会重新创建新的消息管道替换掉旧的管道
			sarama.Logger.Println("Info: retry to start consume")
		}
		sarama.Logger.Println("Debug: defer one finished")
	}()

	defer func() {
		// 保证消费者组和消息管道被关闭
		if err := data.cg.Close(); err != nil {
			sarama.Logger.Printf("Error: closing client: %v\n", err)
		}
		if !(data.retryCnt > 0 && data.retryCnt <= data.retryThreshold) {
			// 重试阶段不关闭消息管道，避免用户消费不到消息提前返回
			close(data.msgChan)
			sarama.Logger.Println("Info: close msg chan")
		}
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
						sarama.Logger.Println("Info: errors chan closeChan")
						return
					}
					data.errChan <- cErr
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
			if err := data.cg.Consume(ctx, data.topics, &consumer); err != nil {
				// 此处失败，日志不容易被用户感知
				sarama.Logger.Printf("Error: consumer group to cunsume: %v\n", err)
				cnt++
				if cnt == 3 {
					// 失败后，重试 3 次，如果依然失败，尝试在 defer 中重新构建消费者组消费
					data.retryCnt++
					cancel()
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
			sarama.Logger.Println("data.cg.Consume returned succeed")
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

	// sigterm := make(chan os.Signal, 1)
	// signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			sarama.Logger.Println("Info: terminated by context cancelled")
			keepRunning = false
			// case <-sigterm:
			// 	sarama.Logger.Println("Info: terminated by via signal")
			// 	keepRunning = false
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

type consumer struct {
	ready chan bool
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
				log.Printf("Info: message channel was closeChan")
				return nil
			}
			data.msgChan <- message
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
