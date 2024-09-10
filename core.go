package kafka_consumer_sarama

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func NewConsumer(pCtx context.Context, c *Config) (Consumer, error) {
	cfg, err := c.check()
	if err != nil {
		return nil, err
	}
	cg, err := sarama.NewConsumerGroup(c.Brokers, c.Group, cfg)
	if err != nil {
		return nil, err
	}

	mCap := 10000
	if c.MsgChanCap > 0 {
		mCap = c.MsgChanCap
	} else if c.MsgChanCap < 0 {
		mCap = 0
	}

	if pCtx == nil {
		pCtx = context.Background()
	}
	ctx, cle := context.WithCancel(pCtx)
	mCer := &myConsumer{
		saramaCfg: cfg,
		brokers:   c.Brokers,
		topics:    c.Topics,
		group:     c.Group,
		ctx:       ctx,
		cancel:    cle,
		cg:        cg,
		msgChan:   make(chan *sarama.ConsumerMessage, mCap),
		errChan:   nil,
		closeChan: make(chan bool),
	}

	return mCer, nil
}

// Start 调用改函数后开始从kafka消费消息，调用方应该从 Messages 函数中获取消息
func (c *myConsumer) Start() {
	defer func() {
		if err := recover(); err != nil {
			sarama.Logger.Printf("Error: panic recover %v", err)
		}
		// 保证消费者组被关闭
		if err := c.cg.Close(); err != nil {
			sarama.Logger.Printf("Error: closing client: %v\n", err)
		}

		sarama.Logger.Println("Debug: defer first finished")
	}()

	defer func() {
		// 保证消息管道被关闭
		close(c.msgChan)
		sarama.Logger.Println("Info: close msg chan")
		close(c.closeChan)
		sarama.Logger.Println("Debug: defer second finished")
	}()

	wg := &sync.WaitGroup{}

	// 消费错误信息
	if c.saramaCfg.Consumer.Return.Errors {
		c.errChan = make(chan error, 200)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(c.errChan)
			for {
				select {
				case cErr, ok := <-c.cg.Errors():
					if !ok {
						sarama.Logger.Println("Info: sarama errors chan channel was closeChan")
						return
					}
					c.errChan <- cErr
				case <-c.ctx.Done():
					sarama.Logger.Println("Info: stop to consume sarama error msg")
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
			if err := c.cg.Consume(c.ctx, c.topics, &consumer); err != nil {
				// 此处失败，日志不容易被用户感知
				sarama.Logger.Printf("Error: myConsumer group to cunsume: %v\n", err)
				cnt++
				if cnt == 3 {
					// 失败后，重试 3 次
					c.cancel()
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
			sarama.Logger.Println("Debug: msCer.cg.Consume returned succeed")
			if c.ctx.Err() != nil {
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
		case <-c.ctx.Done():
			sarama.Logger.Println("Info: terminated by context cancelled")
			keepRunning = false
		}
	}
	c.cancel()
	wg.Wait()
}

// Close 关闭消费者组和消息管道，释放资源，该函数应该被调用
func (c *myConsumer) Close() {
	c.cancel()
	<-c.closeChan
	sarama.Logger.Println("Info: close finished")
}

// Messages 获取消息管道，调用法从其中获取消息
func (c *myConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return c.msgChan
}

// Errors 配置中开启了 ReturnErrors 后才会有消息，默认是直接写入 LogOut 配置中
func (c *myConsumer) Errors() <-chan error {
	return c.errChan
}

type Consumer interface {
	Start()
	Close()
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan error
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
	ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error
}

type myConsumer struct {
	saramaCfg *sarama.Config
	brokers   []string
	topics    []string
	group     string
	ctx       context.Context
	cancel    context.CancelFunc
	cg        sarama.ConsumerGroup
	msgChan   chan *sarama.ConsumerMessage
	errChan   chan error
	closeChan chan bool
}

func (c *myConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *myConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *myConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("Info: sarama message channel was closeChan")
				return nil
			}
			c.msgChan <- message
			session.MarkMessage(message, "")
			if !c.saramaCfg.Consumer.Offsets.AutoCommit.Enable {
				session.Commit()
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
