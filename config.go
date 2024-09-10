package kafka_consumer_sarama

import (
	"errors"
	"io"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

// Config 对于下列选填配置，要么严格按照给定的格式填写，要么不填，否则配置无效
type Config struct {
	Brokers           []string  // 必填，kafka 节点
	Topics            []string  // 必填，消费主题
	Group             string    // 必填，消费者组
	SASLEnable        bool      // 是否使用 SASL 身份验证
	SASLUser          string    // SASL 用户
	SASLPassword      string    // SASL 密码
	Version           string    // 版本，格式 0.10.2.1，默认 0.10.2.1
	Assignor          string    // 分区分配策略，range、roundrobin、sticky，默认是 range，sticky 均匀性好且 rebalance 后开销小
	InitialOffset     string    // 如果之前未提交任何偏移量，则要使用的初始偏移量，newest 或 oldest，默认 newest
	ReturnErrors      bool      // 是否返回错误，默认丢弃，如果开启后需要通过 Errors() 消费错误信息，避免管道阻塞
	DisableAutoCommit bool      // 是否自动提交 offset，默认 false，即自动提交；true，表示手动提交，即每次消费消息后同步提交消费位移，即保证了每次消费出来的消息位移被提交，不会重复消费
	CommitInterval    int       // 自动提交间隔，默认 1，单位 s，服务 panic 时 1s 内的位移可能未被提交，可能会导致重复消费
	RefreshFrequency  int       // 元数据刷新时间间隔，默认 60，单位 s
	MsgChanCap        int       // 消息管道的容量，缺省时默认 10000，0 值默认为缺省。如果调用方 panic 最多丢失 10000 条数据；如果不想丢失消息，建议设为 -1，创建阻塞管道
	LogOut            io.Writer // 日志输出的地方，默认直接丢弃
}

func (c *Config) check() (*sarama.Config, error) {
	if c == nil {
		return nil, errors.New("config is nil")
	}
	if len(c.Topics) == 0 {
		return nil, errors.New("config topic is empty")
	}
	if c.Group == "" {
		return nil, errors.New("config group is empty")
	}
	if c.SASLEnable && c.SASLUser == "" {
		return nil, errors.New("config sasl user is empty")
	}
	version := sarama.V0_10_2_1
	if c.Version != "" {
		var err error
		version, err = sarama.ParseKafkaVersion(c.Version)
		if err != nil {
			return nil, err
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
		return nil, errors.New("config assignor unknown, should be one of range, roundrobin and sticky")
	}

	var initial int64
	switch c.InitialOffset {
	case "oldest":
		initial = sarama.OffsetOldest
	case "newest", "":
		initial = sarama.OffsetNewest
	default:
		return nil, errors.New("config initial offset unknown, should be oldest or newest")
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
	return cfg, nil
}
