package kafka_consumer_sarama

import (
	"context"
	"io"

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
	Assignor          string    // 分区分配策略，range、roundrobin、sticky，默认是 sticky，均匀性好且 rebalance 后开销小
	InitialOffset     string    // 如果之前未提交任何偏移量，则要使用的初始偏移量，newest 或 oldest，默认 newest
	ReturnErrors      bool      // 是否返回错误，默认丢弃，如果开启后需要通过 Errors() 消费错误信息，避免管道阻塞
	DisableAutoCommit bool      // 是否自动提交 offset，默认 false，即自动提交
	CommitInterval    int       // 自动提交间隔，默认 1，单位 s
	RefreshFrequency  int       // 元数据刷新时间间隔，默认 60，单位 s
	MsgChanCap        int       // 消息管道的容量，缺省时默认 10000，0 值默认为缺省。如果调用方 panic 最多丢失 10000 条数据；如果不想丢失消息，建议设为 -1，创建阻塞管道
	LogOut            io.Writer // 日志输出的地方，默认直接丢弃
}

type innerData struct {
	saramaCfg  *sarama.Config
	brokers    []string
	topics     []string
	group      string
	pCtx       context.Context
	cancel     context.CancelFunc
	cg         sarama.ConsumerGroup
	msgChan    chan *sarama.ConsumerMessage
	errChan    chan error
	closeChan  chan bool
	msgChanCap int
}
