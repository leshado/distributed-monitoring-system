package adapters

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

const (
	headerContentType = "content-type"
	headerSchema      = "schema"
	headerEventID     = "event_id"
)

var (
	ErrPermanent = errors.New("kafka: permanent error")

	textMapPropagator = otel.GetTextMapPropagator()
)

func Permanent(err error) error {
	if err == nil {
		return ErrPermanent
	}
	return fmt.Errorf("%w: %w", ErrPermanent, err)
}

func IsPermanent(err error) bool {
	return errors.Is(err, ErrPermanent)
}

type KafkaMetrics struct {
	HandlerDuration *prometheus.HistogramVec
	RetriesTotal    *prometheus.CounterVec
	DLQTotal        *prometheus.CounterVec
}

func DefaultKafkaMetrics(reg prometheus.Registerer, service string) *KafkaMetrics {
	m := &KafkaMetrics{
		HandlerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "dms",
			Subsystem: service,
			Name:      "kafka_handler_duration_seconds",
			Help:      "Kafka message handler duration.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"topic"}),
		RetriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dms",
			Subsystem: service,
			Name:      "kafka_handler_retries_total",
			Help:      "Number of handler retries due to transient errors.",
		}, []string{"topic"}),
		DLQTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dms",
			Subsystem: service,
			Name:      "kafka_dlq_total",
			Help:      "Number of messages published to DLQ.",
		}, []string{"topic"}),
	}
	if reg != nil {
		for _, collector := range []prometheus.Collector{m.HandlerDuration, m.RetriesTotal, m.DLQTotal} {
			if err := reg.Register(collector); err != nil {
				if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
					panic(err)
				}
			}
		}
	}
	return m
}

type RetryConfig struct {
	MaxAttempts int
	MinDelay    time.Duration
	MaxDelay    time.Duration
	Jitter      float64
}

func (c RetryConfig) withDefaults() RetryConfig {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 8
	}
	if c.MinDelay <= 0 {
		c.MinDelay = 100 * time.Millisecond
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = 30 * time.Second
	}
	if c.Jitter <= 0 {
		c.Jitter = 0.2
	}
	return c
}

type ConsumerOptions struct {
	ServiceName string
	Retry       RetryConfig
	Producer    *Producer
	DLQEnabled  bool
	Metrics     *KafkaMetrics
	Propagator  propagation.TextMapPropagator
}

func (o ConsumerOptions) withDefaults() ConsumerOptions {
	o.Retry = o.Retry.withDefaults()
	if o.Propagator == nil {
		o.Propagator = textMapPropagator
	}
	return o
}

type ProducerOptions struct {
	Propagator propagation.TextMapPropagator
}

func (o ProducerOptions) withDefaults() ProducerOptions {
	if o.Propagator == nil {
		o.Propagator = textMapPropagator
	}
	return o
}

type messageHeadersCarrier struct {
	hdrs *[]kafkago.Header
}

func (c messageHeadersCarrier) Get(key string) string {
	for _, h := range *c.hdrs {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c messageHeadersCarrier) Set(key, val string) {
	for i := range *c.hdrs {
		if (*c.hdrs)[i].Key == key {
			(*c.hdrs)[i].Value = []byte(val)
			return
		}
	}
	*c.hdrs = append(*c.hdrs, kafkago.Header{Key: key, Value: []byte(val)})
}

func (c messageHeadersCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.hdrs))
	for _, h := range *c.hdrs {
		keys = append(keys, h.Key)
	}
	return keys
}

func getHeader(msg Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func ensureHeader(msg *kafkago.Message, key, value string) {
	for i := range msg.Headers {
		if msg.Headers[i].Key == key {
			msg.Headers[i].Value = []byte(value)
			return
		}
	}
	msg.Headers = append(msg.Headers, kafkago.Header{Key: key, Value: []byte(value)})
}

func retryDelay(cfg RetryConfig, attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	d := cfg.MinDelay * time.Duration(1<<(attempt-1))
	if d > cfg.MaxDelay {
		d = cfg.MaxDelay
	}
	if cfg.Jitter > 0 {
		j := float64(d) * cfg.Jitter
		if j > 0 {
			delta := (rand.Float64()*2 - 1) * j
			d = time.Duration(float64(d) + delta)
			if d < 0 {
				d = 0
			}
		}
	}
	return d
}

type Producer struct {
	writer *kafkago.Writer
	logger *zap.Logger
	opts   ProducerOptions
}

func NewProducer(brokers []string, logger *zap.Logger) *Producer {
	return NewProducerWithOptions(brokers, logger, ProducerOptions{})
}

func NewProducerWithOptions(brokers []string, logger *zap.Logger, opts ProducerOptions) *Producer {
	opts = opts.withDefaults()
	return &Producer{
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Balancer:               &kafkago.Hash{},
			BatchSize:              100,
			BatchTimeout:           25 * time.Millisecond,
			RequiredAcks:           kafkago.RequireAll,
			AllowAutoTopicCreation: true,
			Async:                  false,
		},
		logger: logger,
		opts:   opts,
	}
}

func (p *Producer) Publish(ctx context.Context, topic string, key []byte, event any) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal kafka event: %w", err)
	}
	msg := kafkago.Message{
		Topic: topic,
		Key:   key,
		Value: value,
		Time:  time.Now().UTC(),
		Headers: []kafkago.Header{
			{Key: headerContentType, Value: []byte("application/json")},
			{Key: headerSchema, Value: []byte("v1")},
		},
	}
	if b, ok := event.(interface{ GetEventID() string }); ok {
		if id := b.GetEventID(); id != "" {
			ensureHeader(&msg, headerEventID, id)
		}
	}
	h := msg.Headers
	p.opts.Propagator.Inject(ctx, messageHeadersCarrier{hdrs: &h})
	msg.Headers = h

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish %s: %w", topic, err)
	}
	p.logger.Debug("kafka event published", zap.String("topic", topic), zap.ByteString("key", key))
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

type Message = kafkago.Message

type Handler func(context.Context, Message) error

type Consumer struct {
	reader *kafkago.Reader
	logger *zap.Logger
	opts   ConsumerOptions
}

func NewConsumerWithOptions(brokers []string, topic, groupID string, logger *zap.Logger, opts ConsumerOptions) *Consumer {
	opts = opts.withDefaults()
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:                brokers,
			Topic:                  topic,
			GroupID:                groupID,
			MinBytes:               1,
			MaxBytes:               10e6,
			MaxWait:                500 * time.Millisecond,
			CommitInterval:         0,
			WatchPartitionChanges:  true,
			PartitionWatchInterval: 5 * time.Second,
		}),
		logger: logger,
		opts:   opts,
	}
}

func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Warn("kafka fetch failed", zap.Error(err))
			continue
		}

		eventID := getHeader(msg, headerEventID)
		ctx = c.opts.Propagator.Extract(ctx, messageHeadersCarrier{hdrs: &msg.Headers})
		ctx, span := otel.Tracer(c.opts.ServiceName).Start(ctx, "kafka.consume")

		start := time.Now()

		attempt := 0
		for {
			attempt++
			if c.opts.Metrics != nil {
				if attempt > 1 {
					c.opts.Metrics.RetriesTotal.WithLabelValues(msg.Topic).Inc()
				}
			}

			hErr := handler(ctx, msg)
			if hErr == nil {
				break
			}

			fields := []zap.Field{
				zap.Error(hErr),
				zap.String("topic", msg.Topic),
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.String("event_id", eventID),
				zap.Int("attempt", attempt),
			}

			if IsPermanent(hErr) {
				c.logger.Warn("kafka message handling permanent error", fields...)
				_ = c.tryPublishDLQ(ctx, msg, eventID, hErr)
				break
			}

			if attempt >= c.opts.Retry.MaxAttempts {
				c.logger.Error("kafka message handling exceeded retry attempts", fields...)
				_ = c.tryPublishDLQ(ctx, msg, eventID, hErr)
				break
			}

			c.logger.Warn("kafka message handling transient error", fields...)
			d := retryDelay(c.opts.Retry, attempt)
			if d > 0 {
				t := time.NewTimer(d)
				select {
				case <-ctx.Done():
					t.Stop()
					span.End()
					return nil
				case <-t.C:
				}
			}
		}

		span.End()
		if c.opts.Metrics != nil {
			c.opts.Metrics.HandlerDuration.WithLabelValues(msg.Topic).Observe(time.Since(start).Seconds())
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("kafka commit failed", zap.Error(err), zap.String("topic", msg.Topic), zap.Int64("offset", msg.Offset), zap.String("event_id", eventID))
		}
	}
}

func (c *Consumer) tryPublishDLQ(ctx context.Context, msg Message, eventID string, cause error) error {
	if !c.opts.DLQEnabled || c.opts.Producer == nil {
		return nil
	}
	dlqTopic := msg.Topic + ".dlq"

	msg.Headers = append([]kafkago.Header(nil), msg.Headers...)
	ensureHeader(&msg, headerEventID, eventID)
	ensureHeader(&msg, "x-dlq-topic", msg.Topic)
	ensureHeader(&msg, "x-dlq-partition", strconv.Itoa(msg.Partition))
	ensureHeader(&msg, "x-dlq-offset", strconv.FormatInt(msg.Offset, 10))
	ensureHeader(&msg, "x-dlq-error", cause.Error())

	dlqMsg := kafkago.Message{
		Topic:   dlqTopic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: msg.Headers,
		Time:    time.Now().UTC(),
	}
	h := dlqMsg.Headers
	c.opts.Propagator.Inject(ctx, messageHeadersCarrier{hdrs: &h})
	dlqMsg.Headers = h

	if err := c.opts.Producer.writer.WriteMessages(ctx, dlqMsg); err != nil {
		c.logger.Error("kafka dlq publish failed",
			zap.Error(err),
			zap.String("dlq_topic", dlqTopic),
			zap.String("topic", msg.Topic),
			zap.Int("partition", msg.Partition),
			zap.Int64("offset", msg.Offset),
			zap.String("event_id", eventID),
		)
		return err
	}
	if c.opts.Metrics != nil {
		c.opts.Metrics.DLQTotal.WithLabelValues(msg.Topic).Inc()
	}
	c.logger.Warn("kafka message published to dlq",
		zap.String("dlq_topic", dlqTopic),
		zap.String("topic", msg.Topic),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
		zap.String("event_id", eventID),
	)
	return nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func Decode[T any](msg Message) (T, error) {
	var out T
	err := json.Unmarshal(msg.Value, &out)
	return out, err
}
