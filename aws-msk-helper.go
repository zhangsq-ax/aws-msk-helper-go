package aws_msk_helper

import (
	"context"
	"crypto/tls"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
	"time"
)

func getScramMechanism(username string, password string) (sasl.Mechanism, error) {
	return scram.Mechanism(scram.SHA512, username, password)
}

func createScramDialer(username string, password string) (*kafka.Dialer, error) {
	mechanism, err := getScramMechanism(username, password)
	if err != nil {
		return nil, err
	}
	return &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}, nil
}

func createScramTransport(username string, password string) (*kafka.Transport, error) {
	mechanism, err := getScramMechanism(username, password)
	if err != nil {
		return nil, err
	}
	return &kafka.Transport{
		DialTimeout: 10 * time.Second,
		SASL:        mechanism,
		TLS:         &tls.Config{},
	}, nil
}

type NewKafkaReaderOptions struct {
	Brokers     []string
	GroupTopics []string
	Topic       string
	GroupID     string
	Partition   int
	Username    string
	Password    string
}

func NewKafkaReader(opts *NewKafkaReaderOptions) (*kafka.Reader, error) {
	dialer, err := createScramDialer(opts.Username, opts.Password)
	if err != nil {
		return nil, err
	}

	conf := kafka.ReaderConfig{
		Brokers:     opts.Brokers,
		GroupTopics: opts.GroupTopics,
		Topic:       opts.Topic,
		GroupID:     opts.GroupID,
		Partition:   opts.Partition,
		Dialer:      dialer,
	}

	return kafka.NewReader(conf), nil
}

func Subscribe(ctx context.Context, opts *NewKafkaReaderOptions, handler func(message kafka.Message)) error {
	reader, err := NewKafkaReader(opts)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					return
				}
				handler(msg)
			}
		}
	}()

	return nil
}

type NewKafkaWriterOptions struct {
	Brokers      []string
	Topic        string        // optional
	BatchSize    int           // optional
	BatchTimeout time.Duration // optional
	Username     string
	Password     string
}

func NewKafkaWriter(opts *NewKafkaWriterOptions) (*kafka.Writer, error) {
	transport, err := createScramTransport(opts.Username, opts.Password)
	if err != nil {
		return nil, err
	}

	return &kafka.Writer{
		Addr:         kafka.TCP(opts.Brokers...),
		Transport:    transport,
		BatchSize:    opts.BatchSize,
		BatchTimeout: opts.BatchTimeout,
		Topic:        opts.Topic,
	}, nil
}

func WriteMessage(ctx context.Context, writer *kafka.Writer, key []byte, value []byte, topic string, partition *int) error {
	msg := kafka.Message{
		Key:   key,
		Value: value,
	}
	if partition != nil {
		msg.Partition = *partition
	}
	if topic != "" {
		msg.Topic = topic
	}
	return writer.WriteMessages(ctx, msg)
}
