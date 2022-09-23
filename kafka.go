package kafka

import (
	"context"
	"github.com/Nixson/environment"
	"github.com/Nixson/logNx"
	kfk "github.com/segmentio/kafka-go"
	"strings"
	"time"
)

type Listener func([]byte) error

func Listen(listener Listener, topic, group string) {
	go runListener(listener, topic, group)
}

func Send(topicName string, message []byte) error {
	env := environment.GetEnv()
	var writer = getWriter(env.Get("kafka.url"), topicName)
	ctx := context.Background()
	err := writer.WriteMessages(ctx, kfk.Message{
		Value: message,
	})
	if err != nil {
		return err
	}
	return nil
}

func runListener(listener Listener, kafkaTopic, kafkaGroup string) {
	env := environment.GetEnv()
	reader := getReader(env.Get("kafka.url"), kafkaTopic, kafkaGroup)
	defer reader.Close()

	ctx := context.Background()
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			logNx.Get().Error(err.Error())
			continue
		}

		err = listener(m.Value)
		if err != nil {
			logNx.Get().Error(err.Error())
			continue
		}
	}
}

func getReader(kafkaURL, kafkaTopic, kafkaGroup string) *kfk.Reader {
	env := environment.GetEnv()
	brokers := strings.Split(kafkaURL, ",")
	return kfk.NewReader(kfk.ReaderConfig{
		Brokers:        brokers,
		GroupID:        kafkaGroup,
		Topic:          kafkaTopic,
		MinBytes:       env.GetInt("kafka.minBytes"),
		MaxBytes:       env.GetInt("kafka.maxBytes"),
		CommitInterval: time.Second * time.Duration(env.GetInt("kafka.interval")),
	})
}

func getWriter(kafkaURL, kafkaTopic string) *kfk.Writer {
	brokers := strings.Split(kafkaURL, ",")
	w := &kfk.Writer{
		Addr:                   kfk.TCP(brokers...),
		Topic:                  kafkaTopic,
		Balancer:               &kfk.RoundRobin{},
		AllowAutoTopicCreation: true,
	}
	return w
}
