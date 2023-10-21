package conn

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/segmentio/kafka-go"
)

type Reader struct {
	Conn   *Conn
	Config *kafka.ReaderConfig
}

func (kafkaReader *Reader) ReadWithChannel(ch chan ([]byte)) error {
	if kafkaReader.Conn == nil {
		return errors.New("no conn")
	}
	if kafkaReader.Config == nil {
		return errors.New("no config")
	}
	kafkaReader.Config.Dialer = kafkaReader.Conn.Dialer
	kafkaReader.Config.Brokers = kafkaReader.Conn.Brokers

	reader := kafka.NewReader(*kafkaReader.Config)
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err == nil {
			ch <- msg.Value

		}
	}

}

func (kafkaReader *Reader) ReadWithCallback(cb func(value interface{}) error) error {
	if kafkaReader.Conn == nil {
		return errors.New("no conn")
	}
	if kafkaReader.Config == nil {
		return errors.New("no config")
	}
	kafkaReader.Config.Dialer = kafkaReader.Conn.Dialer
	kafkaReader.Config.Brokers = kafkaReader.Conn.Brokers

	reader := kafka.NewReader(*kafkaReader.Config)
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err == nil {
			var item interface{}
			json.Unmarshal(msg.Value, &item)
			cb(item)
		}
	}

}
