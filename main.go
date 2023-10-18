package main

import (
	"fmt"

	"github.com/corluk/kafkahelper/conn"
	"github.com/segmentio/kafka-go"
)

func main() {

	dialer := kafka.Dialer{}
	brokers := []string{"localhost:9092"}

	kafkaConn := conn.Conn{
		Dialer:  &dialer,
		Brokers: brokers,
	}

	kafkaReader := conn.Reader{
		Conn: &kafkaConn,
		Config: &kafka.ReaderConfig{
			Topic: "test-topic",
		},
	}

	kafkaReader.Read(func(value interface{}) error {
		fmt.Printf("value is  %s\n", value)
		return nil
	})
}
