package conn

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"
)

type Conn struct {
	Dialer  *kafka.Dialer
	Brokers []string
}

func (Conn *Conn) Setup() (*kafka.Conn, error) {
	if Conn.Dialer == nil {
		return nil, errors.New("dialer is not defined")
	}
	if len(Conn.Brokers) < 1 {
		return nil, errors.New("no broker is defined")
	}
	for _, broker := range Conn.Brokers {

		conn, err := Conn.Dialer.DialContext(context.Background(), "tcp", broker)
		if err == nil {

			return conn, nil
		}

	}
	return nil, errors.New("cannot find valid host")
}
func (kafkaConn *Conn) Do(fn func(conn *kafka.Conn) error) error {
	conn, err := kafkaConn.Setup()
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(conn)

}
