package conn

import (
	"context"
	"errors"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

// KafkaMagic
type ConnMode string

const (
	CONTROLLER ConnMode = "controller"
)

type Conn struct {
	Dialer         *kafka.Dialer
	Brokers        []string
	WithController bool
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
			if !Conn.WithController {
				return conn, err
			}
			controller, err := conn.Controller()
			if err != nil {
				continue
			}
			conn.Close()
			conn, err := Conn.Dialer.DialContext(context.Background(), "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))

			if err != nil {
				continue
			}
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

func (kafkaConn *Conn) DoWithChannel(ch chan (*kafka.Conn)) error {
	conn, err := kafkaConn.Setup()
	if err != nil {
		return err
	}

	ch <- conn
	return nil
}
