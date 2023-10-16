package connection

import (
	"context"
	"errors"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

type ConnCallback struct {
	Dialer  *kafka.Dialer
	Brokers []string
}

func (kafkaConnection *ConnCallback) Setup() (*kafka.Conn, error) {
	if kafkaConnection.Dialer == nil {
		return nil, errors.New("dialer is not defined")
	}
	if len(kafkaConnection.Brokers) < 1 {
		return nil, errors.New("no broker is defined")
	}
	for _, broker := range kafkaConnection.Brokers {

		conn, err := kafkaConnection.Dialer.DialContext(context.Background(), "tcp", broker)
		if err == nil {

			return conn, nil
		}

	}
	return nil, errors.New("cannot find valid host")
}
func (kafkaConn *ConnCallback) DoWithConnection(fn func(conn *kafka.Conn) error) error {
	conn, err := kafkaConn.Setup()
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(conn)

}
func (kafkaConn *ConnCallback) DoWithAdmin(fn func(controller *kafka.Conn) error) error {

	return kafkaConn.DoWithConnection(func(conn *kafka.Conn) error {
		controller, err := conn.Controller()
		if err != nil {
			return err
		}

		controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))

		if err != nil {
			return err
		}
		defer controllerConn.Close()
		return fn(controllerConn)
	})
}
