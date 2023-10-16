package conn

import (
	"context"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

type ConnAdmin struct {
	Conn *Conn
}

func (connAdmin *ConnAdmin) Do(fn func(controller *kafka.Conn) error) error {

	return connAdmin.Conn.Do(func(conn *kafka.Conn) error {
		controller, err := conn.Controller()
		if err != nil {
			return err
		}

		adminConn, err := connAdmin.Conn.Dialer.DialContext(context.Background(), "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))

		if err != nil {
			return err
		}
		defer adminConn.Close()
		return fn(adminConn)
	})
}
