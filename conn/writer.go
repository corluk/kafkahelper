package conn

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Writer struct {
	Conn *Conn
}

func (writer *Writer) GetWriter(topic string) (*kafka.Writer, error) {

	_, err := writer.Conn.Setup()
	if err != nil {
		return nil, err
	}
	w := &kafka.Writer{
		Addr:     kafka.TCP(writer.Conn.Brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Transport: &kafka.Transport{
			Dial: writer.Conn.Dialer.DialFunc,
			SASL: writer.Conn.Dialer.SASLMechanism,
		},
	}
	return w, nil
}

// changed to arguments as dots
func (writer *Writer) WriteJSON(topic string, message []kafka.Message) error {

	w, err := writer.GetWriter(topic)
	if err != nil {
		return err
	}
	// closes connection
	defer w.Close()
	
	return w.WriteMessages(context.Background(), message...)
}
