package conn

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Writer struct {
	Conn *Conn
}

// changed to arguments as dots
func (writer *Writer) WriteJSON(topic string, message kafka.Message) error {

	_, err := writer.Conn.Setup()
	if err != nil {
		return err
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
	/*
		var kafkaMessages []kafka.Message

		for _, message := range messages {

			b, err := json.Marshal(message)
			if err == nil {
				kafkaMessages = append(kafkaMessages, kafka.Message{

					Value: b,
				})

			}
		}
	*/
	defer w.Close()

	return w.WriteMessages(context.Background(), message)

}
