package conn

import (
	"encoding/json"

	"github.com/go-playground/locales/mg_MG"
	"github.com/segmentio/kafka-go"
)

type KafkaWriter struct { 
	Brokers     []string
	Dialer      *kafka.Dialer
	TopicConfig *kafka.TopicConfig
	Messages    []interface{}
}

func (kafkaWriter *KafkaWriter) Send( message interface{}) error {


	kafkaWriter.Messages = []interface{message} 
	conn := Conn{
		Dialer: kafkaWriter.Dialer, 
		Brokers: kafkaWriter.Brokers ,
		WithController:  false,
	}
	conn.Do(func(kconn *kafka.Conn) error {
		for _ , msg := range(kafkaWriter.Messages) {
			b  , err := json.Marshal(msg)
			if err != nil {

				m := &kafka.Message{
					Topic: kafkaWriter.TopicConfig.Topic,
					Value: ,
				}
			}
			
			
		}
		kconn.WriteMessages()
	})
	err := conn.CreateTopic(kafkaWriter.TopicConfig) 
	if err != nil {
		return err 
	}

	w := &kafka.Writer{
		Topic:  kafkaWriter.TopicConfig.Topic,
		
	} 
}

func (KafkaWrite)
