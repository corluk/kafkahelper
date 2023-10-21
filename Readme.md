Simple Kafka Helper Package 

func Read() chan ([]byte) {
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
	ch := make(chan ([]byte))
	go kafkaReader.ReadWithChannel(ch)
	return ch
}
func main() {

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	readChan := Read()
	for {
		select {
		case value := <-readChan:

			fmt.Printf("value %s", value)
		case <-c:
			fmt.Println("closing with ")
			os.Exit(0)
		}
	}
}
