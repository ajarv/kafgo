package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	kafka "github.com/segmentio/kafka-go"
)

type KProducer struct {
	writer     kafka.Writer
	retryCount int64
	err        error
}

type message struct {
	key   []byte
	value []byte
}

func (producer *KProducer) String() string {
	return fmt.Sprintf("producer: %v\n", producer)
}

func (producer *KProducer) produce(ctx context.Context, wg *sync.WaitGroup, ch <-chan message) {
	defer wg.Done() // decrements the WaitGroup counter by one when the function returns

	defer func() {
		if err := producer.writer.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}()
	for {
		select {
		case <-ctx.Done(): // Done returns a channel that's closed when work done on behalf of this context is canceled
			fmt.Println("Exiting from writing")
			return
		case m, ok := <-ch:
			if !ok {
				fmt.Println("Channel has been closed")
				return
			}
			err := producer.writer.WriteMessages(ctx,
				// NOTE: Each Message has Topic defined, otherwise an error is returned.
				kafka.Message{
					Key:   m.key,
					Value: m.value,
				},
			)
			if err != nil {
				log.Fatal("failed to write messages:", err)
			}

		}
	}

}

func main() {
	log.SetFlags(0)

	var brokers, topic, groupID string
	var count int64

	flag.StringVar(&brokers, "brokers", "localhost:9092", "Kafka Broker Url")
	flag.StringVar(&topic, "topic", "arrivals.topic", "Kafka Topic")
	flag.Int64Var(&count, "count", 1, "Messages count")
	flag.StringVar(&groupID, "groupId", "plastic", "Consumer Group Id")
	flag.Parse()

	fmt.Printf("brokers %s, topic %s\n", brokers, topic)

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cancel()
	}()

	producer := &KProducer{
		writer: kafka.Writer{
			Addr:     kafka.TCP(brokers),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
		retryCount: 0,
	}

	channel := make(chan message)

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)
	go producer.produce(ctx, &waitGroup, channel)
	value := `{"Event_Id": "e_8_105", "Venue_Id": 17, "firstname": "Irina", "surname": "Sundberg", "favorite_quote": "All work and no play makes Jack a dull boy."}`
	for i := int64(1); i <= count; i++ {
		channel <- message{key: []byte("key1"), value: []byte(value)}
	}
	close(channel)

	waitGroup.Wait() // it blocks until the WaitGroup counter is zero

}
