package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type KConsumer struct {
	consumerId   string
	readerConfig kafka.ReaderConfig
	retryCount   int64
	err          error
}
type MProcessor struct {
	processorId string
	err         error
}

func (consumer *KConsumer) String() string {
	return fmt.Sprintf("consumer: %v,retryCount: %v, err: %v\n", consumer.consumerId, consumer.retryCount, consumer.err)
}

func (consumer *KConsumer) consume(ctx context.Context, wg *sync.WaitGroup, ch chan<- kafka.Message) {

	defer wg.Done() // decrements the WaitGroup counter by one when the function returns
	defer close(ch)
	start := time.Now()
	sleepDuration := time.Duration(int64(time.Second) * consumer.retryCount * 3)
	// fmt.Println("Sleeping for some time", sleepDuration)
	time.Sleep(sleepDuration)

	reader := kafka.NewReader(consumer.readerConfig)
	defer reader.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(ctx)
		t := time.Now()
		fmt.Printf("Time elapsed %v\n", t.Sub(start))
		if err != nil {
			consumer.err = err
			fmt.Println("Error Here ->", err)
			return
		}

		ch <- m
	}
}

func (processor *MProcessor) process(ctx context.Context, wg *sync.WaitGroup, ch <-chan kafka.Message) {
	defer wg.Done() // decrements the WaitGroup counter by one when the function returns
	for {
		select {
		case <-ctx.Done(): // Done returns a channel that's closed when work done on behalf of this context is canceled
			fmt.Println("Exiting from reading go routine")
			return
		case m, ok := <-ch:
			if !ok {
				fmt.Println("Channel has been closed")
				return
			}
			fmt.Printf("Processor %s - message at topic:%v partition:%v offset:%v	%s = %s\n",
				processor.processorId, m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		}
	}
}

func main() {
	log.SetFlags(0)

	var brokers, topic, groupID string

	flag.StringVar(&brokers, "brokers", "localhost:9092", "Kafka Broker Url")
	flag.StringVar(&topic, "topic", "arrivals.topic", "Kafka Topic")
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

	consumer := &KConsumer{
		consumerId: "GopiNath",
		readerConfig: kafka.ReaderConfig{
			Brokers:  strings.Split(brokers, ","),
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		},
		retryCount: 0,
	}

	channel := make(chan kafka.Message)

	waitGroup := sync.WaitGroup{}
	for _, name := range strings.Split("tanku rambo monkey kirpal jasmeet harbhajan dilkhush dravid", " ") {
		p := &MProcessor{processorId: name}
		waitGroup.Add(1)
		go p.process(ctx, &waitGroup, channel)
	}
	waitGroup.Add(1) // adds delta, if the counter becomes zero, all goroutines blocked on Wait are released

	go consumer.consume(ctx, &waitGroup, channel)
	waitGroup.Wait() // it blocks until the WaitGroup counter is zero

}
