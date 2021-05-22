package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	log.SetFlags(0)

	var brokers string

	flag.StringVar(&brokers, "brokers", "localhost:9092", "Kafka Broker Url")
	flag.Parse()

	fmt.Printf("brokers %s\n", brokers)

	reader := kafka.ReaderConfig{
		Brokers: strings.Split(brokers, ","),
	}
	topics, err := reader.ReadTopicNames(context.Background())
	if err != nil {
		fmt.Println("Error Here ->", err)
		return
	}
	jstr, err := json.MarshalIndent(topics, "", " ")
	if err != nil {
		fmt.Println("Error Here ->", err)
		return
	}
	fmt.Println("Metadata ", string(jstr))
}
