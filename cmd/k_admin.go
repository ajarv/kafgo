package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	log.SetFlags(0)

	var brokers string

	flag.StringVar(&brokers, "brokers", "localhost:9092", "Kafka Broker Url")
	flag.Parse()

	fmt.Printf("brokers %s\n", brokers)

	client := &kafka.Client{
		Addr:    kafka.TCP(brokers),
		Timeout: 5 * time.Second,
	}
	metadata, err := client.Metadata(context.Background(), &kafka.MetadataRequest{})
	if err != nil {
		fmt.Println("Error Here ->", err)
		return
	}
	jstr, err := json.MarshalIndent(metadata, "", " ")
	if err != nil {
		fmt.Println("Error Here ->", err)
		return
	}
	fmt.Println("Metadata ", string(jstr))
}
