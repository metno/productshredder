package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
)

var (
	brokers   = flag.String("brokers", os.Getenv("KAFKA_BROKERS"), "The Kafka brokers to connect to, as a comma separated list")
	topic     = flag.String("topic", os.Getenv("KAFKA_TOPIC"), "The Kafka brokers to connect to, as a comma separated list")
	verbose   = flag.Bool("verbose", false, "Turn on Sarama logging")
	verifySsl = flag.Bool("verify", true, "Verify SSL certificates chain")
)

type Server struct {
	Consumer sarama.Consumer
}

func newConsumer(brokerList []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Net.TLS.Enable = true
	config.Net.KeepAlive = 30
	config.Consumer.Return.Errors = false // FIXME
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func main() {
	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	consumer, err := newConsumer(brokerList)
	if err != nil {
		log.Fatalf("Error while creating consumer: %s", err)
	}

	partition, err := consumer.ConsumePartition(*topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Error while creating partition consumer: %s", err)
	}

	defer func() {
		if err := partition.Close(); err != nil {
			log.Println("Failed to close partition consumer", err)
		}
		if err := consumer.Close(); err != nil {
			log.Println("Failed to close consumer", err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	signal.Notify(signals, os.Kill)

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partition.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			value := string(msg.Value)
			log.Printf("%+v\n", value)
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}
