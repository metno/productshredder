package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/metno/productshredder/productstatus"
)

var (
	brokers          = flag.String("brokers", os.Getenv("KAFKA_BROKERS"), "The Kafka brokers to connect to, as a comma separated list")
	topic            = flag.String("topic", os.Getenv("KAFKA_TOPIC"), "The Kafka brokers to connect to, as a comma separated list")
	productstatusUrl = flag.String("productstatus", os.Getenv("PRODUCTSTATUS_URL"), "URL to the Productstatus web service")
	verbose          = flag.Bool("verbose", false, "Turn on Sarama logging")
	verifySsl        = flag.Bool("verify", true, "Verify SSL certificates chain")
	offset           = flag.Int64("offset", sarama.OffsetNewest, "Kafka message offset to start reading from")
)

// Productstatus message types
const (
	UNKNOWN = iota
	HEARTBEAT
	RESOURCE
	EXPIRED
)

// msgTypes maps string message types to integer constants
var msgTypes = map[string]int{
	"heartbeat": HEARTBEAT,
	"resource":  RESOURCE,
	"expired":   EXPIRED,
}

// Message holds a normalized Productstatus message.
type Message struct {
	Message_id        string
	Message_timestamp string
	Product           string
	Resource          string
	Service_backend   string
	Type              string
	Uri               string
	Uris              []string
	Version           []int
}

// T returns the type of Productstatus message.
func (m *Message) T() int {
	return msgTypes[m.Type]
}

// readMessage parses a JSON marshalled Productstatus message, and returns a Message struct.
func readMessage(kafkaMessage []byte) (*Message, error) {
	var err error

	message := &Message{
		Version: make([]int, 0),
		Uris:    make([]string, 0),
	}
	reader := bytes.NewReader(kafkaMessage)
	decoder := json.NewDecoder(reader)

	// decode an array value (Message)
	err = decoder.Decode(message)
	if err != nil {
		return message, fmt.Errorf("while decoding body: %s", err)
	}

	return message, nil
}

// newConsumer returns a Kafka consumer.
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

	productstatusClient, err := productstatus.New(*productstatusUrl)
	if err != nil {
		fmt.Printf("Error while creating Productstatus client: %s\n", err)
		os.Exit(1)
	}
	productstatusClient.Get("/api/v1/")

	brokerList := strings.Split(*brokers, ",")
	fmt.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	consumer, err := newConsumer(brokerList)
	if err != nil {
		fmt.Printf("Error while creating consumer: %s", err)
		os.Exit(1)
	}

	partition, err := consumer.ConsumePartition(*topic, 0, *offset)
	if err != nil {
		fmt.Printf("Error while creating partition consumer: %s\n", err)
		os.Exit(1)
	}

	defer func() {
		if err := partition.Close(); err != nil {
			fmt.Println("Failed to close partition consumer", err)
		}
		if err := consumer.Close(); err != nil {
			fmt.Println("Failed to close consumer", err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	signal.Notify(signals, os.Kill)

	// Message and error queues
	errors := make(chan error, 1024)
	messages := make(chan *Message, 1024)
	dataInstances := make(chan *productstatus.DataInstance)

	// Start processing coroutines
	go handleExpired(productstatusClient, messages, dataInstances, errors)
	go handleDelete(productstatusClient, dataInstances, errors)

	// Consume messages from Kafka and post them to handling functions
	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partition.Messages():
			fmt.Printf("[%9d] ", msg.Offset)
			message, err := readMessage(msg.Value)
			if err != nil {
				fmt.Printf("Error decoding message: %s\n", err)
				continue
			}
			switch message.T() {
			case HEARTBEAT:
				fmt.Printf("Heartbeat, server time is %s\n", message.Message_timestamp)
			case RESOURCE:
				fmt.Printf("Resource of type '%s' at '%s'\n", message.Resource, message.Uri)
			case EXPIRED:
				fmt.Printf("Expire event for %d data instances on product '%s', service backend '%s'\n", len(message.Uris), message.Product, message.Service_backend)
				messages <- message
			}

		case err := <-errors:
			fmt.Printf("ERROR: %s", err)

		case <-signals:
			break ConsumerLoop
		}
	}

	fmt.Printf("Consumed: %d\n", consumed)
}

func handleDelete(c *productstatus.Client, dataInstances chan *productstatus.DataInstance, errors chan error) {
	for {
		dataInstance := <-dataInstances

		fmt.Printf("- [expired: %s] %s\n", dataInstance.Expires, dataInstance.Url)
	}
}

func handleExpired(c *productstatus.Client, messages chan *Message, dataInstances chan *productstatus.DataInstance, errors chan error) {
	for {
		m := <-messages

		/*
			product, err := c.GetProduct(m.Product)
			if err != nil {
				errors <- err
				return
			}

			serviceBackend, err := c.GetServiceBackend(m.Service_backend)
			if err != nil {
				errors <- err
				return
			}
		*/

		for _, uri := range m.Uris {
			dataInstance, err := c.GetDataInstance(uri)
			if err != nil {
				errors <- err
				continue
			}

			dataInstances <- dataInstance
		}
	}
}
