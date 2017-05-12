package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/metno/productshredder/productstatus"
)

var (
	brokers          = flag.String("brokers", os.Getenv("KAFKA_BROKERS"), "The Kafka brokers to connect to, as a comma separated list")
	topic            = flag.String("topic", os.Getenv("KAFKA_TOPIC"), "The Kafka brokers to connect to, as a comma separated list")
	ssl              = flag.Bool("ssl", false, "Use SSL for Kafka connection")
	productstatusUrl = flag.String("productstatus", os.Getenv("PRODUCTSTATUS_URL"), "URL to the Productstatus web service")
	username         = flag.String("username", os.Getenv("PRODUCTSTATUS_URL"), "Productstatus username")
	apiKey           = flag.String("apikey", os.Getenv("PRODUCTSTATUS_URL"), "Productstatus API key")
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
	config.Net.TLS.Enable = *ssl
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

	productstatusClient, err := productstatus.New(*productstatusUrl, *username, *apiKey)
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
	dataInstances := make(chan *productstatus.DataInstance, 1024)
	patches := make(chan *productstatus.DataInstance, 1024)

	// Start processing coroutines
	go func() {
		for {
			m := <-messages
			errors <- handleExpired(productstatusClient, m, dataInstances)
		}
	}()
	go func() {
		for {
			dataInstance := <-dataInstances
			errors <- handleDelete(dataInstance, patches)
		}
	}()
	go func() {
		for {
			dataInstance := <-patches
			errors <- handlePatch(productstatusClient, dataInstance)
		}
	}()

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
			if err != nil {
				fmt.Printf("%s\n", err)
			}

		case <-signals:
			break ConsumerLoop
		}
	}

	fmt.Printf("Consumed: %d\n", consumed)
}

// rm removes files from physical media using the 'rm' shell command.
func rm(path string) error {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	cmd := exec.Command("rm", "-fv", path)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err == nil {
		return err
	}

	switch err.(type) {
	case *exec.ExitError:
		msg := strings.Trim(stderr.String(), " \n")
		return fmt.Errorf("%s: %s", err, msg)
	}

	return err
}

// handlePatch updates a DataInstance resource remotely, marking it as deleted.
func handlePatch(c *productstatus.Client, dataInstance *productstatus.DataInstance) error {
	err := c.DeleteResource(dataInstance)
	if err != nil {
		return fmt.Errorf("Unable to mark resource '%s' as deleted: %s", dataInstance.Resource_uri, err)
	}
	fmt.Printf("Resource '%s' has been marked as deleted in Productstatus.\n", dataInstance.Resource_uri)
	return nil
}

// handleDelete physically removes the file pointed to by a DataInstance.
func handleDelete(dataInstance *productstatus.DataInstance, patch chan *productstatus.DataInstance) error {
	url, err := url.Parse(dataInstance.Url)
	if err != nil {
		return fmt.Errorf("%s: error while parsing DataInstance URL: %s", dataInstance.Url, err)
	}

	if url.Scheme != "file" {
		return fmt.Errorf("%s: productshredder can only delete files with URL scheme 'file'", dataInstance.Url)
	}

	err = rm(url.Path)
	if err != nil {
		return fmt.Errorf("Failed to delete '%s': %s", url.Path, err)
	}

	fmt.Printf("Deleted: '%s'\n", url.Path)

	patch <- dataInstance

	return nil
}

// handleExpired reads an expired message, and queues all its DataInstance resources for deletion.
func handleExpired(c *productstatus.Client, m *Message, dataInstances chan *productstatus.DataInstance) error {
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
			return err
		}

		dataInstances <- dataInstance
	}

	return nil
}
