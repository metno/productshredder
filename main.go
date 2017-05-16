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
	apiKey           = flag.String("apikey", os.Getenv("PRODUCTSTATUS_API_KEY"), "Productstatus API key")
	brokers          = flag.String("brokers", os.Getenv("KAFKA_BROKERS"), "The Kafka brokers to connect to, as a comma separated list")
	offset           = flag.Int64("offset", sarama.OffsetNewest, "Kafka message offset to start reading from")
	products         = flag.String("products", "", "Which products backends to process expired messages for")
	productstatusUrl = flag.String("productstatus", os.Getenv("PRODUCTSTATUS_URL"), "URL to the Productstatus web service")
	serviceBackends  = flag.String("servicebackends", "", "Which service backends to process expired messages for")
	ssl              = flag.Bool("ssl", false, "Use SSL for Kafka connection")
	topic            = flag.String("topic", os.Getenv("KAFKA_TOPIC"), "The Kafka brokers to connect to, as a comma separated list")
	username         = flag.String("username", os.Getenv("PRODUCTSTATUS_USERNAME"), "Productstatus username")
	verbose          = flag.Bool("verbose", false, "Turn on Sarama logging")
	verifySsl        = flag.Bool("verify", true, "Verify SSL certificates chain")
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

// inSlice returns true if the given string is in the list, or if the list is empty.
func inSlice(s string, slice []string) bool {
	if len(slice) == 0 {
		return true
	}
	for _, t := range slice {
		if s == t {
			return true
		}
	}
	return false
}

func main() {
	flag.Parse()

	log.SetPrefix("[productscredder] ")
	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	productstatusClient, err := productstatus.New(*productstatusUrl, *username, *apiKey)
	if err != nil {
		log.Printf("Error while creating Productstatus client: %s\n", err)
		os.Exit(1)
	}
	productstatusClient.Get("/api/v1/")

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	consumer, err := newConsumer(brokerList)
	if err != nil {
		log.Printf("Error while creating consumer: %s", err)
		os.Exit(1)
	}

	partition, err := consumer.ConsumePartition(*topic, 0, *offset)
	if err != nil {
		log.Printf("Error while creating partition consumer: %s\n", err)
		os.Exit(1)
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

	// Only delete DataInstances belonging in this list (if list is non-empty)
	ffunc := func(r rune) bool {
		return r == ','
	}
	backendUUIDs := strings.FieldsFunc(*serviceBackends, ffunc)
	productUUIDs := strings.FieldsFunc(*products, ffunc)

	// Consume messages from Kafka and post them to handling functions
	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partition.Messages():
			log.Printf("[%9d] ", msg.Offset)
			message, err := readMessage(msg.Value)
			if err != nil {
				log.Printf("Error decoding message: %s\n", err)
				continue
			}
			switch message.T() {
			case HEARTBEAT:
				log.Printf("Heartbeat, server time is %s\n", message.Message_timestamp)
			case RESOURCE:
				log.Printf("Resource of type '%s' at '%s'\n", message.Resource, message.Uri)
			case EXPIRED:
				log.Printf("Expire event for %d data instances on product '%s', service backend '%s'\n", len(message.Uris), message.Product, message.Service_backend)
				if !inSlice(message.Product, productUUIDs) {
					log.Printf("Expired event filtered out because it doesn't have the correct Product UUID.\n")
					continue
				}
				if !inSlice(message.Service_backend, backendUUIDs) {
					log.Printf("Expired event filtered out because it doesn't have the correct ServiceBackend UUID.\n")
					continue
				}
				messages <- message
			}

		case err := <-errors:
			if err != nil {
				log.Printf("%s\n", err)
			}

		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
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
	log.Printf("Resource '%s' has been marked as deleted in Productstatus.\n", dataInstance.Resource_uri)
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

	log.Printf("Deleted: '%s'\n", url.Path)

	patch <- dataInstance

	return nil
}

// handleExpired reads an expired message, and queues all its DataInstance resources for deletion.
func handleExpired(c *productstatus.Client, m *Message, dataInstances chan *productstatus.DataInstance) error {
	for _, uri := range m.Uris {
		dataInstance, err := c.GetDataInstance(uri)
		if err != nil {
			return err
		}
		dataInstances <- dataInstance
	}
	return nil
}
