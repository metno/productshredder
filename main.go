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
	dryRun           = flag.Bool("dry-run", false, "Disable all write operations")
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

// ProductstatusMessage holds a normalized Productstatus message.
type ProductstatusMessage struct {
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

// Job holds the completion state of a delete event.
type Job struct {
	Message      *ProductstatusMessage
	Client       *productstatus.Client
	DataInstance *productstatus.DataInstance
	Error        error
}

func logJob(job Job, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	message = fmt.Sprintf("[%s] %s", job.Message.Message_id, message)
	log.Printf("%s\n", message)
}

// T returns the type of Productstatus message.
func (m *ProductstatusMessage) T() int {
	return msgTypes[m.Type]
}

// readMessage parses a JSON marshalled Productstatus message, and returns a Message struct.
func readMessage(kafkaMessage []byte) (*ProductstatusMessage, error) {
	var err error

	message := &ProductstatusMessage{
		Version: make([]int, 0),
		Uris:    make([]string, 0),
	}
	reader := bytes.NewReader(kafkaMessage)
	decoder := json.NewDecoder(reader)

	// decode an array value (ProductstatusMessage)
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

	log.SetPrefix("[productshredder] ")
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
	jobQueue := make(chan Job, 1024)
	finishQueue := make(chan Job, 1024)
	deleteQueue := make(chan Job, 1024)
	patchQueue := make(chan Job, 1024)

	//
	// Start processing coroutines
	//

	// Read from job queue, get a list of data instance objects, and pass them
	// to the delete handler.
	go func() {
		for {
			job := <-jobQueue
			handleExpired(job, deleteQueue, finishQueue)
		}
	}()

	// Get deletion jobs from queue, delete files, and pass them to the patch handler.
	go func() {
		for {
			job := <-deleteQueue
			handleDelete(job, patchQueue, finishQueue)
		}
	}()
	go func() {
		for {
			job := <-patchQueue
			handlePatch(job, finishQueue)
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
			message, err := readMessage(msg.Value)
			if err != nil {
				log.Printf("Error decoding message: %s\n", err)
				continue
			}
			job := Job{
				Message: message,
				Client:  productstatusClient,
			}

			switch message.T() {
			case HEARTBEAT:
				logJob(job, "Heartbeat, server time is %s", job.Message.Message_timestamp)
			case RESOURCE:
				logJob(job, "Resource of type '%s' at '%s'", job.Message.Resource, message.Uri)
			case EXPIRED:
				logJob(job, "Expire event for %d data instances on product '%s', service backend '%s'", len(job.Message.Uris), job.Message.Product, message.Service_backend)
				if !inSlice(job.Message.Product, productUUIDs) {
					logJob(job, "Expired event filtered out because it doesn't have the correct Product UUID.")
					continue
				}
				if !inSlice(job.Message.Service_backend, backendUUIDs) {
					logJob(job, "Expired event filtered out because it doesn't have the correct ServiceBackend UUID.")
					continue
				}

				jobQueue <- job
			}

		case job := <-finishQueue:
			if job.Error != nil {
				logJob(job, "Error: %s", job.Error)
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

// handleExpired reads an expired message, and queues all its DataInstance resources for deletion.
func handleExpired(job Job, deleteQueue chan Job, finishQueue chan Job) {
	for _, uri := range job.Message.Uris {
		job.DataInstance, job.Error = job.Client.GetDataInstance(uri)
		if job.Error == nil {
			deleteQueue <- job
		} else {
			finishQueue <- job
		}
	}
}

// handleDelete physically removes the file pointed to by a DataInstance.
func handleDelete(job Job, patchQueue chan Job, finishQueue chan Job) {
	url, err := url.Parse(job.DataInstance.Url)

	if err != nil {
		job.Error = fmt.Errorf("%s: error while parsing DataInstance URL: %s", job.DataInstance.Url, err)
		finishQueue <- job
		return
	}

	if url.Scheme != "file" {
		job.Error = fmt.Errorf("%s: productshredder can only delete files with URL scheme 'file'", job.DataInstance.Url)
		finishQueue <- job
		return
	}

	prefix := ""
	if *dryRun {
		prefix = "[DRY-RUN] "
	} else {
		err = rm(url.Path)
		if err != nil {
			job.Error = fmt.Errorf("Failed to delete '%s': %s", url.Path, err)
			finishQueue <- job
			return
		}
	}

	logJob(job, "%sDeleted: '%s'", prefix, url.Path)

	patchQueue <- job
}

// handlePatch updates a DataInstance resource remotely, marking it as deleted.
func handlePatch(job Job, finishQueue chan Job) {
	prefix := ""
	if *dryRun {
		prefix = "[DRY-RUN] "
	} else {
		err := job.Client.DeleteResource(job.DataInstance)
		if err != nil {
			job.Error = fmt.Errorf("Unable to mark resource '%s' as deleted: %s", job.DataInstance.Resource_uri, err)
			finishQueue <- job
			return
		}
	}
	logJob(job, "%sResource '%s' has been marked as deleted in Productstatus.", prefix, job.DataInstance.Resource_uri)
	finishQueue <- job
}
