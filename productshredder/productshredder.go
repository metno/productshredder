package productshredder

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/metno/productshredder/productstatus"
)

var (
	ApiKey           = flag.String("apikey", os.Getenv("PRODUCTSTATUS_API_KEY"), "Productstatus API key")
	Brokers          = flag.String("brokers", os.Getenv("KAFKA_BROKERS"), "The Kafka brokers to connect to, as a comma separated list")
	DryRun           = flag.Bool("dry-run", false, "Disable all write operations")
	Offset           = flag.Int64("offset", sarama.OffsetNewest, "Kafka message offset to start reading from")
	Products         = flag.String("products", "", "Which products backends to process expired messages for")
	ProductstatusUrl = flag.String("productstatus", os.Getenv("PRODUCTSTATUS_URL"), "URL to the Productstatus web service")
	ServiceBackends  = flag.String("servicebackends", "", "Which service backends to process expired messages for")
	Ssl              = flag.Bool("ssl", false, "Use SSL for Kafka connection")
	Topic            = flag.String("topic", os.Getenv("KAFKA_TOPIC"), "The Kafka brokers to connect to, as a comma separated list")
	Username         = flag.String("username", os.Getenv("PRODUCTSTATUS_USERNAME"), "Productstatus username")
	Verbose          = flag.Bool("verbose", false, "Turn on Sarama logging")
	VerifySsl        = flag.Bool("verify", true, "Verify SSL certificates chain")
)

// Productstatus message types
const (
	UNKNOWN = iota
	HEARTBEAT
	RESOURCE
	EXPIRED
)

// msgTypes maps string message types to integer constants
var MsgTypes = map[string]int{
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

func LogJob(job Job, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	message = fmt.Sprintf("[%s] %s", job.Message.Message_id, message)
	log.Printf("%s\n", message)
}

// T returns the type of Productstatus message.
func (m *ProductstatusMessage) T() int {
	return MsgTypes[m.Type]
}

// readMessage parses a JSON marshalled Productstatus message, and returns a Message struct.
func ReadMessage(kafkaMessage []byte) (*ProductstatusMessage, error) {
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
func NewConsumer(brokerList []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Net.TLS.Enable = *Ssl
	config.Net.KeepAlive = 30
	config.Consumer.Return.Errors = false // FIXME
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

// inSlice returns true if the given string is in the list, or if the list is empty.
func InSlice(s string, slice []string) bool {
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

// handlePatch updates a DataInstance resource remotely, marking it as deleted.
func HandlePatch(job Job, finishQueue chan Job) {
	prefix := ""
	if *DryRun {
		prefix = "[DRY-RUN] "
	} else {
		err := job.Client.DeleteResource(job.DataInstance)
		if err != nil {
			job.Error = fmt.Errorf("Unable to mark resource '%s' as deleted: %s", job.DataInstance.Resource_uri, err)
			finishQueue <- job
			return
		}
	}
	LogJob(job, "%sResource '%s' has been marked as deleted in Productstatus.", prefix, job.DataInstance.Resource_uri)
	finishQueue <- job
}

// handleExpired reads an expired message, and queues all its DataInstance resources for deletion.
func HandleExpired(job Job, deleteQueue chan Job, finishQueue chan Job) {
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
func HandleDelete(job Job, patchQueue chan Job, finishQueue chan Job) {
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
	if *DryRun {
		prefix = "[DRY-RUN] "
	} else {
		err = rm(url.Path)
		if err != nil {
			job.Error = fmt.Errorf("Failed to delete '%s': %s", url.Path, err)
			finishQueue <- job
			return
		}
	}

	LogJob(job, "%sDeleted: '%s'", prefix, url.Path)

	patchQueue <- job
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
