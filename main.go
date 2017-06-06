package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	. "github.com/metno/productshredder/productshredder"
	"github.com/metno/productshredder/productstatus"
)

func main() {
	flag.Parse()

	log.SetPrefix("[productshredder] ")
	if *Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if *Brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	productstatusClient, err := productstatus.New(*ProductstatusUrl, *Username, *ApiKey)
	if err != nil {
		log.Printf("Error while creating Productstatus client: %s\n", err)
		os.Exit(1)
	}
	productstatusClient.Get("/api/v1/")

	brokerList := strings.Split(*Brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	consumer, err := NewConsumer(brokerList)
	if err != nil {
		log.Printf("Error while creating consumer: %s", err)
		os.Exit(1)
	}

	partition, err := consumer.ConsumePartition(*Topic, 0, *Offset)
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
			HandleExpired(job, deleteQueue, finishQueue)
		}
	}()

	// Get deletion jobs from queue, delete files, and pass them to the patch handler.
	go func() {
		for {
			job := <-deleteQueue
			HandleDelete(job, patchQueue, finishQueue)
		}
	}()
	go func() {
		for {
			job := <-patchQueue
			HandlePatch(job, finishQueue)
		}
	}()

	// Only delete DataInstances belonging in this list (if list is non-empty)
	ffunc := func(r rune) bool {
		return r == ','
	}
	backendUUIDs := strings.FieldsFunc(*ServiceBackends, ffunc)
	productUUIDs := strings.FieldsFunc(*Products, ffunc)

	// Consume messages from Kafka and post them to handling functions
	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partition.Messages():
			message, err := ReadMessage(msg.Value)
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
				LogJob(job, "Heartbeat, server time is %s", job.Message.Message_timestamp)
			case RESOURCE:
				LogJob(job, "Resource of type '%s' at '%s'", job.Message.Resource, message.Uri)
			case EXPIRED:
				LogJob(job, "Expire event for %d data instances on product '%s', service backend '%s'", len(job.Message.Uris), job.Message.Product, message.Service_backend)
				if !InSlice(job.Message.Product, productUUIDs) {
					LogJob(job, "Expired event filtered out because it doesn't have the correct Product UUID.")
					continue
				}
				if !InSlice(job.Message.Service_backend, backendUUIDs) {
					LogJob(job, "Expired event filtered out because it doesn't have the correct ServiceBackend UUID.")
					continue
				}

				jobQueue <- job
			}

		case job := <-finishQueue:
			if job.Error != nil {
				LogJob(job, "Error: %s", job.Error)
			}

		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}
