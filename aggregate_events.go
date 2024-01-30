package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

type Event struct {
	UserID    int    `json:"userId"`
	EventType string `json:"eventType"`
	Timestamp int64  `json:"timestamp"`
}

type AggregatedData map[int]map[string]map[string]int

func aggregateEvents(inputFile string, outputFile string, update bool) {
	fileData, err := os.ReadFile(inputFile)
	if err != nil {
		fmt.Println("Error reading input file:", err)
		return
	}

	// unmarshal events from input file
	var events []Event
	if err := json.Unmarshal(fileData, &events); err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
		return
	}

	aggregatedData := make(AggregatedData)

	for _, event := range events {
		timestamp := time.Unix(event.Timestamp, 0).UTC().Format("2006-01-02")

		if _, ok := aggregatedData[event.UserID]; !ok {
			aggregatedData[event.UserID] = make(map[string]map[string]int)
		}

		if _, ok := aggregatedData[event.UserID][timestamp]; !ok {
			aggregatedData[event.UserID][timestamp] = make(map[string]int)
		}

		aggregatedData[event.UserID][timestamp][event.EventType]++
	}

	var result []map[string]interface{}

	for userID, userData := range aggregatedData {
		for timestamp, eventTypes := range userData {
			entry := map[string]interface{}{"userId": userID, "date": timestamp}
			for eventType, count := range eventTypes {
				entry[eventType] = count
			}
			result = append(result, entry)
		}
	}

	outputData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Println("Error marshalling JSON:", err)
		return
	}

	err = os.WriteFile(outputFile, outputData, 0644)
	if err != nil {
		fmt.Println("Error writing to output file:", err)
		return
	}

	if update {
		fmt.Println("Real-time update enabled. Watching for changes...")
		watchForUpdates(inputFile, outputFile)
	}
}

func watchForUpdates(inputFile string, outputFile string) {
	lastModified := time.Now()

	for {
		info, err := os.Stat(inputFile)
		if err != nil {
			fmt.Println("Error checking file status:", err)
			return
		}

		if info.ModTime().After(lastModified) {
			fmt.Println("File updated. Updating output...")
			lastModified = info.ModTime()
			aggregateEvents(inputFile, outputFile, false)
		}

		time.Sleep(2 * time.Second)
	}
}

func main() {
	inputFile := flag.String("i", "", "Input JSON file containing user events")
	outputFile := flag.String("o", "", "Output JSON file for daily summary reports")
	update := flag.Bool("update", false, "Update output file in real-time")

	flag.Parse()

	// return an error statement that input and output files are required
	if *inputFile == "" || *outputFile == "" {
		fmt.Println("Input and output file paths are required")
		return
	}

	aggregateEvents(*inputFile, *outputFile, *update)
}
