package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
)

func main() {
	logger := log.New(os.Stdout, "", 0)

	inputFileCli := flag.String("input", "./input.json", "Input file to split")
	outputDir := flag.String("output", "./output/", "Location to put all output files. Directory must exist and preferably be empty.")
	splitArrayLength := flag.Int("length", 100, "Number of array elements each split file should have")
	prettyPrint := flag.Bool("prettyPrint", false, "If present, the output is pretty printed")

	flag.Parse()

	f, err := os.Open(*inputFileCli)
	if err != nil {
		logger.Fatalf("open input file: %s", err)
	}
	defer func() {
		err := f.Close()
		if err != nil {
			logger.Fatalf("close input file: %s", err)
		}
	}()
	logger.Printf("Using input file %s", *inputFileCli)

	dec := json.NewDecoder(f)

	// read open bracket
	_, err = dec.Token()
	if err != nil {
		logger.Fatalf("read open bracket: %s", err)
	}
	logger.Printf("Found array start...")

	var wg sync.WaitGroup

	readBatchChan := make(chan []map[string]interface{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		batchCounter := 0
		for {
			debouncedLog(logger, batchCounter, "Reading batch %d...", batchCounter+1)

			batch, hasMore, err := readBatch(dec, *splitArrayLength)
			if err != nil {
				logger.Fatalf("read batch: %s", err)
			}

			readBatchChan <- batch
			if !hasMore {
				close(readBatchChan)
				break
			}

			batchCounter++
		}
	}()

	marshalledBatchChan := make(chan []byte)
	wg.Add(1)
	go func() {
		defer wg.Done()
		batchCounter := 0
		for batch := range readBatchChan {
			debouncedLog(logger, batchCounter, "Marshalling batch %d...", batchCounter+1)

			var bytes []byte
			if *prettyPrint {
				bytes, err = json.MarshalIndent(batch, "", "  ")
			} else {
				bytes, err = json.Marshal(batch)
			}
			if err != nil {
				logger.Fatalf("marshal batch: %s", err)
			}

			marshalledBatchChan <- bytes
			batchCounter++
		}
		close(marshalledBatchChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		batchCounter := 0
		for bytes := range marshalledBatchChan {
			outFileName := fmt.Sprintf("%s/part%d.json", *outputDir, batchCounter)
			debouncedLog(logger, batchCounter, "Writing batch to file '%s'...", outFileName)
			err = os.WriteFile(outFileName, bytes, 0600)
			if err != nil {
				logger.Fatalf("write batch: %s", err)
			}

			batchCounter++
		}
	}()

	wg.Wait()

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		logger.Fatalf("read closing bracket: %s", err)
	}

	logger.Println("Done")
}

func readBatch(dec *json.Decoder, batchSize int) (batch []map[string]interface{}, hasMore bool, err error) {
	batch = make([]map[string]interface{}, batchSize)

	for i := range batch {
		// while the array contains values
		if dec.More() {
			var m map[string]interface{}
			// decode an array value
			err := dec.Decode(&m)
			if err != nil {
				return nil, false, err
			}
			batch[i] = m
		} else {
			return batch, false, nil
		}
	}

	return batch, true, nil
}

func debouncedLog(logger *log.Logger, bounceCounter int, format string, args ...interface{}) {
	if bounceCounter < 10 ||
		(bounceCounter < 100 && bounceCounter%10 == 0) ||
		(bounceCounter < 1000 && bounceCounter%100 == 0) {
		logger.Printf(format, args)

	}
}
