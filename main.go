package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/uncharted-distil/tile-tx/analytics"
	log "github.com/unchartedsoftware/plog"
)

const (
	metadataFileName = "metadata.json"
)

func main() {
	inputDir := flag.String("input", ".", "Input directory containing geotiff files.")
	outputFile := flag.String("output", ".", "Output file path.")
	operation := flag.String("operation", "mean_NDVI", "Operation to perform on the tiles.")
	workers := *flag.Int("workers", 8, "number of workers")
	flag.Parse()

	// Load the metadata associated with the tile dataset
	metadata, err := loadMetadata(*inputDir)
	if err != nil {
		log.Error(err, "could not load dataset metadata")
		os.Exit(1)
	}

	// Instantiate a tile analytic based on the operation specified in the command line params
	tileAnalytic, err := analytics.CreateTileAnalytic(metadata, analytics.Operation(*operation))
	if err != nil {
		log.Error(err, "could initialize tile analytic")
		os.Exit(1)
	}

	// Initialize output CSV file
	dir := path.Dir(*outputFile)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			log.Error(err, "failed to create output directory")
			os.Exit(1)
		}
	}
	csvFile, err := os.Create(*outputFile)
	if err != nil {
		log.Error("failed to create csv file")
		os.Exit(1)
	}
	defer csvFile.Close()

	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	// write the header row
	err = csvWriter.Write(append([]string{"tile_id", "date", "bounds"}, tileAnalytic.ValueNames()...))
	if err != nil {
		log.Error(err, "could not write csv header")
		os.Exit(1)
	}

	// generate row data from tiles
	rows := processTiles(workers, *inputDir, tileAnalytic)

	// write out results
	for _, row := range rows {
		if err = csvWriter.Write(row); err != nil {
			continue
		}
	}
}

// apply analytic operation to tiles and write results out as a row data
func processTiles(workers int, inputDir string, tileAnalytic analytics.Transformer) [][]string {
	// Scan the input dir and collect tile information by parsing each file name
	tileMap, err := createTileMap(inputDir)
	if err != nil {
		log.Warnf("failed to read tile information")
		os.Exit(1)
	}

	// flatten tilemap to an array
	tileArray := []analytics.Tile{}
	for _, tiles := range tileMap {
		tileArray = append(tileArray, tiles...)
	}

	results := make(chan []string, len(tileArray))
	tiles := make(chan analytics.Tile, len(tileArray))

	var wg sync.WaitGroup
	wg.Add(workers)

	// Start workers.  Mileage will vary given that IO is the bottleneck, and HDD
	// reads don't parallelize.  SSD will allow for parallel reads, and you should
	// get some OS level cacheing in either case if the tile data has been loaded recently.
	for i := 0; i < workers; i++ {
		go tileWorker(i, tiles, results, &wg, tileAnalytic, inputDir)
	}

	// Send all of the tiles to the workers
	for _, tile := range tileArray {
		tiles <- tile
	}
	close(tiles)

	// Wait for workers to finish
	go func() {
		defer close(results)
		wg.Wait()
	}()

	// Collect the results
	rows := [][]string{}
	for r := range results {
		rows = append(rows, r)
	}

	return rows
}

// Processes a tile batch.
func tileWorker(worker int, tiles chan analytics.Tile, results chan []string,
	wg *sync.WaitGroup, tileAnalytic analytics.Transformer, inputDir string) {

	setupErrCount := 0
	var lastSetupErr error

	transformErrCount := 0
	var lastTransformErr error

	for tile := range tiles {
		count := 0
		count++
		if count%100 == 0 {
			log.Infof("worker %d: processed %d", worker, count)
		}

		// Load the required tile images and run the tile transform on them.
		images, err := tileAnalytic.Setup(inputDir, &tile)
		if err != nil {
			setupErrCount++
			lastSetupErr = err
			continue
		}
		values, err := tileAnalytic.Transform(images)
		if err != nil {
			transformErrCount++
			lastTransformErr = err
			continue
		}

		// Reformat the results
		formattedValues := make([]string, len(values))
		for i, value := range values {
			formattedValues[i] = strconv.FormatFloat(value, 'f', -1, 64)
		}

		// Reformat the tile timestamp to YYYY-MM-DD.
		date := time.Unix(tile.Timestamp, 0).Format("2006-01-02")

		// Extract the geobounds from the first image
		geoBounds := images[0].Bounds

		row := []string{tile.GeoHash, date, geoBounds.String()}
		row = append(row, formattedValues...)

		results <- row
	}

	log.Infof("worker %d: tile processing complete", worker)

	if setupErrCount > 0 {
		log.Warnf("encountered %d setup errors", setupErrCount)
		log.Warnf("last setup error: %s", lastSetupErr)
	}

	if transformErrCount > 0 {
		log.Warnf("encountered %d transform errors", transformErrCount)
		log.Warnf("last transform error: %s", lastTransformErr)
	}

	wg.Done()
}

// Creates entries for tile data by parsing file names.  Entries are mapped
// by a derived ID.
func createTileMap(inputDir string) (map[string][]analytics.Tile, error) {

	log.Infof("scanning directory")

	// Read the directory to get the list of files
	filePaths, err := ioutil.ReadDir(inputDir)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	// Process the tile paths - will skip any bad records encountered
	log.Infof("processing %d tiles", len(filePaths))

	tileMap := map[string][]analytics.Tile{}
	parsedTiles := map[string]bool{}
	for _, filePath := range filePaths {
		// ignore the metadata file
		if filePath.Name() == metadataFileName {
			continue
		}

		// parse the file into tile id, date
		splitPath := strings.Split(filePath.Name(), "_")
		if len(splitPath) < 2 {
			log.Warnf("improperly formatted file name %s", splitPath)
			continue
		}

		// parse the ID
		id := splitPath[0]

		// parse the date
		dateString := splitPath[1]
		layout := "20060102T030405"
		date, err := time.Parse(layout, dateString)
		if err != nil {
			log.Warnf("cannot parse date %s", dateString)
			continue
		}

		// track the unique id/date combinations so that we only generate one tile
		// entry per id/date pair
		tileKey := fmt.Sprintf("%s_%s", id, dateString)
		if _, ok := parsedTiles[tileKey]; ok {
			continue
		}
		parsedTiles[tileKey] = true

		// store it in our map
		tileInfo := analytics.Tile{
			GeoHash:   id,
			Date:      dateString,
			Timestamp: date.Unix(),
		}
		if _, ok := tileMap[id]; !ok {
			tileMap[id] = []analytics.Tile{}
		}
		tileMap[id] = insertSorted(tileMap[id], tileInfo)
	}
	return tileMap, nil
}

// Loads the associated JSON config from the dataset folder and returns
// it as a string for processing by analytic config
func loadMetadata(inputDir string) (analytics.JSONString, error) {
	path := path.Join(inputDir, "metadata.json")
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return "", errors.Wrapf(err, "failed to iniialize metadata")
	}
	return analytics.JSONString(raw), nil
}

// Inserts a tile into list sorted by date.
func insertSorted(tiles []analytics.Tile, t analytics.Tile) []analytics.Tile {
	index := sort.Search(len(tiles), func(i int) bool { return tiles[i].Timestamp > t.Timestamp })
	tiles = append(tiles, analytics.Tile{})
	copy(tiles[index+1:], tiles[index:])
	tiles[index] = t
	return tiles
}
