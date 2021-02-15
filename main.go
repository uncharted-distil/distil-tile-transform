package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/uncharted-distil/tile-tx/analytics"
	log "github.com/unchartedsoftware/plog"
)

func main() {
	inputDir := flag.String("input", ".", "Input directory containing geotiff files.")
	operation := flag.String("operation", "mean_NDVI", "Operation to perform on the tiles.")
	flag.Parse()

	fmt.Print("scanning directory...\n")

	// Read the directory to get the list of files
	filePaths, err := ioutil.ReadDir(*inputDir)
	if err != nil {
		log.Error(err)
		return
	}

	fmt.Print("processing tiles...\n")

	tileMap := map[string][]analytics.Tile{}
	parsedTiles := map[string]bool{}
	for _, filePath := range filePaths {

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
			ID:        id,
			Date:      dateString,
			Timestamp: date.Unix(),
		}
		if _, ok := tileMap[id]; !ok {
			tileMap[id] = []analytics.Tile{}
		}
		tileMap[id] = insertSorted(tileMap[id], tileInfo)
	}

	csvFile, err := os.Create("output.csv")
	if err != nil {
		log.Warnf("failed to create csv file")
		os.Exit(1)
	}
	defer csvFile.Close()

	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	var tileAnalytic analytics.Transformer
	if *operation == analytics.OperationCategoryCounts {
		tileAnalytic, err = analytics.NewCategoryCounts(*inputDir)
	} else if *operation == analytics.OperationMeanNDVI {
		tileAnalytic = analytics.MeanNDVI{}
	}

	if err != nil {
		log.Error(err, "could initialize tile analytic")
		os.Exit(1)
	}

	// write the header row
	err = csvWriter.Write(append([]string{"tile_id", "date"}, tileAnalytic.ValueNames()...))
	if err != nil {
		log.Error(err, "could not write csv header")
		os.Exit(1)
	}

	// Run analytic for each tile at each timestep and write to our csv file
	errorCount := 0
	var lastError error
	processedCount := 0
	total := 0
	for _, tiles := range tileMap {
		if total == 0 {
			total = len(tileMap) * len(tiles)
		}
		for _, tile := range tiles {
			processedCount++
			if processedCount%100 == 0 {
				log.Infof("processed %d / %d", processedCount, total)
			}

			// Load the required tile images and run the tile transform on them.
			images, err := tileAnalytic.Setup(*inputDir, &tile)
			if err != nil {
				lastError = err
				errorCount++
				continue
			}
			values, err := tileAnalytic.Transform(images)
			if err != nil {
				lastError = err
				errorCount++
				continue
			}

			// Reformat the results
			formattedValues := make([]string, len(values))
			for i, value := range values {
				formattedValues[i] = strconv.FormatFloat(value, 'f', -1, 64)
			}

			// Reformat the tile timestamp to YYYY-MM-DD.
			d := time.Unix(tile.Timestamp, 0).Format("2006-01-02")

			// Write the tile ID, date and value to the CSV as row data.
			if err = csvWriter.Write(append([]string{tile.ID, d}, formattedValues...)); err != nil {
				lastError = err
				errorCount++
				continue
			}
		}
	}

	if lastError != nil {
		log.Warnf("encountered %d errors - last: %s", errorCount, lastError)
	}
}

// Inserts a tile into list sorted by date.
func insertSorted(tiles []analytics.Tile, t analytics.Tile) []analytics.Tile {
	index := sort.Search(len(tiles), func(i int) bool { return tiles[i].Timestamp > t.Timestamp })
	tiles = append(tiles, analytics.Tile{})
	copy(tiles[index+1:], tiles[index:])
	tiles[index] = t
	return tiles
}