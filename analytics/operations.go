package analytics

import (
	"fmt"
	"image"
	"math"
	"os"
	"path"

	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
	"github.com/uncharted-distil/gdal"
	log "github.com/unchartedsoftware/plog"
)

// Operation defines the type of the operation specifier
type Operation string

// JSONString defines a JSON object as a string
type JSONString string

const (
	// OperationCategoryCounts is the category counts operation.
	OperationCategoryCounts = "category_counts"

	// OperationMeanNDVI is the mean NDVI operation.
	OperationMeanNDVI = "mean_ndvi"

	// Constants for data sources
	// TODO: these should really be part of some configuration
	// file that is supplied and updated as new datasource are included.

	// sentinel constants
	band8        = "B08"
	band4        = "B04"
	sentinel2Max = 10000

	// copernicus land coverage constants
	discreteLandCoverBand           = "discrete_classification"
	discreteLandCoverCategoryValues = "discrete_classification_class_values"
	discreteLandCoverCategoryNames  = "discrete_classification_class_names"
)

// Tile is structure that provides geospatial tile information.
type Tile struct {
	ID        string
	Date      string
	Timestamp int64
}

// Transformer in an interface that defines an operation on tile data.
type Transformer interface {
	Setup(inputDir string, tile *Tile) ([]*image.Gray16, error)
	Transform(tileData []*image.Gray16) ([]float64, error)
	ValueNames() []string
}

// CreateTileAnalytic creates and initializes a tile analytic based on a requested operation
// type.
func CreateTileAnalytic(metadata JSONString, operation Operation) (Transformer, error) {
	var tileAnalytic Transformer
	var err error
	if operation == OperationCategoryCounts {
		tileAnalytic, err = NewCategoryCounts(metadata)
		if err != nil {
			return nil, err
		}
	} else if operation == OperationMeanNDVI {
		tileAnalytic = MeanNDVI{}
	} else {
		log.Warnf("unrecognized operation - defaulting to %s", OperationMeanNDVI)
		tileAnalytic = MeanNDVI{}
	}
	return tileAnalytic, nil
}

// MeanNDVI domputes mean NDVI for a sentinel-2 tiles
type MeanNDVI struct{}

// Transform implements the MeanNDVI tile transformation, which computes the average NDVI for a given tile.
func (m MeanNDVI) Transform(tileData []*image.Gray16) ([]float64, error) {
	sumNDVI := 0.0
	numValues := 0
	for i := 0; i < (tileData[0].Bounds().Max.X * tileData[0].Bounds().Max.Y * 2); i += 2 {
		// extract the 16 bit pixel values for each input band
		grayValue0 := uint16(tileData[0].Pix[i])<<8 | uint16(tileData[0].Pix[i+1])
		grayValue1 := uint16(tileData[1].Pix[i])<<8 | uint16(tileData[1].Pix[i+1])

		// compute NDVI ratio
		transformedValue := 0.0
		if grayValue0 != 0 || grayValue1 != 0 {
			transformedValue = math.Max(0, float64(int32(grayValue0)-int32(grayValue1))/float64(int32(grayValue0)+int32(grayValue1)))
		}
		sumNDVI += transformedValue
		numValues++
	}

	// compute the mean NDVI
	mean := sumNDVI / float64(numValues)
	return []float64{mean}, nil
}

// Setup loads the data for the MeanNDVI tile transformation.
func (m MeanNDVI) Setup(inputDir string, tile *Tile) ([]*image.Gray16, error) {
	band8FileName := fmt.Sprintf("%s_%s_%s.tif", tile.ID, tile.Date, band8)
	band8Path := path.Join(inputDir, band8FileName)
	band8Image, err := loadAsGray16(band8Path)
	if err != nil {
		log.Error(err, "band 8 file not loaded")
		os.Exit(1)
	}

	band4FileName := fmt.Sprintf("%s_%s_%s.tif", tile.ID, tile.Date, band4)
	band4Path := path.Join(inputDir, band4FileName)
	band4Image, err := loadAsGray16(band4Path)
	if err != nil {
		log.Error(err, "band 4 file not loaded")
		os.Exit(1)
	}

	return []*image.Gray16{band8Image, band4Image}, nil
}

// ValueNames returns the name of the Mean NDVI value.
func (m MeanNDVI) ValueNames() []string {
	return []string{"mean_ndvi"}
}

// CategoryData provides a category label and its associated numeric value
// from a raster.
type CategoryData struct {
	Value uint16
	Label string
}

// CategoryCounts computes number of pixels for each category of each value
type CategoryCounts struct {
	Categories []CategoryData
	IndexMap   map[uint16]int
}

// NewCategoryCounts create a new CategoryCounts tile operation
func NewCategoryCounts(metadata JSONString) (CategoryCounts, error) {
	labels, err := getCategoryNames(discreteLandCoverCategoryNames, metadata)
	if err != nil {
		return CategoryCounts{}, err
	}

	values, err := getCategoryValues(discreteLandCoverCategoryValues, metadata)
	if err != nil {
		return CategoryCounts{}, err
	}

	indexMap := map[uint16]int{}
	categories := make([]CategoryData, len(values))
	for idx := range values {
		label := labels[idx]
		value := values[idx]
		indexMap[value] = idx
		categories[idx] = CategoryData{Value: value, Label: label}
	}

	c := CategoryCounts{
		IndexMap:   indexMap,
		Categories: categories,
	}

	return c, nil
}

// Transform implements the CategoryCounts tile transformation, which counts the number
// of pixels of each category.
func (c CategoryCounts) Transform(tileData []*image.Gray16) ([]float64, error) {
	if len(c.Categories) == 0 {
		return nil, errors.New("labels unspecified")
	}

	categoryCounts := make([]float64, len(c.Categories))
	for i := 0; i < (tileData[0].Bounds().Max.X * tileData[0].Bounds().Max.Y * 2); i += 2 {
		// extract the 16 bit pixel values for each input band
		value := uint16(tileData[0].Pix[i])<<8 | uint16(tileData[0].Pix[i+1])
		index := c.IndexMap[value]

		// update the count for the associated category
		categoryCounts[index]++
	}
	return categoryCounts, nil
}

// Setup implements the CategoryCounts setup, loading tile data from disk.
func (c CategoryCounts) Setup(inputDir string, tile *Tile) ([]*image.Gray16, error) {
	// CDB: the band is hard coded to  land cover - it needs to be part of a configuration
	// supplied at runtime
	fileName := fmt.Sprintf("%s_%s_%s.tif", tile.ID, tile.Date, discreteLandCoverBand)
	path := path.Join(inputDir, fileName)
	img, err := loadAsGray16(path)
	if err != nil {
		log.Error(err, "channel file not loaded")
		os.Exit(1)
	}
	return []*image.Gray16{img}, nil
}

// ValueNames returns the names of the values in the same order as they are returned by the
// Transform call.
func (c CategoryCounts) ValueNames() []string {
	valueNames := make([]string, len(c.Categories))
	for i, category := range c.Categories {
		valueNames[i] = category.Label
	}
	return valueNames
}

func getCategoryValues(categoryValuesProperty string, metadata JSONString) ([]uint16, error) {
	// fetch the category values from the metadata
	jsonPath := fmt.Sprintf("%s.%s", "properties", categoryValuesProperty)
	result := gjson.Get(string(metadata), jsonPath)
	if !result.IsArray() {
		return nil, errors.Errorf("failed to find array %s in metadata", categoryValuesProperty)
	}

	categoryValues := make([]uint16, len(result.Array()))
	for i, result := range result.Array() {
		// Underlying JSON library will just panic on malformed result data
		categoryValues[i] = uint16(result.Int())
	}
	return categoryValues, nil
}

func getCategoryNames(categoryNamesProperty string, metadata JSONString) ([]string, error) {
	// fetch the category values from the metadata
	jsonPath := fmt.Sprintf("%s.%s", "properties", categoryNamesProperty)
	result := gjson.Get(string(metadata), jsonPath)
	if !result.IsArray() {
		return nil, errors.Errorf("failed to find array %s in metadata", categoryNamesProperty)
	}

	categoryNames := make([]string, len(result.Array()))
	for i, result := range result.Array() {
		// Underlying JSON library will just panic on malformed result data
		categoryNames[i] = result.String()
	}
	return categoryNames, nil
}

// Load a 16-bit or 8-bit geotiff as a single channel 16-bit raster.  If the file contains more than
// one band, only the first will be used.
func loadAsGray16(filePath string) (*image.Gray16, error) {
	// Load each of the datasets
	dataset, err := gdal.Open(filePath, gdal.ReadOnly)
	if err != nil {
		return nil, errors.Wrap(err, "band file not loaded")
	}

	// Accept a single band.
	numBands := dataset.RasterCount()
	if numBands == 0 {
		log.Warnf("found 0 bands - skipping")
	} else if numBands > 1 {
		log.Warnf("found %d bands - using band 0 only", numBands)
	}
	inputBand0 := dataset.RasterBand(1)

	// extract input raster size and update max x,y
	xSize := dataset.RasterXSize()
	ySize := dataset.RasterYSize()

	// extract input band data type
	dataType := inputBand0.RasterDataType()

	// Read in 8 or 16 bit uint data, produce a 16-bit grayscale image
	bandImage := image.NewGray16(image.Rect(0, 0, xSize, ySize))
	if dataType == gdal.UInt16 {
		// read the band data into the image buffer
		buffer := make([]uint16, xSize*ySize)
		if err = inputBand0.IO(gdal.Read, 0, 0, xSize, ySize, buffer, xSize, ySize, 0, 0); err != nil {
			dataset.Close()
			return nil, errors.Wrapf(err, "failed to load band data for %s", filePath)
		}
		dataset.Close() // done with GDAL buffer

		// crappy for now - go image lib stores its gray16 as [uint8, uint8] so we need an extra copy here
		badCount := 0
		for i, grayVal := range buffer {
			if grayVal > sentinel2Max {
				grayVal = sentinel2Max
				badCount++
			}
			// decompose the 16-bit value into 8 bit values with a big endian ordering as per the image lib
			// documentation
			bandImage.Pix[i*2] = uint8(grayVal & 0xFF00 >> 8)
			bandImage.Pix[i*2+1] = uint8(grayVal & 0xFF)
		}
		if badCount > 0 {
			log.Warnf("truncated %d values from %s", badCount, filePath)
		}
	} else if dataType == gdal.Byte {
		// read the band data into the image buffer
		buffer := make([]uint8, xSize*ySize)
		if err = inputBand0.IO(gdal.Read, 0, 0, xSize, ySize, buffer, xSize, ySize, 0, 0); err != nil {
			dataset.Close()
			return nil, errors.Wrapf(err, "failed to load band data for %s", filePath)
		}
		dataset.Close() // done with GDAL buffer

		// copy from gdal
		for i, grayVal := range buffer {
			// write the single 8 bit value into the 2nd byte of image data to respect big endian ordering
			bandImage.Pix[i*2] = 0
			bandImage.Pix[i*2+1] = grayVal
		}
	}

	return bandImage, nil
}
