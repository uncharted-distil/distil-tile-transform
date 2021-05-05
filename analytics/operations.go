package analytics

import (
	"fmt"
	"math"
	"os"
	"path"

	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
	log "github.com/unchartedsoftware/plog"
)

// Operation defines the type of the operation specifier
type Operation string

// JSONString defines a JSON object as a string
type JSONString string

const (
	// OperationCategoryCountsRaw counts the instances of each category value in a tile.
	OperationCategoryCountsRaw = "category_counts_raw"

	// OperationCategoryCountsPercentage counts the instances of each category value in a tile and returns them
	// as a percentage of the total tile pixels.
	OperationCategoryCountsPercentage = "category_counts_percentage"

	// OperationCategoryBinary counts the instances of each category value in a tile and return a 1 if the
	// the count is non-zero.
	OperationCategoryBinary = "category_binary"

	// OperationMeanNDVI computes the mean NDVI for a tile.
	OperationMeanNDVI = "mean_ndvi"

	// OperationMean computes the mean for a tile.
	OperationMean = "mean"

	// Constants for data sources
	// TODO: these should really be part of some configuration
	// file that is supplied and updated as new datasource are included.

	// sentinel constants
	band8 = "B08"
	band4 = "B04"

	// copernicus land coverage constants
	discreteLandCoverBand           = "discrete_classification"
	discreteLandCoverCategoryValues = "discrete_classification_class_values"
	discreteLandCoverCategoryNames  = "discrete_classification_class_names"
)

// Tile is structure that provides geospatial tile information.
type Tile struct {
	GeoHash   string
	Date      string
	Timestamp int64
}

// Transformer in an interface that defines an operation on tile data.
type Transformer interface {
	Setup(inputDir string, tile *Tile) ([]*GeoImage, error)
	Transform(tileData []*GeoImage) ([]float64, error)
	ValueNames() []string
}

// CreateTileAnalytic creates and initializes a tile analytic based on a requested operation
// type.
func CreateTileAnalytic(metadata JSONString, operation Operation) (Transformer, error) {
	var tileAnalytic Transformer
	var err error
	if operation == OperationCategoryCountsRaw {
		tileAnalytic, err = NewCategoryCountsRaw(metadata)
		if err != nil {
			return nil, err
		}
	} else if operation == OperationCategoryCountsPercentage {
		tileAnalytic, err = NewCategoryCountsPercentage(metadata)
		if err != nil {
			return nil, err
		}
	} else if operation == OperationCategoryBinary {
		tileAnalytic, err = NewCategoryBinary(metadata)
		if err != nil {
			return nil, err
		}
	} else if operation == OperationMeanNDVI {
		tileAnalytic = MeanNDVI{}
	} else if operation == OperationMean {
		tileAnalytic, err = NewMean(metadata)
		if err != nil {
			return nil, err
		}
	} else {
		log.Warnf("unrecognized operation - defaulting to %s", OperationMeanNDVI)
		tileAnalytic = MeanNDVI{}
	}
	return tileAnalytic, nil
}

// MeanNDVI domputes mean NDVI for sentinel-2 tiles
type MeanNDVI struct{}

// Transform implements the MeanNDVI tile transformation, which computes the average NDVI for a given tile.
func (m MeanNDVI) Transform(tileData []*GeoImage) ([]float64, error) {
	sumNDVI := 0.0
	numValues := 0
	image0 := tileData[0].Data
	image1 := tileData[1].Data
	for i := range image0 {
		// extract the 16 bit pixel values for each input band
		value0 := image0[i]
		value1 := image1[i]

		// compute NDVI ratio
		transformedValue := 0.0
		if value0 != 0 || value1 != 0 {
			transformedValue = math.Max(0, float64(int32(value0)-int32(value1))/float64(int32(value0)+int32(value1)))
		}
		sumNDVI += transformedValue
		numValues++
	}

	// compute the mean NDVI
	mean := sumNDVI / float64(numValues)
	return []float64{mean}, nil
}

// Setup loads the data for the MeanNDVI tile transformation.
func (m MeanNDVI) Setup(inputDir string, tile *Tile) ([]*GeoImage, error) {
	band8FileName := fmt.Sprintf("%s_%s_%s.tif", tile.GeoHash, tile.Date, band8)
	band8Path := path.Join(inputDir, band8FileName)
	band8Image, err := loadGeoImage(band8Path)
	if err != nil {
		log.Error(err, "band 8 file not loaded")
		os.Exit(1)
	}

	band4FileName := fmt.Sprintf("%s_%s_%s.tif", tile.GeoHash, tile.Date, band4)
	band4Path := path.Join(inputDir, band4FileName)
	band4Image, err := loadGeoImage(band4Path)
	if err != nil {
		log.Error(err, "band 4 file not loaded")
		os.Exit(1)
	}

	return []*GeoImage{band8Image, band4Image}, nil
}

// ValueNames returns the name of the Mean NDVI value.
func (m MeanNDVI) ValueNames() []string {
	return []string{"mean_ndvi"}
}

// Mean computes mean for a single band tile
type Mean struct {
	ColumnName string
}

// NewMean creates a new mean operation.
func NewMean(metadata JSONString) (*Mean, error) {
	// fetch band name
	result := gjson.Get(string(metadata), "bands.0.id")
	if result.String() == "" {
		return nil, errors.Errorf("failed to find band ID in metadata")
	}

	return &Mean{ColumnName: result.String()}, nil
}

// Transform implements the mean tile transformation, which computes the average value for a given tile.
func (m Mean) Transform(tileData []*GeoImage) ([]float64, error) {
	sum := 0.0
	data := tileData[0].Data
	for i := range data {
		// extract the 16 bit pixel values for each input band
		sum += data[i]
	}

	// compute the mean NDVI
	mean := sum / float64(len(data))
	return []float64{mean}, nil
}

// Setup loads the data for the MeanNDVI tile transformation.
func (m Mean) Setup(inputDir string, tile *Tile) ([]*GeoImage, error) {
	fileName := fmt.Sprintf("%s_%s_%s.tif", tile.GeoHash, tile.Date, m.ColumnName)
	path := path.Join(inputDir, fileName)
	image, err := loadGeoImage(path)
	if err != nil {
		log.Error(err, "mean file not loaded")
		os.Exit(1)
	}
	return []*GeoImage{image}, nil
}

// ValueNames returns the name of the Mean NDVI value.
func (m Mean) ValueNames() []string {
	return []string{m.ColumnName}
}

// CategoryData provides a category label and its associated numeric value
// from a raster.
type CategoryData struct {
	Value int
	Label string
}

// CategoryCounts computes number of pixels for each category of each value
type CategoryCounts struct {
	Categories []CategoryData
	IndexMap   map[int]int
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

	indexMap := map[int]int{}
	categories := make([]CategoryData, len(values))
	for idx := range values {
		label := labels[idx]
		value := int(values[idx])
		indexMap[value] = idx
		categories[idx] = CategoryData{Value: value, Label: label}
	}

	c := CategoryCounts{
		IndexMap:   indexMap,
		Categories: categories,
	}

	return c, nil
}

// Setup implements the CategoryCounts setup, loading tile data from disk.
func (c CategoryCounts) Setup(inputDir string, tile *Tile) ([]*GeoImage, error) {
	// CDB: the band is hard coded to  land cover - it needs to be part of a configuration
	// supplied at runtime
	fileName := fmt.Sprintf("%s_%s_%s.tif", tile.GeoHash, tile.Date, discreteLandCoverBand)
	path := path.Join(inputDir, fileName)
	img, err := loadGeoImage(path)
	if err != nil {
		log.Error(err, "channel file not loaded")
		os.Exit(1)
	}
	return []*GeoImage{img}, nil
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

// CategoryCountsRaw computes the number of pixels in a tile the are assigned
// to a given category.
type CategoryCountsRaw struct {
	CategoryCounts
}

// NewCategoryCountsRaw create a new CategoryCounts tile operation
func NewCategoryCountsRaw(metadata JSONString) (CategoryCountsRaw, error) {
	c, err := NewCategoryCounts(metadata)
	if err != nil {
		return CategoryCountsRaw{}, err
	}
	return CategoryCountsRaw{c}, nil
}

// Transform implements the CategoryCounts tile transformation, which counts the number
// of pixels of each category.
func (c CategoryCountsRaw) Transform(tileData []*GeoImage) ([]float64, error) {
	return computeCounts(&c.CategoryCounts, tileData)
}

// CategoryCountsPercentage computes the percentage of pixels in a given tile the are assigned
// to a given category.
type CategoryCountsPercentage struct {
	CategoryCounts
}

// NewCategoryCountsPercentage create a new CategoryCounts tile operation
func NewCategoryCountsPercentage(metadata JSONString) (CategoryCountsPercentage, error) {
	c, err := NewCategoryCounts(metadata)
	if err != nil {
		return CategoryCountsPercentage{}, err
	}
	return CategoryCountsPercentage{c}, nil
}

// Transform implements the CategoryCountsPercentage tile transformation, which counts the number
// of pixels of each category and returns them as a percentage of the total tile.
func (c CategoryCountsPercentage) Transform(tileData []*GeoImage) ([]float64, error) {
	// compute the raw counts
	counts, err := computeCounts(&c.CategoryCounts, tileData)
	if err != nil {
		return counts, err
	}

	// compute percentage in place
	totalPixels := float64(tileData[0].XSize * tileData[0].YSize)
	for i, count := range counts {
		counts[i] = count / totalPixels
	}

	// compute each as a percentage of the total
	return counts, nil
}

// CategoryBinary sets a value of 1 if a particular category is present for a tile, 0
// otherwise.
type CategoryBinary struct {
	CategoryCounts
}

// NewCategoryBinary create a new CategoryBinary tile operation
func NewCategoryBinary(metadata JSONString) (CategoryBinary, error) {
	c, err := NewCategoryCounts(metadata)
	if err != nil {
		return CategoryBinary{}, err
	}
	return CategoryBinary{c}, nil
}

// Transform implements the CategoryBinary tile transformation, which counts the number
// of pixels of each category, and returns 1 if that number is non-zero.
func (c CategoryBinary) Transform(tileData []*GeoImage) ([]float64, error) {
	// compute the raw counts
	counts, err := computeCounts(&c.CategoryCounts, tileData)
	if err != nil {
		return counts, err
	}

	// convert to a binary indicator
	for i, count := range counts {
		if count > 0 {
			counts[i] = 1.0
		} else {
			counts[i] = 0.0
		}
	}
	return counts, nil
}

func computeCounts(c *CategoryCounts, tileData []*GeoImage) ([]float64, error) {
	if len(c.Categories) == 0 {
		return nil, errors.New("labels unspecified")
	}

	categoryCounts := make([]float64, len(c.Categories))
	for _, val := range tileData[0].Data {
		// extract the 16 bit pixel values for each input band
		value := int(val)
		index := c.IndexMap[value]

		// update the count for the associated category
		categoryCounts[index]++
	}

	return categoryCounts, nil
}
