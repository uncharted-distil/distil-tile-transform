package analytics

import (
	"fmt"
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
	// OperationCategoryCounts counts the instances of each category value in a tile.
	OperationCategoryCounts = "category_counts"

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
	if operation == OperationCategoryCounts {
		tileAnalytic, err = NewCategoryCounts(metadata)
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

// Transform implements the CategoryCounts tile transformation, which counts the number
// of pixels of each category.
func (c CategoryCounts) Transform(tileData []*GeoImage) ([]float64, error) {
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

// GeoBounds defines a rectangular geographic boundary.
type GeoBounds struct {
	MinLon float64
	MinLat float64
	MaxLon float64
	MaxLat float64
}

func (g GeoBounds) String() string {
	return fmt.Sprintf("%f,%f,%f,%f,%f,%f,%f,%f",
		g.MinLon, g.MinLat,
		g.MaxLon, g.MinLat,
		g.MaxLon, g.MaxLat,
		g.MinLon, g.MaxLat,
	)
}

// GeoImage is a gray16 image and its associated geobounds.
type GeoImage struct {
	Data   []float64
	XSize  int
	YSize  int
	Bounds GeoBounds
}

// Load a 16-bit or 8-bit geotiff as a single channel 16-bit raster.  If the file contains more than
// one band, only the first will be used.
func loadGeoImage(filePath string) (*GeoImage, error) {
	// Load each of the datasets
	gdalDataset, err := gdal.Open(filePath, gdal.ReadOnly)
	if err != nil {
		return nil, errors.Wrap(err, "band file not loaded")
	}

	// Accept a single band.
	numBands := gdalDataset.RasterCount()
	if numBands == 0 {
		log.Warnf("found 0 bands - skipping")
	} else if numBands > 1 {
		log.Warnf("found %d bands - using band 0 only", numBands)
	}
	inputBand := gdalDataset.RasterBand(1)

	// extract input raster size and update max x,y
	xSize := gdalDataset.RasterXSize()
	ySize := gdalDataset.RasterYSize()

	// compute the geocoordinates
	tx := gdalDataset.GeoTransform()
	bounds := GeoBounds{
		MinLon: tx[0],
		MinLat: tx[3] + float64(xSize)*tx[4] + float64(ySize)*tx[5],
		MaxLon: tx[0] + float64(xSize)*tx[1] + float64(ySize)*tx[2],
		MaxLat: tx[3],
	}

	// extract input band data type
	dataType := inputBand.RasterDataType()

	// Read data in from tiff and save it out as a float64 array.  This is less efficient than storing
	// each type nativel, but simplifies things downstream.
	bandData := make([]float64, xSize*ySize)
	if dataType == gdal.UInt16 {
		// read the band data into the image buffer
		buffer := make([]uint16, xSize*ySize)
		if err = inputBand.IO(gdal.Read, 0, 0, xSize, ySize, buffer, xSize, ySize, 0, 0); err != nil {
			gdalDataset.Close()
			return nil, errors.Wrapf(err, "failed to load band data for %s", filePath)
		}
		gdalDataset.Close() // done with GDAL buffer

		// copy the data into the final float64 buffer
		for i, val := range buffer {
			bandData[i] = float64(val)
		}
	} else if dataType == gdal.Byte {
		// read the band data into the image buffer
		buffer := make([]uint8, xSize*ySize)
		if err = inputBand.IO(gdal.Read, 0, 0, xSize, ySize, buffer, xSize, ySize, 0, 0); err != nil {
			gdalDataset.Close()
			return nil, errors.Wrapf(err, "failed to load band data for %s", filePath)
		}
		gdalDataset.Close() // done with GDAL buffer

		// copy the data into the final float64 buffer
		for i, val := range buffer {
			bandData[i] = float64(val)
		}
	} else if dataType == gdal.Float32 {
		// read the band data into the image buffer
		buffer := make([]float32, xSize*ySize)
		if err = inputBand.IO(gdal.Read, 0, 0, xSize, ySize, buffer, xSize, ySize, 0, 0); err != nil {
			gdalDataset.Close()
			return nil, errors.Wrapf(err, "failed to load band data for %s", filePath)
		}
		gdalDataset.Close() // done with GDAL buffer

		// copy the data into the final float64 buffer
		for i, val := range buffer {
			bandData[i] = float64(val)
		}
	} else if dataType == gdal.Float64 {
		// read the band data into the image buffer
		buffer := bandData
		if err = inputBand.IO(gdal.Read, 0, 0, xSize, ySize, buffer, xSize, ySize, 0, 0); err != nil {
			gdalDataset.Close()
			return nil, errors.Wrapf(err, "failed to load band data for %s", filePath)
		}
		gdalDataset.Close() // done with GDAL buffer

		// No copy needed - already stored as float64
	} else {
		return nil, errors.Wrapf(err, "unhandled GDAL band type %v for %s", dataType, filePath)
	}

	return &GeoImage{
		Data:   bandData,
		XSize:  xSize,
		YSize:  ySize,
		Bounds: bounds}, nil
}
