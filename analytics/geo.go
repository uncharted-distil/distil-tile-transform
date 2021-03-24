package analytics

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/uncharted-distil/gdal"
	log "github.com/unchartedsoftware/plog"
)

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

// Load a geotiff into a float64 buffer.  If the file contains more than one band, only the first will be used.
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
