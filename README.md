# distil-tile-transform
Applies configurable analytics to tiles fetched via [distil-ge-fetch](https://github.com/uncharted-distil/distil-ge-fetch), 
generating a CSV file containing tile ID, geographic bounds, timestamp, and data generated by the analytic operations.

## Install
```console
go get github.com/uncharted-distil/distil-tile-transform
```

## Usage
```
distil-tile-transform [flags] 

- input Input directory containing geotiff files. (default ".")
- operation Operation to perform on the tiles. (default "mean_NDVI")
- workers Number of workers (default 8)
```
