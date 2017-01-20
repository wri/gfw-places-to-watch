# gfw-places-to-watch
Generate new places to watch data when GLAD is updated

### Workflow
1. Download latest GLAD data
2. Data is tiled into 1x1 degree chunks
3. Convert last 30 days to value 1, all else 0
4. Mask emissions data where GLAD = 1
5. Convert emissions raster to point - saved as csv
6. Snap points to lower left corner of grid, summarize number of points per grid
7. Per region, calculate top 10 scoring grid cells. Return those

## Directory Prep
- carto api key
- emissions tiles

## To Note
- Some GLAD alerts extending beyond the country boundary they are said to cover
