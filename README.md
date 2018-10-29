# gfw-places-to-watch
Generate new places to watch data when GLAD is updated

### About
This code intersects GLAD points with a grid that has been scored for the amount of IFL and WDPA area within each cell.
After this intersection, it multiplies the count of GLAD points by the importance score (determined by IFL and WDPA area)
to help us identify important places of forest change.

We select the top 10 high scoring grid cells for each region (Central America, South America, Africa, etc) and push them to a [CARTO table](https://wri-01.carto.com/tables/ptw_top_10). These are then surfaced on the [staging map](http://staging.globalforestwatch.org/map/use/river_basins/4?analysis=eyJzaG93QW5hbHlzaXMiOmZhbHNlLCJoaWRkZW4iOmZhbHNlfQ%3D%3D&map=eyJkYXRhc2V0cyI6W3siZGF0YXNldCI6ImU2NjNlYjA5LTA0ZGUtNGYzOS1iODcxLTM1YzZjMmVkMTBiNSIsIm9wYWNpdHkiOjEsInZpc2liaWxpdHkiOnRydWUsImxheWVycyI6WyJkZDVkZjg3Zi0zOWMyLTRhZWItYTQ2Mi0zZWY5NjliMjBiNjYiLCI5NGY4YmFhZC0zYWU0LTQ4ZWUtYWFmNy1mYmNjNjQ5YjQ1YjkiXX0seyJsYXllcnMiOlsiZDU5MGY4M2MtOWI1NC00NTQyLThkMjctZjYxYjhiMTlkZjQ2Il0sImRhdGFzZXQiOiI2MzI5NWIwNS01NWExLTQ1NmMtYTU2Yy1jOWNjYjNhNzExZWMiLCJvcGFjaXR5IjoxLCJ2aXNpYmlsaXR5Ijp0cnVlfV0sInpvb20iOjQsImNlbnRlciI6eyJsYXQiOjMuMTI1Nzc1MjkyODgyNzg0NywibG5nIjotNzUuMjUzMjU3NzUxNDY0ODZ9LCJjYW5Cb3VuZCI6ZmFsc2UsImJib3giOm51bGx9&menu=eyJtZW51U2VjdGlvbiI6IiIsImRhdGFzZXRDYXRlZ29yeSI6IiJ9&recentImagery=eyJ2aXNpYmxlIjpmYWxzZX0%3D) under the `Uncurated Places to Watch` toggle on the GLAD dataset.

These areas are then vetted by our staff to determine what stories we'll share as our offical Places to Watch. With the help of Vizzuality, these get added to a separate CARTO table, and are viewable on the [flagship](https://www.globalforestwatch.org/map/3/15.00/27.00/ALL/grayscale/umd_as_it_happens,places_to_watch?tab=analysis-tab&begin=2015-01-01&end=2018-10-20&dont_analyze=true).

### Process
How goes this work? We use [hadoop_pip](https://github.com/wri/hadoop_pip/) to start a hadoop cluster and run the GLAD / grid intersection for us. We then grab the output table (CSV of alerts per grid and the total grid score) and then do the math to figure out the top 10 per region. This data is pushed to the CARTO ptw_top_10 table, then should appear in the `uncurated` table on the staging map.

### Prep
1. Clone hadoop_pip in the same directory that this repo is sitting in
2. Create a `tokens` directory in this repo, with a file called `wri-01@cartodb` in it that contains our CARTO API token.
3. Check the `processing/hadoop_ptw.ini` file to be sure it's up to date. This file contains:
- Grid polygon TSV: `s3://gfw2-data/alerts-tsv/ptw/ptw_grid.tsv`
- Path to GLAD point directory: `s3://gfw2-data/alerts-tsv/glad/`
- SQL query to sum alert area_ha and count alerts, grouping by grid ID: `SELECT _c3, _c4, sum(_c5), sum(_c6), _c8, _c9, count(*) FROM my_table GROUP BY _c3, _c4, _c8, _c9`

### Run it!

`python update_ptw.py --min-date 2018-07-01 --max-date 2018-09-30`
