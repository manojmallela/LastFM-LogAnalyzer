# Last.FM-LogAnalyzer

An Apache Spark application to analyze data from Last.FM's API. The complete dataset is available at the below link. 
`http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html`

Usage:
```
Usage: LastFM Log Analytics [options]
  -m, --master <value> | Spark Master - local[*], yarn, kubernetes (k8s://host:port)
  -e, --execution-mode <value> | Execution mode (Int) - 1: RDD or 2:DF or 3:Both
  -i, --inputPath <value> | LastFm data file path
  -o, --outputPath <value> | Path to write results
```



















[^1]: http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html