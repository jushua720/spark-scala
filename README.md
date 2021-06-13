# boston-crimes

## Submitting Application: 
spark-submit --master local[*] --class com.example.BostonCrimesMap /path/to/jar {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder}

## Statistics
- crimes_total
- crimes_monthly
- average latitude (longitude) value
- frequent_crime_types

## Optimization Techniques
- Broadcast Join<br/>

#### Scenario
We have a huge dataset (crime.csv). At the same time, we have a small dataset which can easily fit in memory (offense_codes.csv). And we need to join these two datasets. 
To avoid shuffle (join requires matching keys to stay on the same Spark executor, and Spark needs to redistribute the records by hashing the join column), we duplicate the small dataset on all the executors.
