{% docs aqi_hourly_observations %}

# AQI: Hourly Observations

Individual air-quality observations. In the base model, we also convert 
raw measurements of pollutants to their AQI score, using a macro.

## Grain
Grain of this table is per-hour per-site per-parameter 
(where a parameter is a type of measurement, like OZONE). 

## Relationships
Contains a FK, `site_id`, to the aqi_sites source. 

## Partition and Clustering
Partitioned on `observed_at`.
Clustered on `site_id`

{% enddocs %}