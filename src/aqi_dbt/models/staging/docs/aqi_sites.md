{% docs aqi_sites %}

# AQI: Monitoring Sites

A dimension table for monitoring sites and their descriptive information.

## Grain
Grain of this table is per-day per-site per-parameter.
(where a parameter is a type of measurement, like OZONE). 

## Partition and Clustering
Clustered on `site_id`, `parameter_name`, `observed_date`

{% enddocs %}