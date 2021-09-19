# AQI Livibility Analysis

An analysis of long-range air quality trends (primarily Ozone and PM2.5), for the purposes of establishing
the lifestyle impacts of poor air quality in select cities.

# Data Use Guidelines

Data are aggregated by the US EPA under the [AirNow Data Exchange Guidelines](https://docs.airnowapi.org/docs/DataUseGuidelines.pdf)

Data are provided by a host of federal, state, local, and tribal agencies; [full list here](https://www.airnow.gov/index.cfm?action=airnow.partnerslist).

This analysis must not be used for any regulatory purposes; analysis contains preliminary and unverified data.

# Architecture

This project uses Google Cloud and dagster

# How To

Launch the dagster daemon and dagit together with:

```
dagster-daemon run & dagit -f src/aqi_livibility_analysis/graphs.py
```