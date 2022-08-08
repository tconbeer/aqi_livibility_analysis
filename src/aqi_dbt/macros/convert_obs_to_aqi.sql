{%- macro convert_obs_to_aqi(parameter_name, reporting_units, observed_value) -%}
case
    when {{ observed_value }} < 0
    then null
    when {{ parameter_name }} = 'OZONE'
    then {{ convert_ozone_to_aqi(reporting_units, observed_value) }}
    when {{ parameter_name }} = 'PM2.5'
    then {{ convert_pm25_to_aqi(reporting_units, observed_value) }}
    when {{ parameter_name }} = 'PM10'
    then {{ convert_pm10_to_aqi(reporting_units, observed_value) }}
end
{%- endmacro -%}

-- see https://forum.airnowtech.org/t/the-aqi-equation/169
{%- macro convert_ozone_to_aqi(reporting_units, observed_value) -%}
{%- set breakpoints = [
    (54.0, 50),
    (70.0, 100),
    (85.0, 150),
    (105.0, 200),
    (200.0, 300),
    (400.0, 500),
] -%}
case
    when {{ reporting_units }} != 'PPB'
    then null
    else {{ interpolate(observed_value, breakpoints) }}
end
{%- endmacro -%}

{%- macro convert_pm25_to_aqi(reporting_units, observed_value) -%}
{%- set breakpoints = [
    (12.0, 50),
    (35.4, 100),
    (55.4, 150),
    (150.4, 200),
    (250.4, 300),
    (500.4, 500),
] -%}
case
    when {{ reporting_units }} != 'UG/M3'
    then null
    else {{ interpolate(observed_value, breakpoints) }}
end
{%- endmacro -%}

{%- macro convert_pm10_to_aqi(reporting_units, observed_value) -%}
{%- set breakpoints = [
    (54.0, 50),
    (154.0, 100),
    (254.0, 150),
    (354.0, 200),
    (424.0, 300),
    (604.0, 500),
] -%}
case
    when {{ reporting_units }} != 'UG/M3'
    then null
    else {{ interpolate(observed_value, breakpoints) }}
end
{%- endmacro -%}

{%- macro interpolate(observed_value, breakpoints) -%}
{%- set prev = namespace() -%}
{%- set prev.concentration = 0 -%}
{%- set prev.aqi = 0 -%}
round(
    case
        {%- for concentration, aqi in breakpoints %}
        when {{ observed_value }} <= {{ concentration }}
        then
            (
                ({{ aqi }} - {{ prev.aqi }}) / (
                    {{ concentration }} - {{ prev.concentration }}
                ) * ({{ observed_value }} - {{ prev.concentration }})
            )
            + {{ prev.aqi }}
            {%- set prev.concentration = concentration -%}
            {%- set prev.aqi = aqi -%}
        {%- endfor %}
        else {{ prev.aqi }}
    end
)
{%- endmacro -%}
