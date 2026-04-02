import openmeteo_requests
from retry_requests import retry
from parameters import weather_points
from datetime import datetime, timezone

retry_session = retry(retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


def fetch_current_weather(weather_points):
    url = "https://api.open-meteo.com/v1/forecast"
    results = []

    for point in weather_points:
        params = {
            "latitude": point["lat"],
            "longitude": point["lon"],
            "current": [
                "temperature_2m",
                "precipitation",
                "relative_humidity_2m",
                "rain",
                "showers",
                "wind_gusts_10m",
                "wind_speed_10m",
                "wind_direction_10m",
                "shortwave_radiation",
                "global_tilted_irradiance",
                "diffuse_radiation",
            ],
        }

        try:
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]
            current = response.Current()

            row = {
                "country_code": point["country_code"],
                "country": point["country"],
                "technology": point["technology"],
                "cluster_name": point["cluster_name"],
                "requested_lat": point["lat"],
                "requested_lon": point["lon"],
                "response_lat": response.Latitude(),
                "response_lon": response.Longitude(),
                "elevation": response.Elevation(),
                "utc_offset_seconds": response.UtcOffsetSeconds(),
                "current_time": datetime.fromtimestamp(
                    current.Time(), tz=timezone.utc
                ).isoformat(),
                "temperature_2m": current.Variables(0).Value(),
                "precipitation": current.Variables(1).Value(),
                "relative_humidity_2m": current.Variables(2).Value(),
                "rain": current.Variables(3).Value(),
                "showers": current.Variables(4).Value(),
                "wind_gusts_10m": current.Variables(5).Value(),
                "wind_speed_10m": current.Variables(6).Value(),
                "wind_direction_10m": current.Variables(7).Value(),
                "shortwave_radiation":current.Variables(8).Value(),
                "global_tilted_irradiance":current.Variables(9).Value(),
                "diffuse_radiation": current.Variables(10).Value(),
            }

            results.append(row)

            print(
                f"OK: {point['country_code']} {point['technology']} - "
                f"{point['cluster_name']}"
            )

        except Exception as e:
            print(
                f"ERROR: {point['country_code']} {point['technology']} - "
                f"{point['cluster_name']}: {e}"
            )

    return results