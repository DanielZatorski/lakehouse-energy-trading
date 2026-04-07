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
            "soil_temperature_0cm",
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "cloud_cover",
            "shortwave_radiation",
            "direct_radiation",
            "diffuse_radiation",
            "global_tilted_irradiance",
            "wind_speed_100m",
            "wind_direction_100m",
            "wind_gusts_10m",
            "temperature_2m",
            "surface_pressure",
            "precipitation"
            ],
        }

        try:
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]
            current = response.Current()

            row = {
                "country_code": point["country_code"],
                "country": point["country"],
                "bidding_zone": point["bidding_zone"],
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
                "soil_temperature_0cm": current.Variables(0).Value(),
                "temperature_2m": current.Variables(1).Value(),
                "relative_humidity_2m": current.Variables(2).Value(),
                "precipitation": current.Variables(3).Value(),
                "cloud_cover": current.Variables(4).Value(),
                "shortwave_radiation": current.Variables(5).Value(),
                "direct_radiation": current.Variables(6).Value(),
                "diffuse_radiation": current.Variables(7).Value(),
                "global_tilted_irradiance": current.Variables(8).Value(),
                "wind_speed_100m": current.Variables(9).Value(),
                "wind_direction_100m": current.Variables(10).Value(),
                "wind_gusts_10m": current.Variables(11).Value(),
                "temperature_2m": current.Variables(12).Value(),
                "surface_pressure": current.Variables(13).Value(),
                "precipitation": current.Variables(14).Value()
            }

            results.append(row)

            print(
                f"OK: {point['bidding_zone']} {point['technology']} - "
                f"{point['cluster_name']}"
            )

        except Exception as e:
            print(
                f"ERROR: {point['bidding_zone']} {point['technology']} - "
                f"{point['cluster_name']}: {e}"
            )

    return results