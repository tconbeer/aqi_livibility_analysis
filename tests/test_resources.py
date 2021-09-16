from aqi_livibility_analysis.resources import AirNow, airnow_resource


def test_airnow_resource() -> None:
    resource = airnow_resource()
    assert resource is not None
    assert isinstance(resource, AirNow)
