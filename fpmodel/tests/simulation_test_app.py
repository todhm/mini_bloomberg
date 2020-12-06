from fastapi.testclient import TestClient
from main import app, get_settings
from settings import SimulationTestSettings
from functools import lru_cache


@lru_cache()
def get_simulation_settings():
    return SimulationTestSettings()


client = TestClient(app)
app.dependency_overrides[get_settings] = get_simulation_settings
settings = get_simulation_settings()
