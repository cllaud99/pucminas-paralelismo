import os
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("WEATHER_KEY")


def get_temperature(city: str, api_key: str = API_KEY) -> Optional[float]:
    """
    Consulta a temperatura atual de uma cidade usando a WeatherAPI.

    Args:
        city (str): Nome da cidade.
        api_key (str): Chave de autenticação da WeatherAPI.

    Returns:
        Optional[float]: Temperatura atual em Celsius ou None em caso de erro.
    """
    url = "https://api.weatherapi.com/v1/current.json"
    params = {"key": api_key, "q": city, "lang": "pt"}

    try:
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()
        return data["current"]["temp_c"]
    except requests.exceptions.RequestException as e:
        print(f"[NETWORK ERROR] City: {city} - {e}")
    except KeyError:
        print(f"[FORMAT ERROR] City: {city} - Unexpected response.")
    except Exception as e:
        print(f"[UNKNOWN ERROR] City: {city} - {e}")

    return None


if __name__ == "__main__":
    city = "Cajuru"
    temperature = get_temperature(city)
    print(f"A temperatura atual em {city} é de {temperature} graus Celsius.")
