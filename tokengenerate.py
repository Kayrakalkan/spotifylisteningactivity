import os
from dotenv import load_dotenv, set_key
import requests

load_dotenv()

def spotify_get_access_token(sp_dc):
    """Retrieve the Spotify access token using the sp_dc cookie."""
    url = "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
    cookies = {"sp_dc": sp_dc}
    response = requests.get(url, cookies=cookies, timeout=10)
    response.raise_for_status()
    access_token = response.json().get("accessToken", "")
    return response.json().get("accessToken", "")

def update_bearer_token(new_token: str):
    dotenv_path = '.env'
    set_key(dotenv_path, 'SPOTIFY_BEARER_TOKEN', new_token)

new_token = access_token = spotify_get_access_token(sp_dc)  
update_bearer_token(new_token)

load_dotenv()  # Yeniden yükleyin
print("Updated BEARER_TOKEN:", os.getenv('SPOTIFY_BEARER_TOKEN'))
print("New token:", new_token)
