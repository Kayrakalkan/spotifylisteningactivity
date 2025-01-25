import requests
import time
from datetime import datetime

def spotify_get_access_token(sp_dc):
    """Retrieve the Spotify access token using the sp_dc cookie."""
    url = "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
    cookies = {"sp_dc": sp_dc}
    response = requests.get(url, cookies=cookies, timeout=10)
    response.raise_for_status()
    return response.json().get("accessToken", "")

def spotify_get_friends_json(access_token):
    """Retrieve the list of Spotify friends' activity."""
    url = "https://guc-spclient.spotify.com/presence-view/v1/buddylist"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()

def spotify_convert_uri_to_url(uri):
    """Convert Spotify URI to a shareable URL."""
    si = "?si=1"
    base_url = "https://open.spotify.com"
    if "spotify:user:" in uri:
        return f"{base_url}/user/{uri.split(':', 2)[2]}{si}"
    elif "spotify:artist:" in uri:
        return f"{base_url}/artist/{uri.split(':', 2)[2]}{si}"
    elif "spotify:track:" in uri:
        return f"{base_url}/track/{uri.split(':', 2)[2]}{si}"
    elif "spotify:album:" in uri:
        return f"{base_url}/album/{uri.split(':', 2)[2]}{si}"
    elif "spotify:playlist:" in uri:
        return f"{base_url}/playlist/{uri.split(':', 2)[2]}{si}"
    return ""

def get_date_from_ts(timestamp):
    """Convert a timestamp to a readable date."""
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

def calculate_timespan(current_time, last_time):
    """Calculate the time difference between now and the last activity."""
    delta = current_time - last_time.timestamp()
    hours, remainder = divmod(delta, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{int(hours)}h {int(minutes)}m"

def spotify_list_friends(friend_activity):
    """List the recent activity of Spotify friends."""
    for friend in friend_activity.get("friends", []):
        user = friend["user"]
        track = friend["track"]
        context = track.get("context", {})

        username = user.get("name", "Unknown")
        artist = track.get("artist", {}).get("name", "Unknown")
        track_name = track.get("name", "Unknown")
        album_name = track.get("album", {}).get("name", "Unknown")
        timestamp = friend.get("timestamp", 0) / 1000

        print("-----------------------------------------------------------")
        print(f"Username: {username}")
        print(f"Track: {artist} - {track_name}")
        print(f"Album: {album_name}")
        print(f"Last Activity: {get_date_from_ts(timestamp)}")
        print(f"Context: {context.get('name', 'Unknown')}")
if __name__ == "__main__":
    sp_dc = "your token"
    try:
        access_token = spotify_get_access_token(sp_dc)
        friend_activity = spotify_get_friends_json(access_token)
        spotify_list_friends(friend_activity)
    except Exception as e:
        print(f"An error occurred: {e}")
