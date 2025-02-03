import requests
import time
from datetime import datetime

def spotify_get_access_token(sp_dc):
    """Retrieve the Spotify access token using the sp_dc cookie."""
    url = "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
    cookies = {"sp_dc": sp_dc}
    response = requests.get(url, cookies=cookies, timeout=10)
    response.raise_for_status()
    access_token = response.json().get("accessToken", "")
    return response.json().get("accessToken", "")

def spotify_get_friends_json(access_token):
    """Retrieve the list of Spotify friends' activity."""
    url = "https://guc-spclient.spotify.com/presence-view/v1/buddylist"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()

def get_date_from_ts(timestamp):
    """Convert a timestamp to a readable date."""
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

def filter_active_friends(friend_activity, active_threshold_seconds=300):
    """Filter only active friends based on the timestamp."""
    current_time = time.time()
    active_friends = []

    for friend in friend_activity.get("friends", []):
        timestamp = friend.get("timestamp", 0) / 1000  # Convert to seconds
        if current_time - timestamp <= active_threshold_seconds:
            active_friends.append(friend)

    return active_friends

def spotify_list_active_friends(active_friends):
    """List the recent activity of active Spotify friends."""
    for friend in active_friends:
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
    sp_dc = "AQBkJe451fMV1bBOPG9k-K2wZcJA1Ae87dlAVYK-Q0x0TuLqozN9t0nntnVrrYDgP1DFqsQ3hJegcjhIz1NUBSJlxYTxkOFoMFetvp6HW4e4CxGS1yzi1_iFszfguNEhGQXNdC0mqlN6px8OEuRUsjSxZGwf0uK3HhKILShjt4j3QjOC89cADwXdcGt3YkRl929bpq05Ax6E37nz19U"  # Your sp_dc cookie
    try:
        access_token = spotify_get_access_token(sp_dc)
        friend_activity = spotify_get_friends_json(access_token)
        active_friends = filter_active_friends(friend_activity, active_threshold_seconds=300)  # 5 minutes threshold
        if active_friends:
            spotify_list_active_friends(active_friends)
        else:
            print("No active friends found.")
    except Exception as e:
        print(f"An error occurred: {e}")
