import sqlite3
import requests
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get the FID from the environment variable
viewer_fid = os.getenv("FARCASTER_DEVELOPER_FID")
api_key = os.getenv("NEYNAR_API_KEY")

# Initialize Database Connection
conn = sqlite3.connect('farcaster_users.db')
cursor = conn.cursor()

# Create Table
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    fid INTEGER PRIMARY KEY,
    username TEXT,
    follower_count INTEGER,
    following_count INTEGER,
    following BOOLEAN,
    followed_by BOOLEAN,
    activeStatus TEXT,
    good_recent_cast TEXT,
    liked_recent_cast INTEGER DEFAULT 0
)
''')

# Function to fetch the last cast with at least 1 reaction and whether it was liked by the viewer
def fetch_recent_cast(fid):
    url = f"https://api.neynar.com/v1/farcaster/casts?fid={fid}&viewerFid={viewer_fid}&limit=5"
    headers = {
        "accept": "application/json",
        "api_key": api_key  # Replace with your actual API key
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        casts = response.json().get('result', {}).get('casts', [])
        for cast in casts:
            if cast.get('reactions', {}).get('count', 0) >= 1:
                liked_by_viewer = int(viewer_fid) in cast.get('reactions', {}).get('fids', [])
                return cast['hash'], liked_by_viewer  # Return the hash of the cast and liked status
        return None, False
    except Exception as e:
        print(f"Error fetching casts for FID {fid}: {e}")
        return None, False


# Update the function to update user data with the good_recent_cast field and liked_recent_cast status
def update_user_data(user_data, good_recent_cast, liked_recent_cast):
    print("Debug: user_data received in update_user_data:", user_data)
    print("Debug: good_recent_cast received in update_user_data:", good_recent_cast)
    print("Debug: liked_recent_cast received in update_user_data:", liked_recent_cast)
    cursor.execute('''
    UPDATE users
    SET username = ?, follower_count = ?, following_count = ?, following = ?, followed_by = ?, activeStatus = ?, good_recent_cast = ?, liked_recent_cast = ?
    WHERE fid = ?
    ''', (user_data['username'], user_data['followerCount'],
          user_data['followingCount'], user_data['viewerContext']['following'],
          user_data['viewerContext']['followedBy'], user_data['activeStatus'], good_recent_cast, liked_recent_cast, user_data['fid']))
    conn.commit()


# Example API Call to Fetch and Update User Data
def fetch_user_data(fid):
    url = f"https://api.neynar.com/v1/farcaster/user?fid={fid}&viewerFid={viewer_fid}"
    headers = {
        "accept": "application/json",
        "api_key": api_key  # Replace with your actual API key
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        user_data = response.json().get('result', {}).get('user', {})
        print(f"Fetched data for FID {fid}.")
        return user_data  # Return the fetched user data
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error for FID {fid}: {e}")
        if response.status_code == 429:  # Typically, 429 is the status code for rate limits
            print("Rate limit hit. Waiting before next request...")
            time.sleep(60)  # Wait for 60 seconds before the next request
    except Exception as e:
        print(f"Error fetching data for FID {fid}: {e}")

# Fetch and Update Data for a Range of Users
for fid in range(1, 25001):
    user_data = fetch_user_data(fid)
    recent_cast_hash, liked_recent_cast = fetch_recent_cast(fid)
    if user_data and recent_cast_hash:
        update_user_data(user_data, recent_cast_hash, liked_recent_cast)  # Update user data with all necessary arguments
    time.sleep(0.01)  # Introduce a small delay between requests


for fid in range(187700, 193001):
    user_data = fetch_user_data(fid)
    recent_cast_hash, liked_recent_cast = fetch_recent_cast(fid)
    if user_data and recent_cast_hash:
        update_user_data(user_data, recent_cast_hash, liked_recent_cast)  # Update user data with all necessary arguments
    time.sleep(0.01)  # Introduce a small delay between requests



# Query Database for Users Sorted by Follower Count
cursor.execute('SELECT * FROM users ORDER BY follower_count DESC')
for row in cursor.fetchall():
    print(row)

# Close Database Connection
conn.close()
