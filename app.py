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
    liked_recent_cast INTEGER DEFAULT 0,
    recently_active INTEGER DEFAULT 0, 
    recent_cast_hash TEXT

)
''')

# Function to Update Data into Database
def update_user_data(user_data):
    cursor.execute('''
    UPDATE users
    SET username = ?, follower_count = ?, following_count = ?, following = ?, followed_by = ?, activeStatus = ?
    WHERE fid = ?
    ''', (user_data['username'], user_data['followerCount'],
          user_data['followingCount'], user_data['viewerContext']['following'],
          user_data['viewerContext']['followedBy'], user_data['activeStatus'], user_data['fid']))
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
        update_user_data(user_data)
        print(f"Data updated for FID {fid}.")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error for FID {fid}: {e}")
        if response.status_code == 429:  # Typically, 429 is the status code for rate limits
            print("Rate limit hit. Waiting before next request...")
            time.sleep(60)  # Wait for 60 seconds before the next request
    except Exception as e:
        print(f"Error fetching data for FID {fid}: {e}")

# Fetch and Update Data for a Range of Users
for fid in range(1, 25001):
    fetch_user_data(fid)
    time.sleep(0.01)  # Introduce a small delay between requests to avoid hitting rate limits

for fid in range(187700, 193001):
    fetch_user_data(fid)
    time.sleep(0.02)  # Introduce a small delay between requests to avoid hitting rate limits


# Query Database for Users Sorted by Follower Count
cursor.execute('SELECT * FROM users ORDER BY follower_count DESC')
for row in cursor.fetchall():
    print(row)

# Close Database Connection
conn.close()
