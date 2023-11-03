import sqlite3
import requests
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get the API Key from the environment variable
api_key = os.getenv("NEYNAR_API_KEY")  # Ensure you have your API key in the .env file
signer_uuid = os.getenv("NEYNAR_FARCASTER_UUID")

def like_cast(fid, cast_hash, api_key, signer_uuid):
    print(f"Attempting to like cast {cast_hash} for user {fid}")  # Logging

    url = "https://api.neynar.com/v2/farcaster/reaction"
    headers = {
        "accept": "application/json",
        "api_key": api_key,
        "content-type": "application/json"
    }
    payload = {
        "signer_uuid": signer_uuid,
        "target": cast_hash,
        "reaction_type": "like"
    }

    print(f"Payload for like request: {payload}")  # Logging

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        print(f"Successfully liked cast {cast_hash} for user: {fid}")

        # Open a new connection to update the database
        with sqlite3.connect('farcaster_users.db') as conn:
            cursor = conn.cursor()
            cursor.execute('''
            UPDATE users SET liked_recent_cast = 1 WHERE fid = ?
            ''', (fid,))
            conn.commit()
        
    except requests.exceptions.HTTPError as e:
        print(f"Error liking cast {cast_hash} for user {fid}: {e}")
        print("Response content:", response.content.decode())

    print(f"Completed liking process for cast {cast_hash} of user {fid}")  # Logging

def like_latest_casts(api_key, signer_uuid):
    while True:
        # Open a new connection for each iteration
        with sqlite3.connect('farcaster_users.db') as conn:
            cursor = conn.cursor()

            # Fetch users whose recent cast has not been liked yet
            cursor.execute('SELECT fid, good_recent_cast FROM users WHERE good_recent_cast IS NOT NULL AND liked_recent_cast = 0')
            users_and_casts = cursor.fetchall()

        for fid, cast_hash in users_and_casts:
            like_cast(fid, cast_hash, api_key, signer_uuid)
            time.sleep(1)  # Pause for 1 second between each like request
        
        print("Waiting 30 seconds before refreshing data...")
        time.sleep(30)  # Wait for 30 seconds before fetching new data

def main():
    # Like the Latest Casts
    like_latest_casts(api_key, signer_uuid)

if __name__ == "__main__":
    main()
