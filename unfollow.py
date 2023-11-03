import sqlite3
import requests
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get the API Key from the environment variable (just create a .env with these variables)
api_key = os.getenv("NEYNAR_API_KEY")  # Ensure you have your API key in the .env file
signer_uuid = os.getenv("NEYNAR_FARCASTER_UUID")

def unfollow_user(fid, cursor, api_key, signer_uuid, conn):
    print("API Key:", api_key)
    print("Signer UUID:", signer_uuid)

    url = "https://api.neynar.com/v2/farcaster/user/follow"
    headers = {
        "accept": "application/json",
        "api_key": api_key,
        "content-type": "application/json"
    }
    payload = {
        "signer_uuid": signer_uuid,
        "target_fids": [fid]
    }

    print("Payload:", payload)  # Debugging

    try:
        response = requests.delete(url, json=payload, headers=headers)
        response.raise_for_status()
        print(f"Successfully unfollowed user: {fid}")
        update_following_status(fid, cursor, conn)
    except requests.exceptions.HTTPError as e:
        print(f"Error unfollowing user {fid}: {e}")
        print("Response content:", response.content.decode())

def update_following_status(fid, cursor, conn):
    cursor.execute('''
    UPDATE users SET following = 0 WHERE fid = ?
    ''', (fid,))
    conn.commit()

def unfollow_users(fids, cursor, api_key, signer_uuid, conn):
    for fid in fids:
        unfollow_user(fid, cursor, api_key, signer_uuid, conn)
        time.sleep(.1)  # Pause for 1 second between each follow request

def main():
    # Initialize Database Connection
    conn = sqlite3.connect('farcaster_users.db')
    cursor = conn.cursor()

    # Define the starting point (the user id where it stopped)
    start_from_id = 1

    # Fetch Users with More Than 100 Followers and Not Already Followed, starting from start_from_id
    cursor.execute('SELECT fid FROM users WHERE fid > ? AND following = 1 AND followed_by = 0 AND activeStatus != "active"', (start_from_id,))
    fids_to_unfollow = [row[0] for row in cursor.fetchall()]

    # Unfollow the Users
    unfollow_users(fids_to_unfollow, cursor, api_key, signer_uuid, conn)

    # Close Database Connection
    conn.close()

if __name__ == "__main__":
    main()
