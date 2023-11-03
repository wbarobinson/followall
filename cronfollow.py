import requests
import sqlite3
import time
import datetime
from dotenv import load_dotenv
import os
from apscheduler.schedulers.blocking import BlockingScheduler

# Load environment variables from .env file
load_dotenv()

# Get the API Key from the environment variable
api_key = os.getenv("NEYNAR_API_KEY")  # Ensure you have your API key in the .env file
signer_uuid = os.getenv("NEYNAR_FARCASTER_UUID")
viewer_fid = os.getenv("FARCASTER_DEVELOPER_FID")

def fetch_recently_active_fids(api_key):
    print("Fetching recently active FIDs...")
    url = "https://api.neynar.com/v1/farcaster/recent-casts?viewerFid=2056&limit=100"
    headers = {
        "accept": "application/json",
        "api_key": api_key
    }
    response = requests.get(url, headers=headers)
    data = response.json()

    active_fids = set()
    total_reactions = 0
    for cast in data['result']['casts']:
        active_fids.update(cast['reactions']['fids'])
        total_reactions += len(cast['reactions']['fids'])

    print(f"Fetched {len(data['result']['casts'])} casts with a total of {total_reactions} reactions.")
    
    return active_fids

def fetch_recent_cast_hash(fid, api_key):
    url = f"https://api.neynar.com/v1/farcaster/casts?fid={fid}&limit=1"
    headers = {
        "accept": "application/json",
        "api_key": api_key
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    casts = data.get('result', {}).get('casts', [])
    if casts:
        return casts[0].get('hash')
    return None

def update_recent_cast_hash_in_database(fid, recent_cast_hash, cursor, conn):
    cursor.execute('UPDATE users SET recent_cast_hash = ? WHERE fid = ?', (recent_cast_hash, fid))
    conn.commit()

def clear_recently_active_column(cursor, conn):
    cursor.execute("UPDATE users SET recently_active = 0")
    conn.commit()

def update_database_with_recently_active(cursor, conn, active_fids):
    for fid in active_fids:
        cursor.execute("UPDATE users SET recently_active = 1 WHERE fid = ?", (fid,))
    conn.commit()

def follow_user(fid, cursor, api_key, signer_uuid, conn):
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

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        update_following_status(fid, cursor, conn)
        log_follow_action(fid)
    except requests.exceptions.HTTPError as e:
        print(f"Error following user {fid}: {e}")

def like_cast(fid, cast_hash, api_key, signer_uuid):
    print(f"Attempting to like cast {cast_hash} for user {fid}")

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

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        print(f"Successfully liked cast {cast_hash} for user: {fid}")
    except requests.exceptions.HTTPError as e:
        print(f"Error liking cast {cast_hash} for user {fid}: {e}")

def update_following_status(fid, cursor, conn):
    cursor.execute('UPDATE users SET following = 1 WHERE fid = ?', (fid,))
    conn.commit()

def update_liked_cast_status(fid, cast_hash, cursor, conn):
    cursor.execute('UPDATE users SET liked_recent_cast = 1 WHERE fid = ? AND recent_cast_hash = ?', (fid, cast_hash))
    conn.commit()

def log_follow_action(fid):
    timestamp = datetime.datetime.now()
    print(f"{timestamp}: Followed user with FID {fid}")

def follow_recently_active_users(cursor, api_key, signer_uuid, conn):
    print("Processing recently active users...")
    
    # Select all recently active users, including opt-out status
    cursor.execute('SELECT fid, recent_cast_hash, following, followed_by, opt_out FROM users WHERE recently_active = 1')
    users_and_casts = cursor.fetchall()

    if not users_and_casts:
        print("No recently active users found.")
        return

    print(f"Found {len(users_and_casts)} recently active users.")

    for fid, cast_hash, following, followed_by, opt_out in users_and_casts:
        # Check if user has opted out
        if opt_out == 1:
            print(f"User {fid} has opted out. Skipping.")
            continue

        # Follow user if not already following and hasn't opted out
        if following == 0:
            follow_user(fid, cursor, api_key, signer_uuid, conn)

        # Like cast if not already followed by the user and hasn't opted out
        if followed_by == 0 and cast_hash:
            print(f"User {fid} is not followed by us. Attempting to like recent cast: {cast_hash}")
            like_cast(fid, cast_hash, api_key, signer_uuid)
        else:
            print(f"User {fid} is already followed by us or has no recent cast. Skipping.")
        
        time.sleep(1)  # Pause for 1 second between each action

def update_opt_out_status(fid, opt_out_status, cursor, conn):
    try:
        query = "UPDATE users SET opt_out = ? WHERE fid = ?"
        cursor.execute(query, (opt_out_status, fid))
        conn.commit()
        print(f"Updated opt-out status for FID {fid} to {opt_out_status}")
    except Exception as e:
        print(f"Failed to update opt-out status for FID {fid}: {e}")
        conn.rollback()


def fetch_and_update_opt_out_users(api_key, cast_hash, cursor, conn):
    url = f"https://api.neynar.com/v1/farcaster/cast-likes?castHash={cast_hash}&viewerFid=2056&limit=25"
    headers = {
        "accept": "application/json",
        "api_key": api_key
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch likes for cast {cast_hash}: {response.status_code}")
        return

    data = response.json()

    # Extract FIDs from the nested structure
    liked_fids = [like['reactor']['fid'] for like in data.get('result', {}).get('likes', [])]

    if not liked_fids:
        print(f"No likes found for cast {cast_hash}.")
        return

    for fid in liked_fids:
        # Update opt-out status
        update_opt_out_status(fid, 1, cursor, conn)

    print(f"Updated opt-out status for {len(liked_fids)} users based on likes for cast {cast_hash}.")



def like_recent_casts(cursor, api_key, signer_uuid, conn):
    print("Checking for casts to like...")
    cursor.execute('SELECT fid, recent_cast_hash, opt_out FROM users WHERE recent_cast_hash IS NOT NULL AND liked_recent_cast = 0 AND following = 1 AND followed_by = 0')
    users_and_casts = cursor.fetchall()
    
    if not users_and_casts:
        print("No recent casts found to like.")
        return

    print(f"Found {len(users_and_casts)} recent casts to potentially like.")
    for fid, cast_hash, opt_out in users_and_casts:
        # Check if user has opted out
        if opt_out == 1:
            print(f"User {fid} has opted out. Skipping like.")
            continue

        print(f"Checking if cast {cast_hash} from user {fid} can be liked...")
        like_cast(fid, cast_hash, api_key, signer_uuid)
        time.sleep(1)  # Pause for 1 second between each like request

def main_task():
    print("Main task started.")
    # Initialize Database Connection
    conn = sqlite3.connect('farcaster_users.db')
    cursor = conn.cursor()

    # Usage example (within the main task or another appropriate place):
    fetch_and_update_opt_out_users(api_key, "0x713ee58f1e803f22e505254a4c2f77e1d1c3e5cb", cursor, conn)


    # Clear the recently_active column
    clear_recently_active_column(cursor, conn)

    # Fetch and Update Recently Active Users
    active_fids = fetch_recently_active_fids(api_key)
    update_database_with_recently_active(cursor, conn, active_fids)

    #  Fetch and update recent cast hashes for recently active users
    for fid in active_fids:
        recent_cast_hash = fetch_recent_cast_hash(fid, api_key)
        if recent_cast_hash:
            update_recent_cast_hash_in_database(fid, recent_cast_hash, cursor, conn)

    # Follow Recently Active Users and Like their Recent Casts
    follow_recently_active_users(cursor, api_key, signer_uuid, conn)

    # Like Recent Casts of Followed Users Who Aren't Following Back
    like_recent_casts(cursor, api_key, signer_uuid, conn)



    # Close Database Connection
    conn.close()

    print(f"Task completed at {datetime.datetime.now()}")


# Scheduler to run the task immediately and then every 5 minutes
scheduler = BlockingScheduler()
scheduler.add_job(main_task, 'interval', minutes=75, next_run_time=datetime.datetime.now())

print("Scheduler started. Running the task immediately and then every 5 minutes.")
scheduler.start()