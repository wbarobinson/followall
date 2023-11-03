import requests
import sqlite3
import datetime

# Define the API URL and headers
url = "https://api.neynar.com/v1/farcaster/recent-casts?viewerFid=2056&limit=100"
headers = {
    "accept": "application/json",
    "api_key": "NEYNAR_API_DOCS"  # Replace with your actual API key
}

# Fetch the response from the API
response = requests.get(url, headers=headers)
data = response.json()

# Extract FIDs of users who reacted to the casts
active_fids = set()
for cast in data['result']['casts']:
    active_fids.update(cast['reactions']['fids'])

# Connect to the database
conn = sqlite3.connect('farcaster_users.db')
cursor = conn.cursor()

# Update the recently_active column for the active FIDs
log_entries = []
for fid in active_fids:
    cursor.execute("UPDATE users SET recently_active = 1 WHERE fid = ?", (fid,))
    affected_rows = cursor.rowcount
    if affected_rows > 0:
        log_entry = f"Updated FID {fid} to recently active at {datetime.datetime.now()}"
        log_entries.append(log_entry)
        print(log_entry)

# Commit the changes and close the connection
conn.commit()
conn.close()

# Optionally, write the log entries to a file
with open('database_update_log.txt', 'a') as log_file:
    for entry in log_entries:
        log_file.write(entry + '\n')

print("Database updated with recently active users. Log entries have been recorded.")
