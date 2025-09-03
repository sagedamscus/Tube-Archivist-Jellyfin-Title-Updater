import os
import time
import sqlite3
import requests
from bs4 import BeautifulSoup

# Configuration
SERVER_URL = "http:jellyfin_url:port"
USERNAME = "username"
PASSWORD = "password"
SCAN_FOLDER = "/folder/with/archivist-media"
CHECK_INTERVAL = 15 * 60  # 15 minutes

# -------------------------------
# SQLite setup
# -------------------------------
DB_FILE = "updated_videos.db"
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()
c.execute('''
CREATE TABLE IF NOT EXISTS updated_videos (
    file_path TEXT PRIMARY KEY,
    video_id TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')
conn.commit()

# -------------------------------
# Jellyfin Client
# -------------------------------
class JellyfinClient:
    def __init__(self, server_url, username, password):
        self.server_url = server_url.rstrip('/')
        self.username = username
        self.password = password
        self.auth_token = None
        self.user_id = None
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

    def authenticate(self):
        auth_url = f"{self.server_url}/Users/AuthenticateByName"
        headers = {
            'X-Emby-Authorization': (
                'MediaBrowser '
                'Client="Python Jellyfin Client", '
                'Device="Python Script", '
                'DeviceId="None", '
                'Version="1.0.0"'
            ),
            'Content-Type': 'application/json',
        }
        data = {'Username': self.username, 'Pw': self.password}
        try:
            response = requests.post(auth_url, headers=headers, json=data)
            response.raise_for_status()
            auth_data = response.json()
            self.auth_token = auth_data['AccessToken']
            self.user_id = auth_data['User']['Id']
            self.headers['X-Emby-Token'] = self.auth_token
            return True
        except requests.exceptions.RequestException as e:
            print(f"Authentication failed: {e}")
            return False

    def get(self, endpoint, params=None):
        url = f"{self.server_url}/{endpoint.lstrip('/')}"
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            return None

    def post(self, endpoint, json_data=None):
        url = f"{self.server_url}/{endpoint.lstrip('/')}"
        try:
            response = requests.post(url, headers=self.headers, json=json_data)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"API POST failed: {e}")
            return None

# -------------------------------
# Helper functions
# -------------------------------
def get_youtube_title(video_id):
    url = f"https://youtu.be/{video_id}"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    title_tag = soup.find("title")
    if title_tag:
        return title_tag.text.replace(" - YouTube", "")
    return None

def scan_for_mp4(folder):
    """Recursively yield .mp4 files"""
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith(".mp4"):
                yield os.path.join(root, file)

def already_updated(file_path):
    c.execute("SELECT 1 FROM updated_videos WHERE file_path=?", (file_path,))
    return c.fetchone() is not None

def mark_as_updated(file_path, video_id):
    c.execute(
        "INSERT OR REPLACE INTO updated_videos (file_path, video_id) VALUES (?, ?)",
        (file_path, video_id)
    )
    conn.commit()

# -------------------------------
# Main processing loop
# -------------------------------
client = JellyfinClient(SERVER_URL, USERNAME, PASSWORD)
if not client.authenticate():
    print("Failed to authenticate. Exiting.")
    exit(1)

print("✅ Jellyfin authentication successful. Starting scan loop...")

while True:
    files_processed = 0

    for file_path in scan_for_mp4(SCAN_FOLDER):
        video_id = os.path.splitext(os.path.basename(file_path))[0]

        if already_updated(file_path):
            continue  # skip files already processed

        print(f"\n📂 Found file: {file_path}")
        print(f"🔑 Video ID: {video_id}")

        try:
            # Get YouTube title
            yt_title = get_youtube_title(video_id)
            if not yt_title:
                print(f"❌ Could not fetch YouTube title for {video_id}")
                continue
            print(f"🎬 YouTube title: {yt_title}")

            # Search Jellyfin
            search_params = {
                "SearchTerm": video_id,
                "IncludeItemTypes": "All",
                "Recursive": True,
                "Limit": 5
            }
            search_results = client.get(f"Users/{client.user_id}/Items", params=search_params)
            items = search_results.get("Items", []) if search_results else []

            if not items:
                print(f"⚠️ No matching video found in Jellyfin for {video_id}")
                continue

            item = items[0]
            item_id = item['Id']
            current_name = item['Name']
            print(f"✅ Found in Jellyfin: {current_name} (ID: {item_id})")

            # Fetch full metadata and update Name
            item_data = client.get(f"Items/{item_id}")
            if not item_data:
                print(f"⚠️ Failed to fetch metadata for {video_id}")
                continue

            item_data['Name'] = yt_title
            update_resp = client.post(f"Items/{item_id}", json_data=item_data)
            if update_resp and update_resp.ok:
                print(f"✅ Updated Jellyfin title to: {yt_title}")
                mark_as_updated(file_path, video_id)
                files_processed += 1

                # Refresh library section to immediately reflect changes
                parent_id = item_data.get("ParentId")
                if parent_id:
                    refresh_resp = client.post(f"Library/Refresh", json_data={"LibraryId": parent_id})
                    if refresh_resp and refresh_resp.ok:
                        print(f"🔄 Library refreshed for updated title.")
                    else:
                        print(f"⚠️ Failed to refresh library section.")
            else:
                print(f"⚠️ Failed to update metadata for {video_id}")

        except Exception as e:
            print(f"💥 Error processing {video_id}: {e}")

    if files_processed == 0:
        print("ℹ️ No new files found. Waiting before next scan...")

    time.sleep(CHECK_INTERVAL)
