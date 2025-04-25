#!/usr/bin/env python3
"""
Script to fix ingestion of YouTube channel videos
"""

import argparse
import json
import logging
import subprocess
import sqlite3
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")
logger = logging.getLogger(__name__)


def add_video(db_path, video_id, title, source="youtube"):
    """Add a video to the database with 'todo' status"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Insert the video with 'todo' status
        cursor.execute(
            """
            INSERT OR REPLACE INTO videos(id, title, source, status, created_at, updated_at, retries)
            VALUES(?, ?, ?, 'todo', datetime('now'), datetime('now'), 0)
            """,
            (video_id, title, source),
        )

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error adding video {video_id} to database: {e}")
        return False


def extract_channel_videos(channel_url, limit=None):
    """
    Use yt-dlp directly to extract videos from a YouTube channel
    """
    videos = []
    limit_arg = f"--playlist-end {limit}" if limit else ""

    try:
        # Build the command to extract video info with yt-dlp
        cmd = f'yt-dlp --flat-playlist --skip-download --dump-json {limit_arg} "{channel_url}"'
        logger.info(f"Running command: {cmd}")

        # Run the command and capture output
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True
        )

        stdout, stderr = process.communicate()
        if process.returncode != 0:
            logger.error(
                f"yt-dlp command failed with code {process.returncode}: {stderr}"
            )
            return videos

        # Process the JSON output (one JSON object per line)
        for line in stdout.splitlines():
            if line.strip():
                try:
                    video_data = json.loads(line)
                    if "id" in video_data:
                        videos.append(
                            {
                                "id": video_data["id"],
                                "title": video_data.get(
                                    "title", f"Video {video_data['id']}"
                                ),
                            }
                        )
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse JSON: {line[:100]}...")

        logger.info(f"Found {len(videos)} videos in channel")

    except Exception as e:
        logger.error(f"Error extracting videos from channel {channel_url}: {e}")

    return videos


def main():
    parser = argparse.ArgumentParser(description="Fix YouTube channel video ingestion")
    parser.add_argument(
        "--channel-url", required=True, help="YouTube channel URL to ingest"
    )
    parser.add_argument("--limit", type=int, help="Limit number of videos to ingest")
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to the SQLite database (default: ../graph_memory.db)",
    )

    args = parser.parse_args()

    # Setup database path
    if args.db_path:
        db_path = Path(args.db_path)
    else:
        # Default path relative to script location
        db_path = Path(__file__).parent.parent / "graph_memory.db"

    if not db_path.exists():
        logger.error(f"Database file not found: {db_path}")
        return 1

    # Extract videos from the channel
    videos = extract_channel_videos(args.channel_url, args.limit)

    if not videos:
        logger.error("No videos found in channel.")
        return 1

    # Add videos to the database
    successful = 0
    for video in videos:
        if add_video(db_path, video["id"], video["title"]):
            successful += 1

    print(f"Successfully ingested {successful} out of {len(videos)} videos.")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
