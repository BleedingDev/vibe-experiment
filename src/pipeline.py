import argparse
import logging
from pathlib import Path
import os
import subprocess
import json

from db import Database
from download import VideoDownloader
from transcriber import Transcriber

# Configure logging
tlogging = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")

DOWNLOAD_DIR = Path("./downloads")

# Configurable maximum retry attempts for failed videos
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "1"))


def ingest(args):
    db = Database()
    # Remote ingestion: fetch metadata without downloading
    if args.channel_url or args.video_urls:
        # Handle channel URLs with direct yt-dlp call for more reliable extraction
        if args.channel_url:
            logging.info(f"Extracting videos from channel URL: {args.channel_url}")
            limit_arg = f"--playlist-end {args.limit}" if args.limit else ""
            try:
                # Use subprocess to call yt-dlp directly with JSON output
                cmd = f'yt-dlp --flat-playlist --skip-download --dump-json {limit_arg} "{args.channel_url}"'
                logging.info(f"Running command: {cmd}")

                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=True,
                    text=True,
                )

                stdout, stderr = process.communicate()
                if process.returncode != 0:
                    logging.error(
                        f"yt-dlp command failed with code {process.returncode}: {stderr}"
                    )
                    print(f"Error extracting channel videos: {stderr}")
                    return

                # yt-dlp returns one JSON object per line
                videos = []
                for line in stdout.splitlines():
                    if line.strip():
                        try:
                            video_data = json.loads(line)
                            # Skip YouTube Shorts (usually shorter than regular videos)
                            if "id" in video_data and not video_data.get(
                                "title", ""
                            ).lower().startswith("short"):
                                # Check if it's a short video by duration if available
                                duration = video_data.get("duration")
                                if (
                                    duration and int(duration) < 60
                                ):  # Skip videos under 60 seconds (likely Shorts)
                                    logging.info(
                                        f"Skipping short video: {video_data.get('title')}"
                                    )
                                    continue

                                videos.append(
                                    {
                                        "id": video_data["id"],
                                        "title": video_data.get(
                                            "title", f"Video {video_data['id']}"
                                        ),
                                    }
                                )
                        except json.JSONDecodeError:
                            logging.warning(f"Could not parse JSON: {line[:100]}...")

                logging.info(f"Found {len(videos)} videos in channel (after filtering)")

                # Apply our own limit to the total count
                if args.limit and len(videos) > args.limit:
                    videos = videos[: args.limit]
                    logging.info(f"Limited to {len(videos)} videos as requested")

                # Add videos to database
                for video in videos:
                    db.add_video(video["id"], video["title"], "youtube")

                print(f"Ingested {len(videos)} videos from channel")
                return
            except Exception as e:
                logging.error(f"Error processing channel URL: {e}")
                print(f"Error processing channel URL: {str(e)}")
                return

        # Handle individual video URLs using VideoDownloader
        urls = [args.channel_url] if args.channel_url else args.video_urls
        downloader = VideoDownloader(
            urls=urls, output_dir=DOWNLOAD_DIR, limit=args.limit
        )
        entries = downloader.get_info()
        for entry in entries:
            vid = entry.get("id")
            title = entry.get("title", "")
            db.add_video(vid, title, "youtube")
        print(f"Ingested {len(entries)} videos")
        return
    # Local ingestion: directly register local file paths
    if args.local_paths:
        for path_str in args.local_paths:
            path = Path(path_str)
            db.add_video(path.stem, path.name, "local", filepath=str(path))
        print(f"Ingested {len(args.local_paths)} local videos")
        return
    # No ingestion flags provided
    print(
        "No ingestion flags provided. Use --channel-url, --video-urls or --local-paths."
    )


def run_worker(args):
    db = Database()
    transcriber = Transcriber()
    while True:
        video = db.get_next_video()
        if not video:
            logging.info("No more videos to process.")
            break
        vid = video["id"]
        try:
            # Download
            db.update_video_status(vid, "downloading")
            if video["source"] == "local":
                file_path = Path(video["filepath"])
            else:
                url = f"https://youtu.be/{vid}"
                downloader = VideoDownloader(urls=[url], output_dir=DOWNLOAD_DIR)
                downloader._run_download()
                # find downloaded mp4
                pattern = f"**/{vid}.mp4"
                file_path = next(DOWNLOAD_DIR.glob(pattern))
            # Transcribe & analyze
            db.update_video_status(vid, "transcribing")
            segments, analysis = transcriber.transcribe(file_path)
            # Persist transcript segments
            for start, end, text in segments:
                db.save_subtitle(vid, int(start), int(end), text)
            db.save_analysis(
                vid,
                analysis.get("summary", ""),
                analysis.get("topics", []),
                analysis.get("key_terms", []),
                analysis.get("recommended_chapters", []),
            )
            db.update_video_status(vid, "done")
            logging.info(f"Processed video {vid}")
        except Exception as e:
            logging.exception(f"Error processing {vid}")
            # Increment retry count and decide next status
            retries = db.increment_retries(vid)
            if retries < MAX_RETRIES:
                db.update_video_status(vid, "todo", str(e))
                logging.info(f"Retrying video {vid} (attempt {retries}/{MAX_RETRIES})")
            else:
                db.update_video_status(vid, "failed", str(e))
                logging.error(f"Video {vid} failed after {retries} attempts")


def status(args):
    db = Database()
    vids = db.list_videos()
    counts = {}
    for v in vids:
        counts[v["status"]] = counts.get(v["status"], 0) + 1
    print("Pipeline status:")
    for status, count in counts.items():
        print(f"  {status}: {count}")
    # Show recent errors for failed videos
    failed = [v for v in vids if v["status"] == "failed" and v.get("error")]
    if failed:
        # sort by updated_at descending
        failed_sorted = sorted(
            failed, key=lambda x: x.get("updated_at", ""), reverse=True
        )
        print("\nRecent errors:")
        for v in failed_sorted[:5]:
            print(f"  {v['id']}: {v.get('error')} (at {v.get('updated_at')})")


def retry(args):
    db = Database()
    vid = args.video_id
    videos = db.list_videos()
    if not any(v["id"] == vid for v in videos):
        print(f"Video {vid} not found in database.")
        return
    db.update_video_status(vid, "todo", None)
    print(f"Video {vid} reset to TODO state.")


def errors(args):
    """
    List all videos that failed with their error messages.
    """
    db = Database()
    vids = db.list_videos()
    failed = [v for v in vids if v["status"] == "failed" and v.get("error")]
    if not failed:
        print("No failed videos.")
        return
    # sort by updated_at descending
    failed_sorted = sorted(failed, key=lambda x: x.get("updated_at", ""), reverse=True)
    print("Failed videos and errors:")
    for v in failed_sorted:
        print(f"  ID: {v['id']}")
        print(f"    Title: {v.get('title', '<unknown>')}")
        print(f"    Error: {v.get('error')}")
        print(f"    Updated At: {v.get('updated_at')}")


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline orchestrator for graph-memory"
    )
    sub = parser.add_subparsers(dest="cmd")

    ing = sub.add_parser("ingest", help="Ingest videos into pipeline")
    ing.add_argument("--channel-url", help="YouTube channel or playlist URL")
    ing.add_argument("--video-urls", nargs="+", help="List of YouTube video URLs")
    ing.add_argument("--local-paths", nargs="+", help="List of local video file paths")
    ing.add_argument("--limit", type=int, help="Limit number of videos to ingest")

    sub.add_parser("run", help="Run pipeline worker")

    sub.add_parser("status", help="Show pipeline status")

    rep = sub.add_parser("retry", help="Retry a failed video")
    rep.add_argument("video_id", help="ID of video to retry")

    sub.add_parser("errors", help="List all failed videos with errors")

    args = parser.parse_args()
    if args.cmd == "ingest":
        ingest(args)
    elif args.cmd == "run":
        run_worker(args)
    elif args.cmd == "status":
        status(args)
    elif args.cmd == "retry":
        retry(args)
    elif args.cmd == "errors":
        errors(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
