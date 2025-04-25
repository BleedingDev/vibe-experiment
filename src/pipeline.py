import argparse
import logging
import asyncio
from pathlib import Path
import os
import subprocess
import json

from db import Database
from download import VideoDownloader
from transcriber import Transcriber
from graphiti_manager import GraphitiManager

# Configure logging
tlogging = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")

DOWNLOAD_DIR = Path("./downloads")

# Configurable maximum retry attempts for failed videos
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "1"))


def prepare(args):
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

                # Extract channel ID from the URL
                channel_id = None
                try:
                    downloader = VideoDownloader(
                        urls=[args.channel_url], output_dir=DOWNLOAD_DIR
                    )
                    channel_id = downloader.extract_channel_id(args.channel_url)
                except Exception as e:
                    logging.error(f"Error extracting channel ID: {e}")

                # Use channel ID as path prefix if available
                for video in videos:
                    # If we have a channel ID, use it as part of the path-based ID
                    if channel_id:
                        path_id = f"{channel_id}/{video['id']}"
                    else:
                        path_id = video["id"]
                    db.add_video(path_id, video["title"], "youtube")

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
            video_id = entry.get("id")
            title = entry.get("title", "")

            # Try to extract channel ID
            channel_id = None
            if video_id:
                for url in urls:
                    channel_id = downloader.extract_channel_id(url)
                    if channel_id:
                        break

            # Create path-based ID if channel ID is available
            if channel_id:
                path_id = f"{channel_id}/{video_id}"
                logging.info(f"Using path-based ID: {path_id}")
            else:
                path_id = video_id
                logging.info(f"Using video ID directly: {path_id}")

            db.add_video(path_id, title, "youtube")
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


def download_video(db, vid, video):
    """
    Download a video based on its metadata.
    Returns the file path to the downloaded or local video file.
    """
    db.update_video_status(vid, "downloading")

    if video["source"] == "local":
        file_path = Path(video["filepath"])
        logging.info(f"Using local file at {file_path}")
    else:
        # Parse path-based ID if it contains a channel ID
        if "/" in vid:
            channel_id, video_id = vid.split("/", 1)
            logging.info(
                f"Using path-based ID with channel {channel_id} and video {video_id}"
            )
            # Direct URL for the specific video
            url = f"https://youtu.be/{video_id}"
            # Set up downloader with channel ID path structure
            downloader = VideoDownloader(urls=[url], output_dir=DOWNLOAD_DIR)
            logging.info(f"Downloading video {video_id} from {url}")
            downloader._run_download()
            # Look for the downloaded mp4 in the channel's folder
            pattern = f"**/{channel_id}/{video_id}.mp4"
        else:
            # Just a plain video ID
            url = f"https://youtu.be/{vid}"
            downloader = VideoDownloader(urls=[url], output_dir=DOWNLOAD_DIR)
            logging.info(f"Downloading video {vid} from {url}")
            downloader._run_download()
            # Look for the downloaded mp4 anywhere
            pattern = f"**/{vid}.mp4"

        try:
            file_path = next(DOWNLOAD_DIR.glob(pattern))
            logging.info(f"Download completed: {file_path}")
        except StopIteration:
            error_msg = f"Downloaded file not found matching pattern {pattern}"
            logging.error(error_msg)
            raise FileNotFoundError(error_msg)

    return file_path


def transcribe_video(db, vid, file_path, transcriber):
    """
    Transcribe a video file and save the results to the database.
    """
    # Check if a transcription file already exists
    transcription_path = Path(file_path).with_name(
        f"{Path(file_path).stem}_transcription.md"
    )
    if transcription_path.exists():
        logging.info(f"Found existing transcription file at {transcription_path}")
        db.update_video_status(vid, "transcribed")
        logging.info(f"Video {vid} marked as transcribed (using existing file)")

        # We still need to process the transcription to extract data
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

        return segments, analysis

    # No existing transcription file, proceed with transcription
    db.update_video_status(vid, "transcribing")
    logging.info(f"Transcribing video {vid} from {file_path}")

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

    # Update status to transcribed so it can be picked up by the ingest step
    db.update_video_status(vid, "transcribed")
    logging.info(f"Transcription completed for video {vid}")
    return segments, analysis


async def ingest_transcription(db, vid, transcription_path):
    """
    Ingest a transcription file into the Graphiti knowledge graph.

    Args:
        db: Database instance
        vid: Video ID
        transcription_path: Path to the transcription file
    """
    try:
        logging.info(f"Ingesting transcription for video {vid} into Graphiti")
        logging.info(f"Transcription file path: {transcription_path}")

        # Check if the transcription file exists
        if not Path(transcription_path).exists():
            error_msg = f"Transcription file does not exist: {transcription_path}"
            logging.error(error_msg)
            db.update_video_status(vid, "failed", error_msg)
            return False

        # Initialize GraphitiManager
        try:
            graphiti = GraphitiManager()

            schema_initialized = await graphiti.initialize_schema()
            if not schema_initialized:
                error_msg = "Failed to initialize Neo4j schema"
                logging.error(error_msg)
                db.update_video_status(vid, "failed", error_msg)
                return False
        except Exception as e:
            error_msg = f"Failed to initialize GraphitiManager: {str(e)}"
            logging.error(error_msg)
            db.update_video_status(vid, "failed", error_msg)
            return False

        # Update status to ingesting
        db.update_video_status(vid, "ingesting")

        # Process and ingest the transcription
        try:
            success = await graphiti.ingest_transcription(transcription_path)
        except Exception as e:
            error_msg = f"Exception during graphiti.ingest_transcription: {str(e)}"
            logging.error(error_msg)
            db.update_video_status(vid, "failed", error_msg)
            return False

        if success:
            db.update_video_status(vid, "done")
            logging.info(f"Successfully ingested transcription for video {vid}")
            return True
        else:
            error_msg = "Failed to ingest transcription into Graphiti"
            db.update_video_status(vid, "failed", error_msg)
            logging.error(error_msg)
            return False

    except Exception as e:
        error_msg = f"Error ingesting transcription: {str(e)}"
        logging.error(error_msg)
        db.update_video_status(vid, "failed", error_msg)
        return False


def run_worker(args):
    """
    Process videos based on the specified step or run the full pipeline if no step is provided.
    """
    db = Database()
    transcriber = Transcriber()
    step = getattr(args, "step", None)

    # Log which step we're running
    if step:
        logging.info(f"Running pipeline step: {step}")
    else:
        logging.info("Running full pipeline")

    while True:
        # Get next video based on the current step
        video = db.get_next_video(step)
        if not video:
            logging.info("No more videos to process.")
            break

        # Log the video we're processing and its current status
        logging.info(f"Processing video {video['id']} (status: {video['status']})")

        vid = video["id"]
        try:
            # Process based on step, or run full pipeline if no step specified
            file_path = None

            # Download step
            if step is None or step == "download":
                file_path = download_video(db, vid, video)
                if step == "download":
                    db.update_video_status(vid, "downloaded")
                    logging.info(f"Download step completed for video {vid}")
                    continue
            else:
                # If we're only transcribing, find the existing file
                if video["source"] == "local":
                    file_path = Path(video["filepath"])
                else:
                    pattern = f"**/{vid}.mp4"
                    try:
                        file_path = next(DOWNLOAD_DIR.glob(pattern))
                    except StopIteration:
                        error_msg = (
                            f"Video file not found for {vid}. Must download first."
                        )
                        logging.error(error_msg)
                        db.update_video_status(vid, "failed", error_msg)
                        continue

            # Transcribe step
            if step is None or step == "transcribe":
                if file_path and file_path.exists():
                    transcribe_video(db, vid, file_path, transcriber)
                else:
                    error_msg = f"Video file not found at {file_path}"
                    logging.error(error_msg)
                    db.update_video_status(vid, "failed", error_msg)
                    continue  # Ingest step for Graphiti
            if step is None or step == "ingest":
                # Initialize transcription_path to None
                transcription_path = None

                # Look for the transcription file with _transcription.md suffix
                if video["source"] == "local":
                    base_path = Path(video["filepath"]).parent
                    transcription_path = base_path / f"{vid}_transcription.md"
                else:
                    # For path-based IDs, parse out the components
                    if "/" in vid:
                        channel_id, video_id = vid.split("/", 1)
                        # First try the channel specific folder
                        expected_path = (
                            DOWNLOAD_DIR / channel_id / f"{video_id}_transcription.md"
                        )

                        if expected_path.exists():
                            logging.info(
                                f"Found transcription in channel folder: {expected_path}"
                            )
                            transcription_path = expected_path
                        else:
                            # Fall back to looking in base download dir with full path-ID
                            fallback_path = DOWNLOAD_DIR / f"{vid}_transcription.md"

                            if fallback_path.exists():
                                logging.info(
                                    f"Found transcription in download dir with full ID: {fallback_path}"
                                )
                                transcription_path = fallback_path
                            else:
                                # Try with just the video ID part
                                video_only_path = (
                                    DOWNLOAD_DIR / f"{video_id}_transcription.md"
                                )
                                if video_only_path.exists():
                                    logging.info(
                                        f"Found transcription with just video ID: {video_only_path}"
                                    )
                                    transcription_path = video_only_path
                                else:
                                    # Try all files in downloads folder as a last resort
                                    for transcription_file in DOWNLOAD_DIR.glob(
                                        "**/*_transcription.md"
                                    ):
                                        if video_id in str(transcription_file):
                                            logging.info(
                                                f"Found transcription by searching: {transcription_file}"
                                            )
                                            transcription_path = transcription_file
                                            break
                    else:
                        # Regular video ID - try direct match first
                        direct_path = DOWNLOAD_DIR / f"{vid}_transcription.md"
                        if direct_path.exists():
                            transcription_path = direct_path
                        else:
                            # Try searching in all subfolders
                            for transcription_file in DOWNLOAD_DIR.glob(
                                "**/*_transcription.md"
                            ):
                                if vid in str(transcription_file):
                                    logging.info(
                                        f"Found transcription by searching: {transcription_file}"
                                    )
                                    transcription_path = transcription_file
                                    break

                if transcription_path and transcription_path.exists():
                    # Use asyncio to run the async ingest function
                    logging.info(f"Using transcription file: {transcription_path}")
                    try:
                        await_result = asyncio.run(
                            ingest_transcription(db, vid, transcription_path)
                        )
                        if not await_result:
                            # If ingest failed, log it but continue with full pipeline processing
                            logging.warning(
                                f"Ingestion failed for {vid} but continuing with pipeline"
                            )
                            if step == "ingest":
                                # If we're only running ingest step, don't continue to other videos
                                continue
                    except Exception as e:
                        error_msg = f"Exception during ingest_transcription: {str(e)}"
                        logging.error(error_msg)
                        db.update_video_status(vid, "failed", error_msg)
                        if step == "ingest":
                            continue
                else:
                    error_msg = f"Transcription file not found at any expected location for video {vid}"
                    logging.error(error_msg)
                    if step == "ingest":
                        db.update_video_status(vid, "failed", error_msg)
                        continue

            # Mark as done if we've completed all steps and we're running the full pipeline
            if step is None:
                db.update_video_status(vid, "done")
                logging.info(f"Processed video {vid} (full pipeline)")
            # If we're running a specific step, the status update has already been handled by that step

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

    # Check if a specific step is requested for retry
    step = getattr(args, "step", None)
    if step == "download":
        # Reset to initial state for downloading
        db.update_video_status(vid, "todo", None)
        print(f"Video {vid} reset to 'todo' state for downloading.")
    elif step == "transcribe":
        # Set to downloaded state to indicate ready for transcription
        db.update_video_status(vid, "downloaded", None)
        print(f"Video {vid} set to 'downloaded' state for transcription.")
    elif step == "ingest":
        # Set to transcribed state to indicate ready for ingestion into Graphiti
        db.update_video_status(vid, "transcribed", None)
        print(f"Video {vid} set to 'transcribed' state for ingestion into Graphiti.")
    else:
        # Default: reset to todo for full pipeline
        db.update_video_status(vid, "todo", None)
        print(f"Video {vid} reset to 'todo' state for full processing.")


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


async def search(args):
    """
    Search the knowledge graph for information matching a query.

    Args:
        args: Command line arguments including a 'query' parameter
    """
    try:
        # Initialize GraphitiManager
        graphiti = GraphitiManager()

        # Get the search query and limit from command line arguments
        query = args.query
        limit = args.limit

        print(f"Searching graph for: '{query}' (limit: {limit} results)")

        # Perform the search
        results = await graphiti.search_graph(query=query, limit=limit)

        if not results:
            print("No results found.")
            return

        print(f"\nFound {len(results)} results:")

        # Process and display the results
        for i, result in enumerate(results):
            print(f"\n--- Result {i + 1} ---")

            try:
                # Try to access attributes of EntityEdge objects directly
                if hasattr(result, "edge_content"):
                    print(f"Content: {result.edge_content}")
                elif hasattr(result, "episode_body"):
                    print(f"Content: {result.episode_body}")
                elif isinstance(result, dict):
                    # Try dictionary style access if it's a dict
                    fact = result.get(
                        "fact", result.get("content", "No content available")
                    )
                    print(f"Content: {fact}")
                else:
                    # Fallback: just print the string representation
                    print(f"Content: {str(result)}")

                # Try to get source description if available
                source_desc = None
                if hasattr(result, "source_description"):
                    source_desc = result.source_description
                elif isinstance(result, dict):
                    source_desc = result.get("source_description")

                if source_desc:
                    print(f"Source: {source_desc}")

            except Exception as e:
                print(f"Error processing search result: {str(e)}")

            # If there's a video ID, format a clickable link with timestamp if available
            video_id = None

            # Try to extract video ID and timestamp from the result metadata
            try:
                if isinstance(result, dict) and "source_description" in result:
                    desc = result["source_description"]
                    if "video" in desc.lower():
                        parts = desc.split()
                        for part in parts:
                            if "video" not in part.lower():
                                video_id = part
                                break
                elif (
                    hasattr(result, "source_description") and result.source_description
                ):
                    desc = result.source_description
                    if "video" in desc.lower():
                        parts = desc.split()
                        for part in parts:
                            if "video" not in part.lower():
                                video_id = part
                                break
            except Exception as e:
                print(f"Error extracting video ID: {str(e)}")

            # If we have a video ID, try to create a YouTube link
            if video_id and video_id != "video":
                # Remove any "for" prefix if present
                if video_id.startswith("for"):
                    video_id = video_id[3:]

                # Handle path-based IDs (channel_id/video_id format)
                if "/" in video_id:
                    _, video_id = video_id.split("/", 1)

                print(f"Video ID: {video_id}")

                # Check if there's timestamp info in the fact or metadata
                import re

                timestamp_match = re.search(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", fact)
                if timestamp_match:
                    minutes = int(timestamp_match.group(1))
                    seconds = int(timestamp_match.group(2))
                    hours = (
                        int(timestamp_match.group(3)) if timestamp_match.group(3) else 0
                    )
                    timestamp_seconds = hours * 3600 + minutes * 60 + seconds

                    print(
                        f"YouTube link: https://youtu.be/{video_id}?t={timestamp_seconds}"
                    )

    except Exception as e:
        print(f"Error searching graph: {str(e)}")


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline orchestrator for graph-memory"
    )
    sub = parser.add_subparsers(dest="cmd")

    ing = sub.add_parser("prepare", help="Prepare videos into pipeline")
    ing.add_argument("--channel-url", help="YouTube channel or playlist URL")
    ing.add_argument("--video-urls", nargs="+", help="List of YouTube video URLs")
    ing.add_argument("--local-paths", nargs="+", help="List of local video file paths")
    ing.add_argument("--limit", type=int, help="Limit number of videos to prepare")

    # Run command with optional step parameter
    run_parser = sub.add_parser("run", help="Run pipeline worker")
    run_parser.add_argument(
        "step",
        nargs="?",
        choices=["download", "transcribe", "ingest"],
        help="Specific step to run (default: run full pipeline)",
    )

    sub.add_parser("status", help="Show pipeline status")

    rep = sub.add_parser("retry", help="Retry a failed video")
    rep.add_argument("video_id", help="ID of video to retry")
    rep.add_argument(
        "--step",
        choices=["download", "transcribe", "ingest"],
        help="Specific step to retry (download or transcribe or ingest)",
    )

    sub.add_parser("errors", help="List all failed videos with errors")

    # Add search command
    search_parser = sub.add_parser("search", help="Search the knowledge graph")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of results to return (default: 5)",
    )

    args = parser.parse_args()
    if args.cmd == "prepare":
        prepare(args)
    elif args.cmd == "run":
        run_worker(args)
    elif args.cmd == "status":
        status(args)
    elif args.cmd == "retry":
        retry(args)
    elif args.cmd == "errors":
        errors(args)
    elif args.cmd == "search":
        # Search is async, so we need to run it with asyncio
        asyncio.run(search(args))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
