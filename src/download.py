#!/usr/bin/env python3
"""
YouTube Video Downloader

A script to download videos from YouTube using yt-dlp.
Can download entire channels or specific videos from a list.
Uses modern Python features, proper error handling, and async operations.
"""

import argparse
import asyncio
import logging
import shutil
import sys
from pathlib import Path
from typing import Any

# You'll need to install yt-dlp: pip install yt-dlp
import yt_dlp  # type: ignore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("yt-channel-downloader")


class VideoDownloader:
    """Handle downloading of YouTube videos or channels."""

    def __init__(
        self,
        urls: list[str],
        output_dir: Path,
        format: str = "best",
        limit: int | None = None,
        extract_audio: bool = False,
        extract_subs: bool = False,
        playlist_items: str | None = None,
        cookies_file: Path | None = None,
        use_native_progress: bool = False,
    ):
        self.urls = urls
        self.output_dir = output_dir
        self.format = format
        self.limit = limit
        self.extract_audio = extract_audio
        self.extract_subs = extract_subs
        self.playlist_items = playlist_items
        self.cookies_file = cookies_file
        self.use_native_progress = use_native_progress

    def _progress_hook(self, d: dict[str, Any]) -> None:
        try:
            # Catch any exception in the progress hook to prevent download interruption
            pass
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)

    def _get_ydl_opts(self) -> dict[str, Any]:
        """
        Configure yt-dlp options based on instance settings.

        Returns:
            Dictionary of options for yt-dlp
        """
        # Use yt-dlp template variables directly, do not slugify here!
        output_template = str(self.output_dir / "%(channel_id)s/%(id)s.%(ext)s")

        ydl_opts = {
            "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
            if self.format == "best"
            else self.format,
            "outtmpl": output_template,
            "ignoreerrors": True,
            "nooverwrites": True,
            "continue": True,
            "writethumbnail": True,
            "embedthumbnail": True,
            "writeinfojson": True,
            "writesubtitles": self.extract_subs,
            "writeautomaticsub": self.extract_subs,
            "subtitleslangs": ["en"] if self.extract_subs else [],
            "playlistend": self.limit if self.limit else None,
            "merge_output_format": "mp4",
            "retries": 10,
            "fragment_retries": 10,
            "skip_unavailable_fragments": True,
            "buffer_size": 16 * 1024,
        }

        ydl_opts["postprocessor_args"] = ["-movflags", "faststart"]

        if self.playlist_items:
            ydl_opts["playlist_items"] = self.playlist_items

        # Set up postprocessors based on options
        postprocessors = []
        if self.extract_audio:
            postprocessors.append(
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }
            )
            postprocessors.append({"key": "FFmpegMetadata"})
            # Optionally embed thumbnail for mp3
            postprocessors.append({"key": "EmbedThumbnail"})
        else:
            postprocessors.append(
                {"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}
            )
            postprocessors.append({"key": "FFmpegMetadata"})
            postprocessors.append({"key": "EmbedThumbnail"})
        ydl_opts["postprocessors"] = postprocessors

        if self.cookies_file:
            ydl_opts["cookiefile"] = str(self.cookies_file)

        return ydl_opts

    async def download(self) -> None:
        """Download all videos asynchronously."""
        try:
            if len(self.urls) == 1:
                logger.info(f"Starting download of URL: {self.urls[0]}")
            else:
                logger.info(f"Starting download of {len(self.urls)} URLs")
            logger.info(f"Output directory: {self.output_dir}")

            # Ensure output directory exists
            self.output_dir.mkdir(parents=True, exist_ok=True)

            # Check for .part files in the output directory
            part_files = list(self.output_dir.glob("**/*.part"))
            if part_files:
                logger.info(
                    f"Found {len(part_files)} partial downloads that will be resumed"
                )

            # Create a separate thread for yt-dlp since it's blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._run_download)

            # Verify successful download by checking if there are still .part files
            remaining_part_files = list(self.output_dir.glob("**/*.part"))
            if remaining_part_files:
                logger.warning(
                    f"Found {len(remaining_part_files)} incomplete downloads after completion. "
                    "These files may need to be downloaded again."
                )
                for part_file in remaining_part_files[:5]:  # List first 5 part files
                    logger.warning(f"Incomplete file: {part_file}")
                if len(remaining_part_files) > 5:
                    logger.warning(f"... and {len(remaining_part_files) - 5} more")
            else:
                logger.info("All downloads completed and processed successfully!")

        except Exception as e:
            logger.error(f"Error during download: {e}", exc_info=True)
            raise

    def _run_download(self) -> None:
        """Execute the download process with yt-dlp."""
        options = self._get_ydl_opts()

        try:
            with yt_dlp.YoutubeDL(options) as ydl:
                # Process each URL
                if len(self.urls) > 1:
                    logger.info(f"Will download {len(self.urls)} videos/channels")

                # First extract info to get details about content
                logger.info("Extracting video information...")

                # Count total videos to be downloaded
                total_videos = 0
                single_videos = []
                playlists = []

                for url in self.urls:
                    try:
                        info_dict = ydl.extract_info(url, download=False)

                        if info_dict is None:
                            logger.warning(f"Could not extract information for {url}")
                            continue

                        # Determine if this is a playlist/channel or single video
                        if "entries" in info_dict:
                            # This is a playlist or channel
                            videos = list(info_dict["entries"])
                            video_count = len(videos)
                            channel_name = info_dict.get(
                                "channel", info_dict.get("uploader", "Unknown Channel")
                            )
                            logger.info(
                                f"Found playlist/channel: {channel_name} with {video_count} videos"
                            )
                            total_videos += video_count
                            playlists.append(url)
                        else:
                            # This is a single video
                            logger.info(
                                f"Found video: {info_dict.get('title', 'Unknown')}"
                            )
                            total_videos += 1
                            single_videos.append(url)
                    except Exception as e:
                        logger.error(f"Error extracting info for {url}: {e}")

                if self.limit and total_videos > self.limit:
                    logger.info(
                        f"Will download up to {self.limit} videos due to specified limit"
                    )

                # Now actually download
                logger.info(f"Starting download of {total_videos} videos...")
                ydl.download(self.urls)

        except yt_dlp.utils.DownloadError as e:
            logger.error(f"Download error: {e}")
            if "ffmpeg" in str(e).lower():
                logger.error(
                    "This appears to be an ffmpeg error. Make sure ffmpeg is installed correctly."
                )
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)

    def get_info(self):
        """
        Retrieve video metadata (id, title, etc.) for the configured URLs without downloading media.
        """
        info_entries = []
        # Use minimal options for metadata extraction: quiet, no warnings, skip media download
        opts: dict[str, Any] = {
            "ignoreerrors": True,
            "quiet": False,  # Temporarily enable output to debug
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": True,  # Extract flattened playlist info
            "dump_single_json": True,  # Help with debugging
        }
        # Enforce playlist limit at extract time
        if self.limit:
            opts["playlistend"] = self.limit
        with yt_dlp.YoutubeDL(opts) as ydl:
            for url in self.urls:
                print(f"Fetching metadata for {url}...")
                info = ydl.extract_info(url, download=False)
                if not info:
                    print(f"No info found for {url}")
                    continue

                # Debug information
                print(f"Info type: {type(info)}")
                if isinstance(info, dict):
                    print(f"Keys: {list(info.keys())}")
                    if "entries" in info:
                        print(f"Found {len(info['entries'])} entries")
                    else:
                        print("No entries found in result")

                if "entries" in info:
                    entries = info.get("entries", []) or []
                    # Filter out None entries and ensure each has an ID
                    entries = [
                        e for e in entries if e and isinstance(e, dict) and e.get("id")
                    ]
                    print(f"Filtered to {len(entries)} valid entries with IDs")
                    if self.limit:
                        entries = entries[: self.limit]
                        print(
                            f"Limited to {len(entries)} entries due to limit={self.limit}"
                        )
                    info_entries.extend(entries)
                else:
                    # Only add as single entry if it has an ID (likely a video, not a channel)
                    if isinstance(info, dict) and info.get("id"):
                        print(f"Adding single entry with ID: {info.get('id')}")
                    info_entries.append(info)

        print(f"Final result: {len(info_entries)} entries to return")
        return info_entries

    def get_channel_videos(self):
        """
        Special method to extract video IDs from YouTube channels/playlists.
        Designed to handle YouTube channel URLs that might not work correctly with standard extraction.
        """
        videos = []

        # Options specific for channel/playlist extraction
        opts = {
            "ignoreerrors": True,
            "quiet": True,  # Suppress yt-dlp output
            "extract_flat": True,  # Only extract video info without downloading
            "skip_download": True,
        }

        if self.limit:
            opts["playlistend"] = self.limit

        with yt_dlp.YoutubeDL(opts) as ydl:
            for url in self.urls:
                try:
                    # Get channel/playlist info
                    info = ydl.extract_info(url, download=False)

                    if not info:
                        logger.warning(f"No info could be extracted from {url}")
                        continue

                    # Extract video entries from channel/playlist
                    if "entries" in info and info["entries"]:
                        # Filter for valid entries (must have an ID)
                        valid_entries = []
                        for entry in info["entries"]:
                            if entry and isinstance(entry, dict) and "id" in entry:
                                # Create a simple dict with essential video info
                                video_info = {
                                    "id": entry["id"],
                                    "title": entry.get("title", f"Video {entry['id']}"),
                                    "uploader": entry.get("uploader", "Unknown"),
                                    "url": f"https://youtu.be/{entry['id']}",
                                }
                                valid_entries.append(video_info)

                        # Apply limit if needed
                        if self.limit and len(valid_entries) > self.limit:
                            valid_entries = valid_entries[: self.limit]

                        videos.extend(valid_entries)
                        logger.info(f"Extracted {len(valid_entries)} videos from {url}")
                    else:
                        logger.warning(f"No video entries found in {url}")

                except Exception as e:
                    logger.error(f"Error extracting videos from {url}: {e}")

        return videos


def check_ffmpeg() -> bool:
    """
    Check if ffmpeg is installed and available in PATH.

    Returns:
        bool: True if ffmpeg is available, False otherwise
    """
    return shutil.which("ffmpeg") is not None


async def main() -> int:
    """Entry point of the script."""
    # Check for ffmpeg first (required for merging streams)
    if not check_ffmpeg():
        logger.error(
            "ffmpeg is not found in your PATH. It's required for merging video and audio.\n"
            "Please install ffmpeg:\n"
            "- Windows: https://ffmpeg.org/download.html\n"
            "- macOS: brew install ffmpeg\n"
            "- Linux: apt-get install ffmpeg (or your distro's equivalent)"
        )
        return 1
    parser = argparse.ArgumentParser(
        description="Download videos or channels from YouTube using yt-dlp"
    )
    parser.add_argument(
        "-u",
        "--urls",
        nargs="+",  # Accept one or more URLs
        help="One or more URLs of YouTube videos or channels to download",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("./downloads"),
        help="Directory to save downloaded videos",
    )
    parser.add_argument(
        "-f",
        "--format",
        default="best",
        help="Video format code (default: best). Use 'mp4' for most compatibility",
    )
    parser.add_argument(
        "--force-mp4",
        action="store_true",
        help="Force output to mp4 format (recommended for compatibility)",
    )
    parser.add_argument(
        "-l", "--limit", type=int, help="Maximum number of videos to download"
    )
    parser.add_argument(
        "-a", "--audio-only", action="store_true", help="Extract audio only (mp3)"
    )
    parser.add_argument(
        "-s", "--subtitles", action="store_true", help="Download subtitles if available"
    )
    parser.add_argument(
        "-p",
        "--playlist-items",
        help="Comma-separated list of playlist items to download, e.g., '1,3,5-7'",
    )
    parser.add_argument(
        "-c",
        "--cookies",
        type=Path,
        help="Path to cookies file for authenticated access",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug mode (more verbose output)"
    )
    parser.add_argument(
        "--use-native-progress",
        action="store_true",
        help="Use yt-dlp's native progress display instead of custom progress",
    )

    args = parser.parse_args()

    # Configure logging level
    if args.debug:
        logger.setLevel(logging.DEBUG)
    elif args.verbose:
        logger.setLevel(logging.INFO)

    # Configure progress display
    use_native_progress = args.use_native_progress

    try:
        # Adjust format if force-mp4 is specified
        format_spec = args.format
        if args.audio_only and args.format == "best":
            format_spec = "bestaudio/best"
        elif args.force_mp4 and format_spec == "best":
            format_spec = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
            logger.info("Forcing MP4 output format")

        downloader = VideoDownloader(
            urls=args.urls,  # Pass the list of URLs
            output_dir=args.output_dir,
            format=format_spec,
            limit=args.limit,
            extract_audio=args.audio_only,
            extract_subs=args.subtitles,
            playlist_items=args.playlist_items,
            cookies_file=args.cookies,
            use_native_progress=use_native_progress,
        )

        await downloader.download()
        return 0

    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
        return 130

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
