#!/usr/bin/env python3
"""
YouTube Channel Downloader

A script to download all videos from a YouTube channel using yt-dlp.
Uses modern Python features, proper error handling, and async operations.
"""

import argparse
import asyncio
import logging
import os
import sys
import shutil
import platform
from pathlib import Path
from typing import List, Optional, Dict, Any, Union

# You'll need to install yt-dlp: pip install yt-dlp
import yt_dlp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("yt-channel-downloader")


class ChannelDownloader:
    """Handle downloading of YouTube channel content."""

    def __init__(
        self,
        channel_url: str,
        output_dir: Path,
        format: str = "best",
        limit: Optional[int] = None,
        extract_audio: bool = False,
        extract_subs: bool = False,
        playlist_items: Optional[str] = None,
        cookies_file: Optional[Path] = None,
        use_native_progress: bool = False,
    ):
        """
        Initialize the downloader with configuration options.

        Args:
            channel_url: URL of the YouTube channel
            output_dir: Directory to save downloaded videos
            format: Video format to download (default 'best')
            limit: Maximum number of videos to download (None = no limit)
            extract_audio: Whether to extract audio
            extract_subs: Whether to extract subtitles
            playlist_items: Comma-separated list of playlist items to download
            cookies_file: Path to cookies file for authenticated access
        """
        self.channel_url = channel_url
        self.output_dir = output_dir
        self.format = format
        self.limit = limit
        self.extract_audio = extract_audio
        self.extract_subs = extract_subs
        self.playlist_items = playlist_items
        self.cookies_file = cookies_file
        self.use_native_progress = use_native_progress

    def _get_ydl_opts(self) -> Dict[str, Any]:
        """
        Configure yt-dlp options based on instance settings.

        Returns:
            Dictionary of options for yt-dlp
        """
        # Base options that apply to all configurations
        ydl_opts = {
            # Use format string that prefers already-merged formats
            "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best" if self.format == "best" else self.format,
            "outtmpl": str(self.output_dir / "%(uploader)s/%(title)s-%(id)s.%(ext)s"),
            "ignoreerrors": True,
            "nooverwrites": True,
            "continue": True,  # Resume partially downloaded files
            "writethumbnail": True,
            "embedthumbnail": True,
            "writeinfojson": True,
            "writesubtitles": self.extract_subs,
            "writeautomaticsub": self.extract_subs,
            "subtitleslangs": ["en"] if self.extract_subs else [],
            "playlistend": self.limit if self.limit else None,
            "merge_output_format": "mp4",  # Force merging into mp4
            "retries": 10,        # Retry up to 10 times
            "fragment_retries": 10,  # Retry fragments up to 10 times
            "skip_unavailable_fragments": True,  # Skip unavailable fragments
            "buffer_size": 16*1024,  # Set buffer size to 16K
        }

        # Choose progress display mode
        if self.use_native_progress:
            # Use yt-dlp's native progress display
            ydl_opts.update({
                "quiet": False,
                "no_warnings": False,
                "verbose": True,
                "noprogress": False,
                # Don't add our custom progress hook
            })
        else:
            # Use our custom progress display
            ydl_opts.update({
                "progress_hooks": [self._progress_hook],
                "no_warnings": False,
                "verbose": False,  # Less verbose to avoid cluttering our output
                "quiet": False,
                "noprogress": False,
                "no_color": True,  # Disable ANSI colors in the console
            })

        # Add post-processor args for optimizing MP4s for streaming
        ydl_opts["postprocessor_args"] = ["-movflags", "faststart"]

        if self.playlist_items:
            ydl_opts["playlist_items"] = self.playlist_items

        # Initialize post-processors list
        postprocessors = []

        # Add video post-processors (always needed)
        postprocessors.extend([
            {"key": "FFmpegVideoConvertor", "preferedformat": "mp4"},
            {"key": "FFmpegMetadata"},
        ])

        # Add thumbnail embedding if supported for the format
        if self.format.find("mp4") != -1 or self.extract_audio:
            postprocessors.append({"key": "EmbedThumbnail"})

        # Add audio extraction post-processor if needed
        if self.extract_audio:
            postprocessors.insert(0, {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            })

        # Update options with post-processors
        ydl_opts["postprocessors"] = postprocessors

        if self.cookies_file:
            ydl_opts["cookiefile"] = str(self.cookies_file)

        return ydl_opts

    def _progress_hook(self, d: Dict[str, Any]) -> None:
        """
        Progress hook to track download progress.

        Args:
            d: Progress dictionary from yt-dlp
        """
        try:
            if d["status"] == "finished":
                logger.info(f"Downloaded: {d.get('filename', 'unknown file')}")
                logger.info("Post-processing video (merging streams)...")
            elif d["status"] == "downloading":
                # Get filename (safely)
                filename = d.get('filename', 'unknown')

                # Handle downloaded bytes (safely)
                downloaded_bytes = d.get("downloaded_bytes")
                if downloaded_bytes is None:
                    # Some downloads don't report bytes
                    logger.info(f"Downloading: {filename} - progress information unavailable")
                    return

                # Get total bytes with fallback (safely)
                total_bytes = d.get("total_bytes")
                if total_bytes is None:
                    total_bytes = d.get("total_bytes_estimate")

                # Format the progress message based on available information
                if total_bytes and total_bytes > 0:  # We have both values for percentage
                    percent = (downloaded_bytes / total_bytes) * 100
                    size_mb = total_bytes / 1048576
                    msg = f"Downloading: {filename} - {percent:.1f}% of {size_mb:.1f}MB"
                else:
                    # Just show downloaded amount if total is unknown
                    size_mb = downloaded_bytes / 1048576
                    msg = f"Downloading: {filename} - {size_mb:.1f}MB downloaded (total size unknown)"

                # Add speed if available
                speed = d.get("speed")
                if speed and speed > 0:
                    msg += f" at {speed / 1024:.1f}KB/s"

                # Add ETA if available
                eta = d.get("eta")
                if eta:
                    msg += f" - ETA: {eta}s"

                logger.info(msg)
            elif d["status"] == "error":
                logger.error(f"Error downloading: {d.get('filename', 'unknown')}: {d.get('error')}")
                # Log full error details for debugging
                if "error" in d:
                    logger.debug(f"Error details: {d['error']}")
        except Exception as e:
            # Catch any exception in the progress hook to prevent download interruption
            logger.error(f"Error in progress tracking (continuing download): {e}")
            # Don't re-raise - we want downloads to continue even if progress reporting fails

    async def download(self) -> None:
        """Download all videos from the channel asynchronously."""
        try:
            logger.info(f"Starting download of channel: {self.channel_url}")
            logger.info(f"Output directory: {self.output_dir}")

            # Ensure output directory exists
            self.output_dir.mkdir(parents=True, exist_ok=True)

            # Check for .part files in the output directory
            part_files = list(self.output_dir.glob("**/*.part"))
            if part_files:
                logger.info(f"Found {len(part_files)} partial downloads that will be resumed")

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
                logger.info("Extracting channel information...")
                # First extract info to get video count
                info_dict = ydl.extract_info(self.channel_url, download=False)

                if info_dict is None:
                    logger.error("Could not extract channel information")
                    return

                # Get channel name and video count
                channel_name = info_dict.get('channel', info_dict.get('uploader', 'Unknown Channel'))

                # Handle both playlist and single video
                if 'entries' in info_dict:
                    videos = list(info_dict['entries'])
                    video_count = len(videos)
                    logger.info(f"Found channel: {channel_name} with {video_count} videos")

                    if self.limit:
                        logger.info(f"Will download up to {self.limit} videos due to specified limit")
                else:
                    logger.info(f"Found single video: {info_dict.get('title', 'Unknown')}")

                # Now actually download
                logger.info("Starting downloads...")
                ydl.download([self.channel_url])
        except yt_dlp.utils.DownloadError as e:
            logger.error(f"Download error: {e}")
            if "ffmpeg" in str(e).lower():
                logger.error("This appears to be an ffmpeg error. Make sure ffmpeg is installed correctly.")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)


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
        description="Download all videos from a YouTube channel using yt-dlp"
    )
    parser.add_argument(
        "channel_url", help="URL of the YouTube channel to download"
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=Path("./downloads"),
        help="Directory to save downloaded videos"
    )
    parser.add_argument(
        "-f", "--format",
        default="best",
        help="Video format code (default: best). Use 'mp4' for most compatibility"
    )
    parser.add_argument(
        "--force-mp4",
        action="store_true",
        help="Force output to mp4 format (recommended for compatibility)"
    )
    parser.add_argument(
        "-l", "--limit",
        type=int,
        help="Maximum number of videos to download"
    )
    parser.add_argument(
        "-a", "--audio-only",
        action="store_true",
        help="Extract audio only (mp3)"
    )
    parser.add_argument(
        "-s", "--subtitles",
        action="store_true",
        help="Download subtitles if available"
    )
    parser.add_argument(
        "-p", "--playlist-items",
        help="Comma-separated list of playlist items to download, e.g., '1,3,5-7'"
    )
    parser.add_argument(
        "-c", "--cookies",
        type=Path,
        help="Path to cookies file for authenticated access"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode (more verbose output)"
    )
    parser.add_argument(
        "--use-native-progress",
        action="store_true",
        help="Use yt-dlp's native progress display instead of custom progress"
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
        if args.force_mp4 and format_spec == "best":
            format_spec = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
            logger.info("Forcing MP4 output format")

        # If user wants native progress, modify yt-dlp options accordingly
        if use_native_progress:
            logger.info("Using yt-dlp's native progress display")
            # We'll modify the ChannelDownloader class to handle this

        downloader = ChannelDownloader(
            channel_url=args.channel_url,
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
