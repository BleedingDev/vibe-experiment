#!/usr/bin/env python3
"""
YouTube Channel Downloader

A script to download all videos from a YouTube channel using yt-dlp.
Uses modern Python features, proper error handling, and async operations.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

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
        limit: int | None = None,
        extract_audio: bool = False,
        extract_subs: bool = False,
        playlist_items: str | None = None,
        cookies_file: Path | None = None,
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

    def _get_ydl_opts(self) -> dict[str, Any]:
        """
        Configure yt-dlp options based on instance settings.

        Returns:
            Dictionary of options for yt-dlp
        """
        ydl_opts = {
            "format": self.format,
            "outtmpl": str(self.output_dir / "%(uploader)s/%(title)s-%(id)s.%(ext)s"),
            "ignoreerrors": True,
            "nooverwrites": True,
            "no_warnings": False,
            "progress_hooks": [self._progress_hook],
            "verbose": True,
            "writethumbnail": True,
            "embedthumbnail": True,
            "writeinfojson": True,
            "writesubtitles": self.extract_subs,
            "writeautomaticsub": self.extract_subs,
            "subtitleslangs": ["en"] if self.extract_subs else [],
            "playlistend": self.limit if self.limit else None,
        }

        if self.playlist_items:
            ydl_opts["playlist_items"] = self.playlist_items

        if self.extract_audio:
            ydl_opts.update({
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "192",
                    },
                    {"key": "EmbedThumbnail"},
                    {"key": "FFmpegMetadata"},
                ]
            })

        if self.cookies_file:
            ydl_opts["cookiefile"] = str(self.cookies_file)

        return ydl_opts

    def _progress_hook(self, d: dict[str, Any]) -> None:
        """
        Progress hook to track download progress.

        Args:
            d: Progress dictionary from yt-dlp
        """
        if d["status"] == "finished":
            logger.info(f"Downloaded: {d['filename']}")
        elif d["status"] == "downloading":
            if "total_bytes" in d and d["total_bytes"]:
                percent = d["downloaded_bytes"] / d["total_bytes"] * 100
                logger.info(
                    f"Downloading: {d.get('filename', 'unknown')} - "
                    f"{percent:.1f}% of {d['total_bytes_estimate'] / 1048576:.1f}MB "
                    f"at {d.get('speed', 0) / 1024:.1f}KB/s - "
                    f"ETA: {d.get('eta', 'N/A')}s"
                )

    async def download(self) -> None:
        """Download all videos from the channel asynchronously."""
        try:
            logger.info(f"Starting download of channel: {self.channel_url}")
            logger.info(f"Output directory: {self.output_dir}")

            # Ensure output directory exists
            self.output_dir.mkdir(parents=True, exist_ok=True)

            # Create a separate thread for yt-dlp since it's blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._run_download)

            logger.info("Download completed successfully!")

        except Exception as e:
            logger.error(f"Error during download: {e}", exc_info=True)
            raise

    def _run_download(self) -> None:
        """Execute the download process with yt-dlp."""
        with yt_dlp.YoutubeDL(self._get_ydl_opts()) as ydl:
            ydl.download([self.channel_url])


async def main() -> int:
    """Entry point of the script."""
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
        help="Video format code (default: best)"
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

    args = parser.parse_args()

    # Configure logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    try:
        downloader = ChannelDownloader(
            channel_url=args.channel_url,
            output_dir=args.output_dir,
            format=args.format,
            limit=args.limit,
            extract_audio=args.audio_only,
            extract_subs=args.subtitles,
            playlist_items=args.playlist_items,
            cookies_file=args.cookies,
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
