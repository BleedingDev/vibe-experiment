import os
import subprocess
import json
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

from dotenv import load_dotenv
from deepgram import DeepgramClient, FileSource, PrerecordedOptions

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class Transcriber:
    """
    Transcriber handles transcription and analysis of video files using configurable backends.
    Supported backends: 'offmute' (default) and 'deepgram'.
    Returns a list of transcript segments with start/end times.
    """

    def __init__(self, backend: Optional[str] = None):
        # Decide backend from parameter or environment variable
        self.backend = (backend or os.getenv("TRANSCRIBE_BACKEND", "offmute")).lower()
        if self.backend not in ("offmute", "deepgram"):
            raise ValueError(f"Unsupported transcription backend: {self.backend}")

        # Always initialize DeepGram client for fallback scenarios
        try:
            self.dg_client = DeepgramClient()
        except Exception:
            # If DeepGram client fails to initialize, log it but continue
            # This allows offmute to work without DeepGram
            logger.warning(
                "Failed to initialize DeepGram client. DeepGram fallback won't be available."
            )
            self.dg_client = None

    def transcribe(
        self, video_path: Path
    ) -> Tuple[List[Tuple[float, float, str]], Dict[str, Any]]:
        """
        Transcribe and analyze the given video file.

        Returns:
            segments (List[Tuple[start_sec, end_sec, text]]): list of transcript chunks
            analysis (dict): keys: summary, topics, key_terms, recommended_chapters
        """
        # Check for existing transcription file first
        transcription_path = video_path.with_name(f"{video_path.stem}_transcription.md")
        if transcription_path.exists():
            logger.info(f"Found existing transcription file at {transcription_path}")
            return self._parse_transcription_file(transcription_path)

        # No existing transcription file, proceed with transcription
        if self.backend == "offmute":
            try:
                return self._offmute_transcribe(video_path)
            except Exception:
                logger.exception(
                    "Offmute backend failed, fallback to DeepGram if available"
                )
                if self.dg_client is None:
                    # Can't fall back, must raise the original exception
                    raise
                return self._deepgram_transcribe(video_path)
        else:
            # DeepGram was explicitly selected as the backend
            if self.dg_client is None:
                raise ValueError(
                    "DeepGram client is not initialized. Make sure DEEPGRAM_API_KEY is set."
                )
            return self._deepgram_transcribe(video_path)

    def _offmute_transcribe(
        self, video_path: Path
    ) -> Tuple[List[Tuple[float, float, str]], Dict[str, Any]]:
        """
        Use `bunx offmute-advanced` CLI to perform transcription and analysis.
        """
        # Check if transcription file already exists
        transcription_path = video_path.with_name(f"{video_path.stem}_transcription.md")
        if transcription_path.exists():
            logger.info(f"Found existing transcription file at {transcription_path}")
            return self._parse_transcription_file(transcription_path)

        cmd = ["bunx", "offmute-advanced", str(video_path), "-t", "budget", "-sc", "0"]

        try:
            # Run with UTF-8 encoding explicitly
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=False,  # Don't raise exception, we'll check return code
            )

            # Check if the command was successful
            if result.returncode != 0:
                logger.error(f"offmute command failed with code {result.returncode}")
                logger.error(f"stderr: {result.stderr}")
                raise RuntimeError(
                    f"offmute command failed with code {result.returncode}"
                )

            # Check if we have output
            if not result.stdout:
                logger.error("offmute command returned empty output")

                # Check if transcription file was generated despite JSON output failure
                if transcription_path.exists():
                    logger.info(
                        f"Despite JSON output failure, found transcription file at {transcription_path}"
                    )
                    return self._parse_transcription_file(transcription_path)

                raise RuntimeError("offmute command returned empty output")

            # Try to parse JSON output
            data = json.loads(result.stdout)

            # Build segments list
            segments = []
            if "segments" in data:
                for seg in data["segments"]:
                    segments.append(
                        (
                            float(seg.get("start", 0)),
                            float(seg.get("end", 0)),
                            seg.get("text", "").strip(),
                        )
                    )
            else:
                transcript = data.get("transcript", "")
                segments.append((0.0, 0.0, transcript))

            # Extract analysis fields
            analysis = {
                "summary": data.get("summary", ""),
                "topics": data.get("topics", []),
                "key_terms": data.get("key_terms", []),
                "recommended_chapters": data.get("recommended_chapters", []),
            }
            return segments, analysis

        except Exception as e:
            logger.exception(f"Error in offmute transcription: {str(e)}")

            # Check if transcription file was generated despite the error
            if transcription_path.exists():
                logger.info(
                    f"Despite error, found transcription file at {transcription_path}"
                )
                return self._parse_transcription_file(transcription_path)

            # Re-raise if no transcription file was found
            raise

    def _deepgram_transcribe(
        self, video_path: Path
    ) -> Tuple[List[Tuple[float, float, str]], Dict[str, Any]]:
        """
        Use DeepGram SDK to transcribe audio extracted from the video.
        """
        # Check if DeepGram client is available
        if self.dg_client is None:
            logger.error(
                "DeepGram client is not available. Make sure DEEPGRAM_API_KEY is set."
            )
            # Return empty result as fallback
            return [
                (0.0, 0.0, "Transcription failed - DeepGram client not available")
            ], {
                "summary": "Transcription failed - DeepGram client not available",
                "topics": [],
                "key_terms": [],
                "recommended_chapters": [],
            }

        # Check if transcription file already exists
        transcription_path = video_path.with_name(f"{video_path.stem}_transcription.md")
        if transcription_path.exists():
            logger.info(f"Found existing transcription file at {transcription_path}")
            return self._parse_transcription_file(transcription_path)

        # Extract audio to WAV for DeepGram
        audio_path = video_path.with_suffix(".wav")
        ffmpeg_cmd = [
            "ffmpeg",
            "-y",  # overwrite
            "-i",
            str(video_path),
            "-ar",
            "16000",
            "-ac",
            "1",
            str(audio_path),
        ]
        subprocess.run(ffmpeg_cmd, capture_output=True, check=True)

        # Prepare and send DeepGram v3 request (sync)
        with audio_path.open("rb") as af:
            buffer_data = af.read()

        payload: FileSource = {"buffer": buffer_data, "mimetype": "audio/wav"}
        options = PrerecordedOptions(punctuate=True)
        # Call transcribe_file on v3 listen.rest API (v1)
        file_response = self.dg_client.listen.rest.v("1").transcribe_file(
            payload, options
        )
        # Convert to JSON dict
        data = json.loads(file_response.to_json())

        # Parse DeepGram response
        transcript = data["results"]["channels"][0]["alternatives"][0]["transcript"]
        segments: List[Tuple[float, float, str]] = [(0.0, 0.0, transcript)]
        analysis = {
            "summary": "",
            "topics": [],
            "key_terms": [],
            "recommended_chapters": [],
        }
        return segments, analysis

    def _parse_transcription_file(
        self, transcription_path: Path
    ) -> Tuple[List[Tuple[float, float, str]], Dict[str, Any]]:
        """
        Parse an existing transcription file in the _transcription.md format.

        Args:
            transcription_path: Path to the transcription file

        Returns:
            segments: List of transcript segments with start/end times
            analysis: Analysis data from the transcription
        """
        logger.info(f"Parsing transcription file: {transcription_path}")

        try:
            with open(transcription_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Extract Audio Analysis section
            audio_analysis_start = content.find("# Audio Analysis")
            audio_analysis_end = (
                content.find("#", audio_analysis_start + 1)
                if audio_analysis_start != -1
                else -1
            )

            # Extract Full Transcription section
            full_transcript_start = content.find("# Full Transcription")

            # Handle audio analysis
            audio_analysis = ""
            if audio_analysis_start != -1 and audio_analysis_end != -1:
                audio_analysis = content[
                    audio_analysis_start:audio_analysis_end
                ].strip()

            # Handle full transcript
            transcript = ""
            if full_transcript_start != -1:
                transcript = content[full_transcript_start:].strip()

            # Create a single segment with the full transcript
            segments = [(0.0, 0.0, transcript)]

            # Create analysis dictionary
            analysis = {
                "summary": audio_analysis,
                "topics": [],  # We don't have structured topics in the MD file
                "key_terms": [],
                "recommended_chapters": [],
            }

            return segments, analysis

        except Exception as e:
            logger.error(f"Failed to parse transcription file: {str(e)}")
            # Return empty data
            return [(0.0, 0.0, "")], {
                "summary": "",
                "topics": [],
                "key_terms": [],
                "recommended_chapters": [],
            }
