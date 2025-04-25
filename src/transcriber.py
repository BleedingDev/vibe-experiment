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

        # Initialize DeepGram client if needed
        if self.backend == "deepgram":
            # Initialize DeepGram v3 client (reads API key from DEEPGRAM_API_KEY env var)
            self.dg_client = DeepgramClient()

    def transcribe(
        self, video_path: Path
    ) -> Tuple[List[Tuple[float, float, str]], Dict[str, Any]]:
        """
        Transcribe and analyze the given video file.

        Returns:
            segments (List[Tuple[start_sec, end_sec, text]]): list of transcript chunks
            analysis (dict): keys: summary, topics, key_terms, recommended_chapters
        """
        if self.backend == "offmute":
            try:
                return self._offmute_transcribe(video_path)
            except Exception:
                logger.exception("Offmute backend failed, falling back to DeepGram")
                return self._deepgram_transcribe(video_path)
        else:
            return self._deepgram_transcribe(video_path)

    def _offmute_transcribe(
        self, video_path: Path
    ) -> Tuple[List[Tuple[float, float, str]], Dict[str, Any]]:
        """
        Use `bunx offmute-advanced` CLI to perform transcription and analysis.
        """
        cmd = ["bunx", "offmute-advanced", str(video_path), "-t", "budget", "-sc", "0"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        # Build segments list
        segments: List[Tuple[float, float, str]] = []
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

    def _deepgram_transcribe(
        self, video_path: Path
    ) -> Tuple[List[Tuple[float, float, str]], Dict[str, Any]]:
        """
        Use DeepGram SDK to transcribe audio extracted from the video.
        """
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
