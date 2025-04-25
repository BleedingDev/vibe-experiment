# Graph Memory Transcriber

A Python tool that extracts audio from videos, transcribes using Deepgram SDK v3, and processes transcripts into a memory graph.

## Requirements

- Python 3.10 or newer
- Windows (tested on Windows with PowerShell)
- `DEEPGRAM_API_KEY` environment variable set
- Dependencies installed (see `pyproject.toml`)

## Setup

- Open a PowerShell terminal and install dependencies using `uv`:

```powershell
uv install
```

## Configuration

- The Deepgram backend is optional. Only set the API key if using Deepgram:

```powershell
$Env:DEEPGRAM_API_KEY = "<YOUR_DEEPGRAM_API_KEY>"
```

## Usage

All commands are run from the project root using the `main.py` entrypoint. No need to change directories:

```powershell
python .\main.py <command> [options]
```

### Commands

#### ingest
Ingest videos into the pipeline database:
```powershell
python .\main.py ingest [--channel-url <CHANNEL_URL>] [--video-urls <URL1> <URL2> ...] [--local-paths <PATH1> <PATH2> ...] [--limit N]
```
Options:
- `--channel-url`: YouTube channel or playlist URL to batch ingest.
- `--video-urls`: One or more YouTube video URLs.
- `--local-paths`: One or more local video file paths.
- `--limit`: Maximum number of videos to ingest (only with `--channel-url`).

Examples:
```powershell
# Ingest first 5 videos from a playlist
python .\main.py ingest --channel-url https://www.youtube.com/playlist?list=... --limit 5

# Ingest specific YouTube URLs
python .\main.py ingest --video-urls https://youtu.be/abc123 https://youtu.be/def456

# Ingest local MP4 files
python .\main.py ingest --local-paths ..\videos\video1.mp4 ..\videos\video2.mp4
```

#### run
Run the pipeline, optionally specifying which step to execute:

```powershell
# Run the full pipeline (download + transcribe)
python .\main.py run

# Only download the videos
python .\main.py run download

# Only transcribe already downloaded videos
python .\main.py run transcribe
```

The pipeline processing happens in these distinct steps:

1. **download**: Downloads videos from YouTube or uses local files
2. **transcribe**: Transcribes the audio and performs analysis

When running a specific step, videos will be marked with intermediate statuses to allow for step-by-step processing.

#### status
Show current pipeline counts and recent failures:
```powershell
python .\main.py status
```

#### retry
Reset a failed video for reprocessing, optionally specifying which step to retry:
```powershell
# Reset a video to retry the full pipeline
python .\main.py retry <VIDEO_ID>

# Reset a video to retry just the download step
python .\main.py retry <VIDEO_ID> --step download

# Reset a video to retry just the transcription step
python .\main.py retry <VIDEO_ID> --step transcribe
```

#### errors
List all videos that have failed and their error messages:
```powershell
python .\main.py errors
```

## License

MIT License
