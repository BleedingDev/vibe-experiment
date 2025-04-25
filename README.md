# Graph Memory Transcriber with Graphiti Integration

A Python tool that extracts audio from videos, transcribes using Offmute or Deepgram, and processes transcripts into a Neo4j knowledge graph using Graphiti.

## Requirements

- Python 3.10 or newer
- Windows (tested on Windows with PowerShell)
- Neo4j 5.26 or higher (for Graphiti integration)
- Gemini API key for LLM and embedding capabilities (for Graphiti)
- `DEEPGRAM_API_KEY` environment variable (optional, only if using Deepgram)
- Dependencies installed (see `pyproject.toml`)

## Setup

- Open a PowerShell terminal and install dependencies using `uv`:

```powershell
uv install
```

## Configuration

1. Copy the template environment file:

```powershell
Copy-Item .env.template .env
```

2. Edit the `.env` file with your configuration:

```
# Neo4j connection details (required for Graphiti)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password_here

# Gemini API key for LLM and embedding capabilities (required for Graphiti)
GEMINI_API_KEY=your_gemini_api_key_here

# Optional: Transcription backend (offmute or deepgram)
TRANSCRIBE_BACKEND=offmute
```

3. If using Deepgram as your transcription backend, set the API key:

```powershell
$Env:DEEPGRAM_API_KEY = "<YOUR_DEEPGRAM_API_KEY>"
```

## Usage

All commands are run from the project root using the `main.py` entrypoint. No need to change directories:

```powershell
python .\main.py <command> [options]
```

### Commands

#### prepare
Ingest videos into the pipeline database:
```powershell
python .\main.py prepare [--channel-url <CHANNEL_URL>] [--video-urls <URL1> <URL2> ...] [--local-paths <PATH1> <PATH2> ...] [--limit N]
```
Options:
- `--channel-url`: YouTube channel or playlist URL to batch prepare.
- `--video-urls`: One or more YouTube video URLs.
- `--local-paths`: One or more local video file paths.
- `--limit`: Maximum number of videos to prepare (only with `--channel-url`).

Examples:
```powershell
# Ingest first 5 videos from a playlist
python .\main.py prepare --channel-url https://www.youtube.com/playlist?list=... --limit 5

# Ingest specific YouTube URLs
python .\main.py prepare --video-urls https://youtu.be/abc123 https://youtu.be/def456

# Ingest local MP4 files
python .\main.py prepare --local-paths ..\videos\video1.mp4 ..\videos\video2.mp4
```

#### run
Run the pipeline, optionally specifying which step to execute:

```powershell
# Run the full pipeline (download + transcribe + ingest)
python .\main.py run

# Only download the videos
python .\main.py run download

# Only transcribe already downloaded videos
python .\main.py run transcribe

# Only ingest already transcribed videos into Graphiti knowledge graph
python .\main.py run ingest
```

The pipeline processing happens in these distinct steps:

1. **download**: Downloads videos from YouTube or uses local files
2. **transcribe**: Transcribes the audio and performs analysis
3. **ingest**: Ingests transcription data into Graphiti knowledge graph for Neo4j

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

# Reset a video to retry just the ingestion step
python .\main.py retry <VIDEO_ID> --step ingest
```

#### errors
List all videos that have failed and their error messages:
```powershell
python .\main.py errors
```

## Graphiti Integration

This project integrates with Graphiti to create a Neo4j knowledge graph from transcription data. The integration provides the following capabilities:

1. **Automated Chunking**: Transcription files are automatically chunked into manageable segments for better retrieval.

2. **Rich Knowledge Graph**: The system extracts audio analysis and full transcription from the transcription files and creates structured nodes and relationships in Neo4j.

3. **Vector Search**: Transcription chunks are embedded using Gemini, enabling semantic search capabilities.

### Using the Knowledge Graph

After ingesting transcriptions, you can explore and query the data in Neo4j using the Cypher query language. The graph structure includes:

- Video nodes with metadata
- Transcript chunk nodes with text content
- Analysis nodes with summaries
- Relationships between these nodes

For example, to find all transcript chunks related to a specific topic, you can search through the Neo4j database using Graphiti's search features.

## License

MIT License
