"""
Graphiti Knowledge Graph Manager

This module handles the integration with Graphiti to ingest transcription data into Neo4j.
It creates nodes, relationships, and embeds text for vector search capabilities.
"""

import os
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

# Graphiti imports
from graphiti_core import Graphiti
from graphiti_core.llm_client.gemini_client import GeminiClient, LLMConfig
from graphiti_core.embedder.gemini import GeminiEmbedder, GeminiEmbedderConfig
from graphiti_core.nodes import EpisodeType

# Setup logger
logger = logging.getLogger(__name__)


class GraphitiManager:
    """
    Manages interaction with Graphiti for ingesting transcription data into Neo4j.

    Key functionalities:
    - Initialize connection to Neo4j through Graphiti
    - Chunk and process transcription files
    - Ingest transcription chunks as episodes in the knowledge graph
    - Create relationships between video entities and transcript chunks
    """

    def __init__(self):
        """
        Initialize the GraphitiManager with Neo4j connection details from environment variables
        and configure Gemini for LLM and embedding capabilities.
        """
        # Get Neo4j connection details from environment variables (will be provided by user)
        neo4j_uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
        neo4j_user = os.environ.get("NEO4J_USER", "neo4j")
        neo4j_password = os.environ.get("NEO4J_PASSWORD", "password")

        # Get Gemini API key from environment
        api_key = os.environ.get("GEMINI_API_KEY")

        if not api_key:
            logger.warning(
                "No Gemini API key found. Graphiti functionality will be limited."
            )

        try:
            # Initialize Graphiti with Gemini clients
            self.graphiti = Graphiti(
                neo4j_uri,
                neo4j_user,
                neo4j_password,
                llm_client=GeminiClient(
                    config=LLMConfig(
                        api_key=api_key,
                        model="gemini-2.0-flash",  # Using Gemini's latest model
                    )
                )
                if api_key
                else None,
                embedder=GeminiEmbedder(
                    config=GeminiEmbedderConfig(
                        api_key=api_key, embedding_model="embedding-001"
                    )
                )
                if api_key
                else None,
            )
            logger.info("Graphiti client initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Graphiti client: {str(e)}")
            raise

    async def initialize_schema(self):
        """Initialize the Neo4j database schema required by Graphiti"""
        logger.info("Initializing Neo4j schema for Graphiti...")
        try:
            await self.graphiti.build_indices_and_constraints()
            logger.info("Schema initialized using Graphiti's built-in method")

            logger.info("Neo4j schema initialized manually")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize schema: {str(e)}")
            return False

    async def ingest_transcription(self, transcription_path: Path) -> bool:
        """
        Process a transcription file and ingest it into Graphiti.

        Args:
            transcription_path: Path to the transcription file (_transcription.md)

        Returns:
            bool: True if ingestion was successful, False otherwise
        """
        try:
            # Verify the transcription file exists
            if not Path(transcription_path).exists():
                logger.error(f"Transcription file does not exist: {transcription_path}")
                return False

            logger.info(f"Processing transcription file: {transcription_path}")

            # Extract video ID from filename
            raw_video_id = Path(transcription_path).stem.replace("_transcription", "")

            # Handle path-based IDs (from channel subfolders)
            # Use the parent folder name + video ID if the transcription file is in a subfolder
            parent_folder = Path(transcription_path).parent.name
            downloads_folder = Path(transcription_path).parent.parent.name

            if (
                parent_folder
                and parent_folder != "downloads"
                and downloads_folder == "downloads"
            ):
                # This is likely a channel ID folder, use it as part of the ID
                video_id = f"{parent_folder}/{raw_video_id}"
                logger.info(f"Using path-based video ID: {video_id}")
            else:
                video_id = raw_video_id
                logger.info(f"Using direct video ID: {video_id}")

            # Clean video_id for use in Neo4j (remove problematic characters)
            safe_video_id = video_id.replace("/", "_").replace("\\", "_")
            logger.info(f"Using safe video ID for Neo4j: {safe_video_id}")

            # Read the transcription file with robust encoding handling
            content = None
            encodings_to_try = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

            for encoding in encodings_to_try:
                try:
                    with open(transcription_path, "r", encoding=encoding) as f:
                        content = f.read()
                    logger.info(f"Successfully read file with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    logger.warning(
                        f"Failed to read with {encoding} encoding, trying next..."
                    )
                    continue

            if content is None:
                logger.error(
                    f"Could not read file with any encoding: {transcription_path}"
                )
                return False

            if not content.strip():
                logger.error(f"Transcription file is empty: {transcription_path}")
                return False

            # Extract important sections
            audio_analysis, full_transcript = self._extract_transcription_sections(
                content
            )

            # Chunk the transcript for ingestion
            chunks = self._chunk_transcript(full_transcript)

            # Log what we found
            logger.info(f"Found {len(chunks)} transcript chunks to process")
            logger.info(f"Audio analysis length: {len(audio_analysis)} characters")
            logger.info(f"Full transcript length: {len(full_transcript)} characters")

            try:
                # First, add the audio analysis as a summary episode
                await self._add_analysis_episode(video_id, audio_analysis)
            except Exception as e:
                logger.error(f"Failed to add analysis episode: {str(e)}")
                # Continue with transcript chunks even if analysis fails
                logger.warning(
                    "Continuing with transcript chunks despite analysis error"
                )

            # Keep track of successful chunks
            success_count = 0
            total_chunks = len(chunks)

            # Process transcript chunks with batch error handling
            for i, chunk in enumerate(chunks):
                try:
                    logger.info(f"Processing chunk {i + 1}/{total_chunks}")
                    await self._add_transcript_chunk_episode(video_id, chunk, i)
                    success_count += 1
                    # Add a small delay between chunks to avoid overwhelming Neo4j
                    # Only delay if there are many chunks
                    if total_chunks > 10:
                        from asyncio import sleep

                        await sleep(0.1)  # 100ms delay
                except Exception as e:
                    logger.error(f"Error processing chunk {i}: {str(e)}")
                    # Continue with next chunk
                    continue

            # Consider it a success if either:
            # 1. We processed at least one chunk, or
            # 2. We successfully added the analysis episode
            if success_count > 0:
                logger.info(
                    f"Successfully ingested {success_count}/{total_chunks} transcript chunks for video {video_id}"
                )
                return True
            elif not chunks and audio_analysis:
                logger.info(
                    f"No transcript chunks but successfully added analysis for video {video_id}"
                )
                return True
            else:
                logger.error(f"Failed to ingest any content for video {video_id}")
                return False

        except Exception as e:
            logger.error(f"Failed to ingest transcription: {str(e)}")
            logger.error("Stack trace: ", exc_info=True)
            return False

    def _extract_transcription_sections(self, content: str) -> Tuple[str, str]:
        """
        Extract the Audio Analysis and Full Transcription sections from the transcription file.

        Args:
            content: The full content of the transcription file

        Returns:
            Tuple containing (audio_analysis, full_transcript)
        """
        # Log the content structure for debugging
        content_preview = content[:100].replace("\n", " ")
        logger.info(f"Content preview: '{content_preview}...'")
        logger.info(f"Content length: {len(content)}")

        # Find main sections using both Markdown and plain text headers
        # Audio Analysis section
        audio_analysis_start = -1
        for header in ["# Audio Analysis", "Audio Analysis:", "AUDIO ANALYSIS"]:
            pos = content.find(header)
            if pos != -1:
                audio_analysis_start = pos
                logger.info(
                    f"Found Audio Analysis section at position {pos} with header '{header}'"
                )
                break

        # Full Transcription section
        full_transcript_start = -1
        for header in [
            "# Full Transcription",
            "Full Transcription:",
            "TRANSCRIPT",
            "# Transcript",
        ]:
            pos = content.find(header)
            if pos != -1:
                full_transcript_start = pos
                logger.info(
                    f"Found Full Transcription section at position {pos} with header '{header}'"
                )
                break

        # If Audio Analysis exists, extract it
        audio_analysis = ""
        if audio_analysis_start != -1:
            # If Full Transcription exists and comes after the Audio Analysis, use it as boundary
            if (
                full_transcript_start != -1
                and full_transcript_start > audio_analysis_start
            ):
                audio_analysis = content[
                    audio_analysis_start:full_transcript_start
                ].strip()
                logger.info(
                    f"Extracted Audio Analysis from positions {audio_analysis_start} to {full_transcript_start}"
                )
            else:
                # Try to find the next section header after Audio Analysis
                next_header_pos = content.find("#", audio_analysis_start + 1)

                if next_header_pos != -1:
                    # Found another header, use it as boundary
                    audio_analysis = content[
                        audio_analysis_start:next_header_pos
                    ].strip()
                    logger.info(
                        f"Extracted Audio Analysis up to next header at position {next_header_pos}"
                    )
                else:
                    # No other headers found, use everything from Audio Analysis to the end
                    audio_analysis = content[audio_analysis_start:].strip()
                    logger.info(
                        "Extracted Audio Analysis to end of content (no other headers found)"
                    )

        # Extract Full Transcription if it exists
        full_transcript = ""
        if full_transcript_start != -1:
            full_transcript = content[full_transcript_start:].strip()
            logger.info(
                f"Extracted Full Transcription from position {full_transcript_start} to end"
            )

        # If no sections were found, use the entire content as transcript
        if not audio_analysis and not full_transcript:
            logger.warning(
                "No standard sections found in transcription file, using entire content"
            )
            full_transcript = content.strip()

        return audio_analysis, full_transcript

    def _chunk_transcript(self, transcript: str, chunk_size: int = 1000) -> List[str]:
        """
        Break the transcript into manageable chunks for ingestion.

        Args:
            transcript: The full transcript text
            chunk_size: The maximum size of each chunk in characters

        Returns:
            List of transcript chunks
        """
        # Return empty list for empty transcript
        if not transcript or not transcript.strip():
            logger.warning("Empty transcript received, returning empty chunks list")
            return []

        # Remove headers and clean up the transcript
        cleaned_transcript = transcript

        # Remove common headers
        headers_to_remove = ["# Full Transcription", "# Transcript", "# Content"]
        for header in headers_to_remove:
            cleaned_transcript = cleaned_transcript.replace(header, "").strip()

        # Check for different transcript formats
        if "~" in cleaned_transcript:
            # Format with speaker indicators (~Speaker~)
            parts = cleaned_transcript.split("~")
        else:
            # Try other common delimiters for chunking
            # First check if there are timestamps in [00:00:00] format
            import re

            timestamp_matches = re.findall(r"\[\d{2}:\d{2}:\d{2}\]", cleaned_transcript)

            if timestamp_matches:
                # Use timestamps as chunk boundaries
                parts = re.split(r"(\[\d{2}:\d{2}:\d{2}\])", cleaned_transcript)
            else:
                # Fall back to paragraph-based chunking
                parts = cleaned_transcript.split("\n\n")

        # Combine parts into chunks
        chunks = []
        current_chunk = ""

        # Avoid empty parts
        parts = [p for p in parts if p.strip()]

        for i in range(0, len(parts)):
            part = parts[i]
            if not part.strip():
                continue

            # If adding this part would exceed chunk size, save current chunk and start a new one
            if len(current_chunk) + len(part) > chunk_size and current_chunk:
                chunks.append(current_chunk.strip())
                current_chunk = part
            else:
                # Otherwise, add to current chunk
                if "~" in cleaned_transcript:
                    # For speaker format, preserve the ~ character
                    current_chunk += "~" + part if current_chunk else part
                else:
                    # For other formats, add a space
                    current_chunk += " " + part if current_chunk else part

        # Add the last chunk if there is any content
        if current_chunk:
            chunks.append(current_chunk.strip())

        # If we ended up with no chunks, create at least one from the cleaned transcript
        if not chunks and cleaned_transcript.strip():
            # Fallback: just split by character count if all else fails
            chunks = [
                cleaned_transcript[i : i + chunk_size].strip()
                for i in range(0, len(cleaned_transcript), chunk_size)
            ]

        logger.info(f"Created {len(chunks)} transcript chunks")
        return chunks

    async def _add_analysis_episode(self, video_id: str, analysis_text: str) -> None:
        """
        Add the audio analysis as a summary episode in Graphiti.

        Args:
            video_id: The ID of the video
            analysis_text: The audio analysis text
        """
        # Ensure we have actual content to add
        if not analysis_text:
            logger.warning(f"No audio analysis found for video {video_id}")
            return

        try:
            # Clean up the analysis text
            # Remove all known headers that might be present
            headers = ["# Audio Analysis", "Audio Analysis:", "AUDIO ANALYSIS"]

            clean_text = analysis_text
            for header in headers:
                clean_text = clean_text.replace(header, "").strip()

            # Ensure we still have content after cleaning
            if not clean_text or len(clean_text) < 10:
                logger.warning(
                    f"No meaningful content in audio analysis for video {video_id} after cleaning"
                )
                # Use a placeholder summary if the analysis is empty or too short
                clean_text = f"Transcription summary for video {video_id} (no analysis available)"

            # Clean any problematic characters from the text
            # Replace smart quotes and other problematic characters
            replacements = {
                '"': '"',  # closing double quote
                """: "'",  # opening single quote
                """: "'",  # closing single quote
                "–": "-",  # en dash
                "—": "-",  # em dash
            }
            for old, new in replacements.items():
                clean_text = clean_text.replace(old, new)

            # Limit the length if needed (Neo4j can have issues with very long texts)
            if len(clean_text) > 10000:
                logger.warning(
                    f"Audio analysis for video {video_id} is very long, truncating"
                )
                clean_text = clean_text[:10000] + "... (truncated)"

            # Make video_id safe for Neo4j (remove slashes)
            safe_video_id = video_id.replace("/", "_").replace("\\", "_")

            # Add the episode
            try:
                await self.graphiti.add_episode(
                    name=f"Audio_Analysis_{safe_video_id}",
                    episode_body=clean_text,
                    source=EpisodeType.summary,
                    reference_time=datetime.now(timezone.utc),
                    source_description=f"Audio analysis for video {video_id}",
                )
                logger.info(f"Added audio analysis episode for video {video_id}")
            except Exception as e:
                logger.error(f"Error during graphiti.add_episode call: {str(e)}")
                # Try one more time with minimal content
                logger.info("Attempting to add episode with minimal content")
                minimal_text = f"Summary for video {video_id}"
                await self.graphiti.add_episode(
                    name=f"Audio_Analysis_{safe_video_id}",
                    episode_body=minimal_text,
                    source=EpisodeType.summary,
                    reference_time=datetime.now(timezone.utc),
                    source_description=f"Audio analysis for video {video_id}",
                )
                logger.info("Successfully added episode with minimal content")
        except Exception as e:
            logger.error(f"Failed to add audio analysis episode: {str(e)}")
            # Log more details about the error for debugging
            logger.error(f"Analysis text preview: {analysis_text[:100]}...")
            logger.error(f"Analysis text length: {len(analysis_text)}")
            # Continue without raising so other chunks can be processed
            # This prevents one error from stopping the entire ingestion
            logger.warning(
                "Continuing with transcript chunks despite audio analysis error"
            )

    async def _add_transcript_chunk_episode(
        self, video_id: str, chunk: str, chunk_index: int
    ) -> None:
        """
        Add a transcript chunk as an episode in Graphiti.

        Args:
            video_id: The ID of the video
            chunk: The transcript chunk text
            chunk_index: The index of this chunk
        """
        try:
            # Skip empty chunks
            if (
                not chunk or len(chunk.strip()) < 10
            ):  # Require at least 10 chars of content
                logger.warning(
                    f"Skipping empty or very small chunk {chunk_index} for video {video_id}"
                )
                return

            # Clean up the chunk
            clean_chunk = chunk.strip()

            # Clean any problematic characters from the text
            replacements = {
                "\u201c": '"',  # left double quotation mark
                "\u201d": '"',  # right double quotation mark
                "\u2018": "'",  # left single quotation mark
                "\u2019": "'",  # right single quotation mark
                "\u2013": "-",  # en dash
                "\u2014": "-",  # em dash
            }
            for old, new in replacements.items():
                clean_chunk = clean_chunk.replace(old, new)

            # Ensure the chunk isn't too long for Neo4j
            if len(clean_chunk) > 10000:
                logger.warning(
                    f"Transcript chunk {chunk_index} for video {video_id} is very long, truncating"
                )
                clean_chunk = clean_chunk[:10000] + "... (truncated)"

            # Create safe_video_id by removing potential problematic characters
            # (for path-based IDs which may contain slashes)
            safe_video_id = video_id.replace("/", "_").replace("\\", "_")

            # Add the episode
            try:
                await self.graphiti.add_episode(
                    name=f"Transcript_{safe_video_id}_chunk_{chunk_index}",
                    episode_body=clean_chunk,
                    source=EpisodeType.message,  # Treat as conversation message
                    reference_time=datetime.now(timezone.utc),
                    source_description=f"Transcript chunk {chunk_index} for video {video_id}",
                )
                logger.info(
                    f"Added transcript chunk {chunk_index} for video {video_id}"
                )
            except Exception as e:
                logger.error(f"Error adding chunk {chunk_index}: {str(e)}")
                # If the chunk is still problematic, try with minimal content
                try:
                    minimal_chunk = (
                        f"Transcript chunk {chunk_index} for video {video_id}"
                    )
                    await self.graphiti.add_episode(
                        name=f"Transcript_{safe_video_id}_chunk_{chunk_index}",
                        episode_body=minimal_chunk,
                        source=EpisodeType.message,
                        reference_time=datetime.now(timezone.utc),
                        source_description=f"Transcript chunk {chunk_index} for video {video_id} (error recovery)",
                    )
                    logger.info(
                        f"Added minimal chunk {chunk_index} after error recovery"
                    )
                except Exception as e2:
                    logger.error(f"Even minimal chunk failed: {str(e2)}")
                    # Skip this chunk and continue
        except Exception as e:
            logger.error(f"Failed to add transcript chunk episode: {str(e)}")
            # Log more details about the error for debugging
            logger.error(f"Chunk preview: {chunk[:100]}...")
            logger.error(f"Chunk length: {len(chunk)}")
            # Don't raise the exception, just log and continue with other chunks

    async def search_graph(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Search the knowledge graph for relevant information.

        Args:
            query: The search query
            limit: Maximum number of results to return

        Returns:
            List of search results
        """
        try:
            results = await self.graphiti.search_edges(
                query=query,
                edge_type=None,  # Search all edge types
                limit=limit,
            )
            return results
        except Exception as e:
            logger.error(f"Failed to search knowledge graph: {str(e)}")
            return []
