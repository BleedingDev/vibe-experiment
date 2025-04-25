# Product Requirements Document (PRD)
**Project Codename:** *Channel Insight*
**Author:** *(your name)*
**Revision Date:** 25 Apr 2025 (hackathon‑day)

---
## 1. Purpose & Vision
Unlock the knowledge buried inside YouTube videos by making every second of content instantly searchable and answerable through natural‑language chat.  The hackathon MVP delivers a single‑machine pipeline that downloads videos, extracts subtitles, enriches them with multimodal analysis, indexes everything in a graph database, and exposes an API for search and chat that deep‑links back to the exact moment in the source video.

*Scope is deliberately narrow:* **YouTube only**, no live‑streams, no proprietary auth, no cloud infra.  The result should prove that long‑form video can be treated like structured, queryable data.

---
## 2. Guiding Principles
1. **Source‑centric** – every answer must cite the original video ID and exact timestamp.
2. **All‑local & portable** – runs end‑to‑end on one laptop using SQLite + local Neo4j.
3. **Modular steps** – each stage (download → subtitle → analysis → index) is a replaceable unit.
4. **Fail‑safe** – every video has a persisted status so the pipeline restarts cleanly.
5. **Speed over polish** – hackathon constraints trump production niceties (TOS, scaling infra, UI extras).

---
## 3. User Personas  _(for context only – **no auth/role logic implemented**)_
| Persona | Needs |
|---------|-------|
| **Explorer** (viewer/hacker) | Ask a question, receive an answer with a timestamp link, click to play it. |
| **Developer** (us) | Run pipeline, observe progress, tweak parallelism and retry failures. |

---
## 4. High‑Level Capabilities
| ID | Capability |
|----|------------|
| C1 | Accept a **channel URL** (with optional *limit*) **or** list of video URLs **or** local video files. |
| C2 | Download videos + subtitles via *yt‑dlp* (YouTube only). |
| C3 | Generate transcript + deep analysis (summary, topics, etc.) with **Gemini** in a single call. |
| C4 | Persist raw subtitles & analysis in **SQLite**; ingest into **Graphiti/Neo4j** for vector search. |
| C5 | Expose **Search API** returning ranked subtitle chunks with video_id & timestamp. |
| C6 | Expose **Chat API** (RAG) that calls Search API, feeds chunks to an LLM, and emits an answer with citation links. |
| C7 | Provide pipeline **status tracking** per video with retry (states: *todo, downloading, transcribing, analysing, ingesting, done, failed*). |
| C8 | Allow configurable **parallelism** knobs to balance speed vs. rate‑limits. |

---
## 5. Functional Requirements
### 5.1 Ingestion Interface
* **R1**   `POST /ingest` payload ⟶ `{channel_url | video_urls[] | local_paths[] , max_videos?}`.
* **R2**   Returns a batch‑id; videos enter *todo* state in SQLite table `videos`.

### 5.2 Download & Subtitle Extraction
* **R3**   Worker pulls *todo* videos, downloads with *yt‑dlp* → `.mp4` + `.vtt`/`.srt`.
* **R4**   Failure marks state *failed* and records exception; retried up to *n* times.

### 5.3 Transcription & Analysis (Gemini)
* **R5**   For each video file (or subtitle file), send to Gemini *offmute* API once → returns:
  * full subtitle (if missing)
  * `{ summary, topics[], key_terms[], recommended_chapters[] }`
* **R6**   Store the subtitle text and analysis JSON in SQLite, linked by `video_id`.

### 5.4 Indexing
* **R7**   Chunk subtitles (~30 s or sentence window), embed, and upsert into Graphiti which auto‑creates relationships (`VIDEO→CHUNK`).
* **R8**   Maintain mapping table `chunk(video_id, start_sec, end_sec, text, embedding)` in SQLite for quick re‑index.

### 5.5 Search API
* **R9**   `GET /search?q=&k=10` ⟶ JSON list of `{text, video_id, start_sec, end_sec, score}` sorted by similarity.

### 5.6 Chat API (RAG)
* **R10**  `POST /chat` with `{question}` → pipeline:
  1. Call `/search` (top‑k).
  2. Inject chunks into LLM prompt.
  3. Stream answer with markdown citation links `[Video‑Title @12:34](https://youtu.be/<id>?t=754)`.

### 5.7 Pipeline Orchestration
* **R11**  Use **Temporal** (preferred) _or_ lightweight SQLite‑based queue to persist state, retries and back‑off across process restarts.
* **R12**  Parallelism config: `MAX_DOWNLOAD_WORKERS`, `MAX_GEMINI_CALLS`, etc. exposed via `.env`.

---
## 6. Data Storage Schema (SQLite)
```text
videos(
  id TEXT PRIMARY KEY,                -- YouTube ID or UUID for local
  title TEXT,
  source TEXT CHECK(source IN ('youtube','local')),
  status TEXT,                        -- todo|downloading|...|failed|done
  retries INTEGER DEFAULT 0,
  duration_sec INTEGER,
  filepath TEXT,                      -- local mp4 path
  subtitle_path TEXT,                 -- .vtt/.srt path
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
subtitles(
  video_id TEXT REFERENCES videos(id),
  start_sec INTEGER,
  end_sec INTEGER,
  text TEXT,
  PRIMARY KEY(video_id,start_sec)
)
analysis(
  video_id TEXT PRIMARY KEY,
  summary TEXT,
  topics TEXT,            -- JSON array
  key_terms TEXT,         -- JSON array
  chapters TEXT           -- JSON array {title,start_sec}
)
```
Graphiti automatically reflects `VIDEO`, `CHUNK`, `TOPIC` nodes and their edges.

---
## 7. Non‑Functional Requirements
| Aspect | Requirement |
|--------|-------------|
| **Performance** | Search ≤ 2 s, chat first token ≤ 4 s on 100‑video set. |
| **Scalability** | Configurable parallelism; target 20 concurrent downloads & 10 concurrent Gemini calls on laptop. |
| **Resilience** | No data loss on crash; video states resume correctly; max 3 automatic retries. |
| **Portability** | Only dependencies: Python ≥3.10, Node for Graphiti, SQLite, Neo4j local, `yt‑dlp`. |
| **Observability** | CLI command `status` lists counts per pipeline state + recent errors. |

---
## 8. Acceptance Criteria (Hackathon Demo)
1. User asks a question through Chat API → answer arrives with at least one timestamp link.
2. Clicking link opens YouTube player at cited second ±2 s.
3. `/search?q=` endpoint returns JSON with correct structure and scores.
4. `status` CLI shows each video’s latest state; failed videos can be re‑queued via `retry <video_id>`.

---
## 9. Risks & Mitigations
| Risk | Impact | Mitigation |
|------|--------|-----------|
| YouTube rate‑limits downloads | Pipeline stalls | Respect‑retry headers; tune `--sleep-interval`; parallelism knob |
| Gemini quota exhausted | Analysis blocked | Cache subtitles; fall back to transcript‑only mode |
| Large subtitle files overwhelm memory | Slow embedding | Stream chunking + batch embeds |
| Laptop crash | Lost progress | All state in SQLite + idempotent workers |

---
## 10. Out of Scope (MVP)
* User authentication & permissions.
* Content sentiment/emotion analysis.
* Cloud deployment and dashboards.
* Non‑YouTube sources, live‑stream ingestion, paywalls.

---
## 11. Future Directions _(post‑hackathon, not part of MVP)_
* Additional content types (PDF, slides) → same graph index.
* Analytics API for cross‑video insights.
* Scalable cloud micro‑services, UI front‑end, etc.

---
### Ready for feedback
Let me know what still feels off or any section needing deeper detail before we freeze this PRD.
