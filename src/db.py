"""
SQLite database wrapper for persisting video ingestion pipeline state, subtitles, and analysis.
"""

import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path(__file__).parent.parent / "graph_memory.db"


class Database:
    def __init__(self, db_path: Path = DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS videos(
            id TEXT PRIMARY KEY,
            title TEXT,
            source TEXT CHECK(source IN ('youtube','local')),
            status TEXT,
            retries INTEGER DEFAULT 0,
            duration_sec INTEGER,
            filepath TEXT,
            subtitle_path TEXT,
            created_at TEXT,
            updated_at TEXT,
            error TEXT
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS subtitles(
            video_id TEXT REFERENCES videos(id),
            start_sec INTEGER,
            end_sec INTEGER,
            text TEXT,
            PRIMARY KEY(video_id,start_sec)
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS analysis(
            video_id TEXT PRIMARY KEY REFERENCES videos(id),
            summary TEXT,
            topics TEXT,
            key_terms TEXT,
            chapters TEXT
        )""")
        self.conn.commit()

    def add_video(
        self,
        id: str,
        title: str,
        source: str,
        filepath: str = None,
        duration_sec: int = None,
    ):
        now = datetime.utcnow().isoformat()
        cur = self.conn.cursor()
        # Use UPSERT: Insert new videos, or update existing ones to reset status to 'todo'
        cur.execute(
            """
            INSERT INTO videos(id, title, source, status, filepath, duration_sec, created_at, updated_at)
            VALUES(?, ?, ?, 'todo', ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                source=excluded.source,
                status='todo',  -- Reset status on re-prepare
                filepath=excluded.filepath,
                duration_sec=excluded.duration_sec,
                updated_at=excluded.updated_at,
                retries=0, -- Reset retries
                error=NULL -- Clear previous errors
        """,
            (id, title, source, filepath, duration_sec, now, now),
        )
        self.conn.commit()

    def get_next_video(self, step=None):
        """
        Atomically claim and return the next video to process for the given pipeline step.
        Ensures only one worker can claim a video at a time, preventing race conditions.
        """
        cur = self.conn.cursor()
        if step == "transcribe":
            status_to_claim = "downloaded"
            new_status = "transcribing"
        elif step == "download":
            status_to_claim = "todo"
            new_status = "downloading"
        elif step == "ingest":
            status_to_claim = "transcribed"
            new_status = "ingesting"
        else:
            status_to_claim = "todo"
            new_status = "downloading"
        # Use a transaction to atomically claim the next video
        self.conn.execute("BEGIN IMMEDIATE")
        row = cur.execute(
            "SELECT id FROM videos WHERE status=? ORDER BY created_at LIMIT 1",
            (status_to_claim,),
        ).fetchone()
        if not row:
            self.conn.commit()
            return None
        vid = row["id"]
        now = datetime.utcnow().isoformat()
        cur.execute(
            "UPDATE videos SET status=?, updated_at=? WHERE id=?",
            (new_status, now, vid),
        )
        self.conn.commit()
        # Return the full row after update
        row = cur.execute("SELECT * FROM videos WHERE id=?", (vid,)).fetchone()
        return dict(row) if row else None

    def update_video_status(self, id: str, status: str, error: str | None = None):
        # Update video status and record error if provided (None clears error)
        now = datetime.now(timezone.utc).isoformat()
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE videos SET status=?, updated_at=?, error=? WHERE id=?",
            (status, now, error, id),
        )
        self.conn.commit()

    def save_subtitle(self, video_id: str, start_sec: int, end_sec: int, text: str):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO subtitles(video_id,start_sec,end_sec,text) VALUES(?,?,?,?)",
            (video_id, start_sec, end_sec, text),
        )
        self.conn.commit()

    def save_analysis(
        self, video_id: str, summary: str, topics: list, key_terms: list, chapters: list
    ):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO analysis(video_id,summary,topics,key_terms,chapters) VALUES(?,?,?,?,?)",
            (
                video_id,
                summary,
                json.dumps(topics),
                json.dumps(key_terms),
                json.dumps(chapters),
            ),
        )
        self.conn.commit()

    def list_videos(self):
        cur = self.conn.cursor()
        rows = cur.execute("SELECT * FROM videos").fetchall()
        return [dict(r) for r in rows]

    def get_video(self, id: str) -> dict | None:
        cur = self.conn.cursor()
        row = cur.execute("SELECT * FROM videos WHERE id=?", (id,)).fetchone()
        return dict(row) if row else None

    def increment_retries(self, id: str) -> int:
        cur = self.conn.cursor()
        cur.execute("UPDATE videos SET retries = retries + 1 WHERE id=?", (id,))
        self.conn.commit()
        row = cur.execute("SELECT retries FROM videos WHERE id=?", (id,)).fetchone()
        return row["retries"] if row else 0

    def close(self):
        self.conn.close()
