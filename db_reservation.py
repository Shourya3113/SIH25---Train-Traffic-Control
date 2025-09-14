"""DB-backed reservation manager using PostgreSQL advisory locks.
Provides try_reserve_path and release_reservation utilities.
Requires psycopg2 (psycopg2-binary).
"""
import psycopg2
import psycopg2.extras
from typing import List, Tuple
import os
from contextlib import contextmanager

DSN = os.getenv('DATABASE_URL', 'postgresql://ttc_user:ttc_pass@localhost:5432/ttc_db')

def lock_key(block_id: int, lane_id: int) -> int:
    """Compose a 64-bit advisory lock key from block and lane."""
    return (block_id << 32) | (lane_id & 0xFFFFFFFF)

@contextmanager
def get_conn():
    conn = psycopg2.connect(DSN)
    try:
        yield conn
    finally:
        conn.close()

def try_reserve_path(conn, train_id: str, path: List[Tuple[int,int]]) -> Tuple[bool,int]:
    """Attempt to reserve given path: list of (block_id,lane_id).
    Returns (True,reservation_id) on success, (False,-1) on failure.
    Uses non-blocking pg_try_advisory_lock and acquires in sorted order.
    """
    cur = conn.cursor()
    # compute sorted lock keys to avoid deadlocks
    keys = sorted([lock_key(b,l) for b,l in path])
    acquired = []
    try:
        for k in keys:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (k,))
            ok = cur.fetchone()[0]
            if not ok:
                # release acquired so far
                for ak in acquired:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (ak,))
                conn.commit()
                return False, -1
            acquired.append(k)
        # all locks acquired -> insert reservation record
        cur.execute("INSERT INTO reservations (train_id, status, details) VALUES (%s,'ACTIVE',%s) RETURNING id",
                    (train_id, psycopg2.extras.Json({"path": path})))
        resv_id = cur.fetchone()[0]
        conn.commit()
        return True, resv_id
    except Exception as e:
        conn.rollback()
        # attempt to release any acquired locks
        for ak in acquired:
            try:
                cur.execute("SELECT pg_advisory_unlock(%s)", (ak,))
            except Exception:
                pass
        conn.commit()
        raise

def release_reservation(conn, resv_id: int) -> bool:
    """Release reservation and corresponding advisory locks."""
    cur = conn.cursor()
    cur.execute("SELECT details FROM reservations WHERE id=%s AND status='ACTIVE'", (resv_id,))
    row = cur.fetchone()
    if not row:
        return False
    details = row[0]
    path = details.get('path', [])
    keys = [lock_key(b,l) for b,l in path]
    for k in keys:
        cur.execute("SELECT pg_advisory_unlock(%s)", (k,))
    cur.execute("UPDATE reservations SET status='RELEASED' WHERE id=%s", (resv_id,))
    conn.commit()
    return True
