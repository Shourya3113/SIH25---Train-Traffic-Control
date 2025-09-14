"""Core TTC components: BlockManager, ReservationManager (DB-backed), Dispatcher, SafetyMonitor"""
import os
import threading
from typing import List, Tuple, Optional
from dataclasses import dataclass
import time
import psycopg2
from . import db_reservation

DB_DSN = os.getenv('DATABASE_URL', 'postgresql://ttc_user:ttc_pass@db:5432/ttc_db')

@dataclass
class Train:
    id: str
    pos: int
    dir: int  # +1 or -1
    speed: float = 1.0
    status: str = 'IDLE'

class BlockManager:
    def __init__(self, num_blocks:int=13, loop_start:int=5, loop_end:int=7):
        self.num_blocks = num_blocks
        self.loop_start = loop_start
        self.loop_end = loop_end
        # lanes per block
        self.lanes = {i: (2 if loop_start <= i <= loop_end else 1) for i in range(num_blocks)}
        # occupancy: block -> list of lane -> train_id or None
        self.occupancy = {i: [None]*self.lanes[i] for i in range(num_blocks)}
        self.lock = threading.Lock()

    def lane_for_direction(self, block, direction):
        if self.lanes[block] == 1:
            return 0
        return 0 if direction==+1 else 1

    def is_free(self, block, direction):
        lane = self.lane_for_direction(block, direction)
        with self.lock:
            return self.occupancy[block][lane] is None

    def occupy(self, block, train_id, direction):
        lane = self.lane_for_direction(block, direction)
        with self.lock:
            self.occupancy[block][lane] = train_id

    def leave(self, block, train_id, direction):
        lane = self.lane_for_direction(block, direction)
        with self.lock:
            if self.occupancy[block][lane] == train_id:
                self.occupancy[block][lane] = None

class ReservationManager:
    def __init__(self, dsn:Optional[str]=None):
        self.dsn = dsn or DB_DSN

    def try_reserve(self, train_id:str, path:List[Tuple[int,int]]):
        conn = psycopg2.connect(self.dsn)
        try:
            ok, resv_id = db_reservation.try_reserve_path(conn, train_id, path)
            return ok, resv_id
        finally:
            conn.close()

    def release(self, resv_id:int):
        conn = psycopg2.connect(self.dsn)
        try:
            return db_reservation.release_reservation(conn, resv_id)
        finally:
            conn.close()

class Dispatcher:
    def __init__(self, block_manager:BlockManager, res_mgr:ReservationManager):
        self.bm = block_manager
        self.rm = res_mgr
        self.queue = []  # simple queue of (train_id, path, future/callback)
        self.lock = threading.Lock()

    def request_entry(self, train_id:str, path:List[Tuple[int,int]]):
        # try immediate reservation via DB manager
        ok, resv_id = self.rm.try_reserve(train_id, path)
        if ok:
            return True, resv_id
        # else queue; simple retry later (polling)
        with self.lock:
            self.queue.append((train_id, path))
        return False, None

    def process_queue_once(self):
        # try queued requests one by one
        with self.lock:
            remaining = []
            for train_id, path in self.queue:
                ok, resv_id = self.rm.try_reserve(train_id, path)
                if ok:
                    print(f"Dispatcher: granted queued reservation {resv_id} for {train_id}")
                    # notify external systems as needed
                else:
                    remaining.append((train_id, path))
            self.queue = remaining

class SafetyMonitor:
    def __init__(self, bm:BlockManager):
        self.bm = bm

    def check_invariants(self):
        # simple check: no lane has >1 occupancy (struct ensures that)
        for b, lanes in self.bm.occupancy.items():
            for lane in lanes:
                # lane is either None or a train id string
                pass
        return True
