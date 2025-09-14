import os, json, threading, time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from backend.server.ttc_core import BlockManager, ReservationManager, Dispatcher, Train
from typing import Dict, Any
from pathlib import Path

app = FastAPI(title='TTC Backend')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

# initialize components
BM = BlockManager()
RM = ReservationManager()
DISP = Dispatcher(BM, RM)
TRAINS: Dict[str, Train] = {}

# basic in-memory reservation index for demo (persisted in DB via ReservationManager)
RESERVATIONS = {}

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active = set()
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)
    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)
    async def broadcast(self, msg: Dict[str,Any]):
        living = []
        for ws in list(self.active):
            try:
                await ws.send_json(msg)
                living.append(ws)
            except Exception:
                self.active.remove(ws)

manager = ConnectionManager()

@app.get('/api/status')
async def status():
    return {'blocks': BM.occupancy, 'reservations': RESERVATIONS, 'trains': {tid: vars(t) for tid,t in TRAINS.items()}}

@app.post('/api/register_train')
def register_train(payload: Dict[str,Any]):
    tid = payload.get('train_id')
    pos = int(payload.get('pos',0))
    direction = int(payload.get('dir',1))
    if tid in TRAINS:
        raise HTTPException(status_code=400, detail='train already exists')
    TRAINS[tid] = Train(id=tid, pos=pos, dir=direction, status='IDLE')
    # occupy initial block
    BM.occupy(pos, tid, direction)
    return {'ok': True, 'train_id': tid}

@app.post('/api/reserve')
def reserve(payload: Dict[str,Any]):
    train_id = payload.get('train_id')
    path = payload.get('path')  # expected [[block,lane], ...]
    if not train_id or not path:
        raise HTTPException(status_code=400, detail='missing train_id or path')
    ok, resv_id = DISP.request_entry(train_id, path)
    if ok:
        RESERVATIONS[str(resv_id)] = {'train_id': train_id, 'path': path, 'status': 'ACTIVE'}
        # broadcast event in background thread (async)
        threading.Thread(target=lambda: None).start()
        return {'ok': True, 'reservation_id': resv_id}
    else:
        return {'ok': False, 'reason': 'queued'}

@app.post('/api/release')
def release(payload: Dict[str,Any]):
    resv_id = int(payload.get('reservation_id'))
    ok = RM.release(resv_id)
    if ok:
        RESERVATIONS.pop(str(resv_id), None)
        return {'ok': True}
    return {'ok': False, 'reason': 'not_found'}

@app.websocket('/ws')
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)
