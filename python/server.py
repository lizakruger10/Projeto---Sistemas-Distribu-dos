import os
import sys
import time
import json
import zmq
import msgpack

SERVER_ID = os.getenv("SERVER_ID", "server_py_1")
BROKER_ADDR = os.getenv("BROKER_ADDR", "tcp://broker:5556")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")

os.makedirs(DATA_DIR, exist_ok=True)
LOGINS_FILE = os.path.join(DATA_DIR, f"{SERVER_ID}_logins.json")
CHANNELS_FILE = os.path.join(DATA_DIR, f"{SERVER_ID}_channels.json")

def load_data():
    logins = []
    channels = ["geral", "telemetria"]
    if os.path.exists(LOGINS_FILE):
        with open(LOGINS_FILE, "r") as f:
            logins = json.load(f)
    if os.path.exists(CHANNELS_FILE):
        with open(CHANNELS_FILE, "r") as f:
            channels = json.load(f)
    return logins, channels

def save_logins(logins_data):
    with open(LOGINS_FILE, "w") as f:
        json.dump(logins_data, f, indent=2)

def save_channels(channels_data):
    with open(CHANNELS_FILE, "w") as f:
        json.dump(channels_data, f, indent=2)

logins, channels = load_data()

def log_event(action, msg_data):
    print("=" * 60, flush=True)
    print(f"[{time.strftime('%H:%M:%S')}] [{SERVER_ID}] [{action}]", flush=True)
    print(f"Tipo: {msg_data.get('type')}", flush=True)
    print(f"Origem: {msg_data.get('sender')}", flush=True)
    print(f"Timestamp Envio: {msg_data.get('timestamp')}", flush=True)
    print(f"Payload: {msg_data.get('payload')}", flush=True)
    print("=" * 60, flush=True)

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect(BROKER_ADDR)

print(f"[{SERVER_ID}] Servidor Python ativo e conectado ao Broker em {BROKER_ADDR}", flush=True)

while True:
    raw_msg = socket.recv()
    request = msgpack.unpackb(raw_msg, raw=False)
    log_event("RECEBIMENTO", request)

    req_type = request.get("type")
    sender = request.get("sender")
    payload = request.get("payload", {})

    reply_payload = {}
    reply_type = f"{req_type}_REP"

    if req_type == "LOGIN_REQ":
        user = payload.get("user")
        if not user:
            reply_payload = {"status": "ERROR", "msg": "Usuario invalido"}
        else:
            logins.append({"user": user, "timestamp": request.get("timestamp")})
            save_logins(logins)
            reply_payload = {"status": "OK", "msg": f"Bem-vindo {user}"}

    elif req_type == "CREATE_CHANNEL_REQ":
        channel_name = payload.get("channel")
        if not channel_name:
            reply_payload = {"status": "ERROR", "msg": "Nome de canal invalido"}
        elif channel_name in channels:
            reply_payload = {"status": "ERROR", "msg": f"Canal '{channel_name}' ja existe"}
        else:
            channels.append(channel_name)
            save_channels(channels)
            reply_payload = {"status": "OK", "msg": f"Canal '{channel_name}' criado"}

    elif req_type == "LIST_CHANNELS_REQ":
        reply_payload = {"status": "OK", "channels": channels}

    else:
        reply_payload = {"status": "ERROR", "msg": "Requisicao desconhecida"}

    response = {
        "type": reply_type,
        "sender": SERVER_ID,
        "timestamp": int(time.time()),
        "payload": reply_payload
    }

    log_event("ENVIO", response)
    socket.send(msgpack.packb(response))