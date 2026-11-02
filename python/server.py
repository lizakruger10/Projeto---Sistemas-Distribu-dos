import os
import time
import json
import zmq
import msgpack

SERVER_ID = os.getenv("SERVER_ID", "server_py_1")
BROKER_ADDR = os.getenv("BROKER_ADDR", "tcp://broker:5556")
PROXY_PUB_ADDR = os.getenv("PROXY_PUB_ADDR", "tcp://proxy:5557")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")

os.makedirs(DATA_DIR, exist_ok=True)
LOGINS_FILE = os.path.join(DATA_DIR, f"{SERVER_ID}_logins.json")
CHANNELS_FILE = os.path.join(DATA_DIR, f"{SERVER_ID}_channels.json")
MESSAGES_FILE = os.path.join(DATA_DIR, f"{SERVER_ID}_messages.json")

def load_data():
    logins, channels, messages = [], ["geral", "telemetria"], []
    if os.path.exists(LOGINS_FILE):
        with open(LOGINS_FILE, "r") as f: logins = json.load(f)
    if os.path.exists(CHANNELS_FILE):
        with open(CHANNELS_FILE, "r") as f: channels = json.load(f)
    if os.path.exists(MESSAGES_FILE):
        with open(MESSAGES_FILE, "r") as f: messages = json.load(f)
    return logins, channels, messages

def save_json(filepath, data):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

logins, channels, messages = load_data()

def log_event(action, msg_data):
    print("=" * 60, flush=True)
    print(f"[{time.strftime('%H:%M:%S')}] [{SERVER_ID}] [{action}]", flush=True)
    print(f"Tipo: {msg_data.get('type')}", flush=True)
    print(f"Origem: {msg_data.get('sender')}", flush=True)
    print(f"Timestamp: {msg_data.get('timestamp')}", flush=True)
    print(f"Payload: {msg_data.get('payload')}", flush=True)
    print("=" * 60, flush=True)

context = zmq.Context()
rep_socket = context.socket(zmq.REP)
rep_socket.connect(BROKER_ADDR)

pub_socket = context.socket(zmq.PUB)
pub_socket.connect(PROXY_PUB_ADDR)

print(f"[{SERVER_ID}] Servidor Python ativo. Broker: {BROKER_ADDR} | Proxy PUB: {PROXY_PUB_ADDR}", flush=True)

while True:
    raw_msg = rep_socket.recv()
    request = msgpack.unpackb(raw_msg, raw=False)
    log_event("RECEBIMENTO", request)

    req_type = request.get("type")
    sender = request.get("sender")
    payload = request.get("payload", {})
    reply_payload = {}

    if req_type == "LOGIN_REQ":
        user = payload.get("user")
        if user:
            logins.append({"user": user, "timestamp": request.get("timestamp")})
            save_json(LOGINS_FILE, logins)
            reply_payload = {"status": "OK", "msg": f"Bem-vindo {user}"}
        else:
            reply_payload = {"status": "ERROR", "msg": "Usuario invalido"}

    elif req_type == "CREATE_CHANNEL_REQ":
        ch_name = payload.get("channel")
        if ch_name and ch_name not in channels:
            channels.append(ch_name)
            save_json(CHANNELS_FILE, channels)
            reply_payload = {"status": "OK", "msg": f"Canal '{ch_name}' criado"}
        else:
            reply_payload = {"status": "ERROR", "msg": "Canal invalido ou existente"}

    elif req_type == "LIST_CHANNELS_REQ":
        reply_payload = {"status": "OK", "channels": channels}

    elif req_type == "PUB_MSG_REQ":
        target_channel = payload.get("channel")
        msg_text = payload.get("message")

        if target_channel and msg_text:
            msg_record = {
                "channel": target_channel,
                "sender": sender,
                "text": msg_text,
                "sent_timestamp": request.get("timestamp"),
                "server_timestamp": int(time.time())
            }
            messages.append(msg_record)
            save_json(MESSAGES_FILE, messages)

            pub_packet = {
                "channel": target_channel,
                "sender": sender,
                "text": msg_text,
                "timestamp_sent": request.get("timestamp")
            }
            pub_socket.send_multipart([
                target_channel.encode("utf-8"),
                msgpack.packb(pub_packet)
            ])

            reply_payload = {"status": "OK", "msg": "Mensagem publicada"}
        else:
            reply_payload = {"status": "ERROR", "msg": "Dados de publicacao invalidos"}

    else:
        reply_payload = {"status": "ERROR", "msg": "Requisicao desconhecida"}

    response = {
        "type": f"{req_type}_REP",
        "sender": SERVER_ID,
        "timestamp": int(time.time()),
        "payload": reply_payload
    }
    log_event("ENVIO", response)
    rep_socket.send(msgpack.packb(response))