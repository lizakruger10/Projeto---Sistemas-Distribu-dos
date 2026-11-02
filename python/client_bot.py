import os
import time
import zmq
import msgpack

BOT_ID = os.getenv("BOT_ID", "bot_py_1")
BROKER_ADDR = os.getenv("BROKER_ADDR", "tcp://broker:5555")

def log_event(action, msg_data):
    print("=" * 60, flush=True)
    print(f"[{time.strftime('%H:%M:%S')}] [{BOT_ID}] [{action}]", flush=True)
    print(f"Tipo: {msg_data.get('type')}", flush=True)
    print(f"Origem: {msg_data.get('sender')}", flush=True)
    print(f"Timestamp: {msg_data.get('timestamp')}", flush=True)
    print(f"Payload: {msg_data.get('payload')}", flush=True)
    print("=" * 60, flush=True)

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect(BROKER_ADDR)

def send_request(req_type, payload):
    msg = {
        "type": req_type,
        "sender": BOT_ID,
        "timestamp": int(time.time()),
        "payload": payload
    }
    log_event("ENVIO", msg)
    socket.send(msgpack.packb(msg))
    
    raw_reply = socket.recv()
    reply = msgpack.unpackb(raw_reply, raw=False)
    log_event("RECEBIMENTO", reply)
    return reply

time.sleep(2)

# 1. Login
login_rep = send_request("LOGIN_REQ", {"user": BOT_ID})
while login_rep.get("payload", {}).get("status") != "OK":
    time.sleep(2)
    login_rep = send_request("LOGIN_REQ", {"user": BOT_ID})

time.sleep(1)

# 2. Listar canais
send_request("LIST_CHANNELS_REQ", {})
time.sleep(1)

# 3. Criar canal novo
new_channel = f"canal-{BOT_ID}"
send_request("CREATE_CHANNEL_REQ", {"channel": new_channel})
time.sleep(1)

# 4. Listar canais novamente
send_request("LIST_CHANNELS_REQ", {})

print(f"[{BOT_ID}] Parte 1 executada com sucesso.", flush=True)
while True:
    time.sleep(10)