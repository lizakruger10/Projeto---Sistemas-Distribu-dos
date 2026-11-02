import os
import time
import random
import threading
import zmq
import msgpack

BOT_ID = os.getenv("BOT_ID", "sensor_clima_01")
BROKER_ADDR = os.getenv("BROKER_ADDR", "tcp://broker:5555")
PROXY_SUB_ADDR = os.getenv("PROXY_SUB_ADDR", "tcp://proxy:5558")

subscribed_channels = set()

def log_event(action, msg_data):
    print("=" * 60, flush=True)
    print(f"[{time.strftime('%H:%M:%S')}] [{BOT_ID}] [{action}]", flush=True)
    print(f"Tipo/Topico: {msg_data.get('type', msg_data.get('channel'))}", flush=True)
    print(f"Origem: {msg_data.get('sender')}", flush=True)
    print(f"Timestamp: {msg_data.get('timestamp', msg_data.get('timestamp_sent'))}", flush=True)
    print(f"Payload/Texto: {msg_data.get('payload', msg_data.get('text'))}", flush=True)
    print("=" * 60, flush=True)

context = zmq.Context()
req_socket = context.socket(zmq.REQ)
req_socket.connect(BROKER_ADDR)

sub_socket = context.socket(zmq.SUB)
sub_socket.connect(PROXY_SUB_ADDR)

def listen_sub():
    while True:
        try:
            topic, raw_msg = sub_socket.recv_multipart()
            msg = msgpack.unpackb(raw_msg, raw=False)
            recv_time = int(time.time())
            print("*" * 60, flush=True)
            print(f"[{time.strftime('%H:%M:%S')}] [{BOT_ID}] [PUB_RECEIVED]", flush=True)
            print(f"Canal: {topic.decode('utf-8')}", flush=True)
            print(f"Autor: {msg.get('sender')}", flush=True)
            print(f"Texto: {msg.get('text')}", flush=True)
            print(f"Timestamp Envio: {msg.get('timestamp_sent')}", flush=True)
            print(f"Timestamp Recebimento: {recv_time}", flush=True)
            print("*" * 60, flush=True)
        except Exception:
            break

threading.Thread(target=listen_sub, daemon=True).start()

def send_request(req_type, payload):
    msg = {
        "type": req_type,
        "sender": BOT_ID,
        "timestamp": int(time.time()),
        "payload": payload
    }
    log_event("ENVIO", msg)
    req_socket.send(msgpack.packb(msg))
    raw_reply = req_socket.recv()
    reply = msgpack.unpackb(raw_reply, raw=False)
    log_event("RECEBIMENTO", reply)
    return reply

time.sleep(2)
send_request("LOGIN_REQ", {"user": BOT_ID})
time.sleep(1)

while True:
    rep = send_request("LIST_CHANNELS_REQ", {})
    available_channels = rep.get("payload", {}).get("channels", [])

    if len(available_channels) < 5:
        new_ch = f"canal-{BOT_ID}-{random.randint(100, 999)}"
        send_request("CREATE_CHANNEL_REQ", {"channel": new_ch})
        time.sleep(1)
        rep = send_request("LIST_CHANNELS_REQ", {})
        available_channels = rep.get("payload", {}).get("channels", [])

    for ch in available_channels:
        if len(subscribed_channels) >= 3:
            break
        if ch not in subscribed_channels:
            sub_socket.setsockopt_string(zmq.SUBSCRIBE, ch)
            subscribed_channels.add(ch)
            print(f"[{BOT_ID}] Inscrito no canal: {ch}", flush=True)

    if available_channels:
        target_ch = random.choice(available_channels)
        print(f"[{BOT_ID}] Disparando 10 mensagens no canal '{target_ch}'...", flush=True)

        for i in range(1, 11):
            temp = round(random.uniform(20.0, 35.0), 2)
            umid = random.randint(40, 80)
            telemetry_data = f"Métrica #{i}: Temp={temp}C, Umidade={umid}%"
            
            send_request("PUB_MSG_REQ", {
                "channel": target_ch,
                "message": telemetry_data
            })
            time.sleep(1)

    time.sleep(3)