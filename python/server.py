import os
import time
import json
import threading
import zmq
import msgpack

SERVER_ID = os.getenv("SERVER_ID", "server_py_1")
BROKER_ADDR = os.getenv("BROKER_ADDR", "tcp://broker:5556")
PROXY_PUB_ADDR = os.getenv("PROXY_PUB_ADDR", "tcp://proxy:5557")
REF_ADDR = os.getenv("REF_ADDR", "tcp://reference:5559")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
PROXY_SUB_ADDR = os.getenv("PROXY_SUB_ADDR", "tcp://proxy:5558")
PEER_PORT = os.getenv("PEER_PORT", "5560")          # porta REP para comunicação servidor<->servidor
CLOCK_SKEW = float(os.getenv("CLOCK_SKEW", "0"))    # defasagem inicial do relógio (para testar a sincronização)
SERVERS_TOPIC = "servers"
SYNC_EVERY = 15                                     # sincroniza o relógio a cada 15 mensagens
PEER_TIMEOUT_MS = 2000

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

# Estado Distribuído (Lamport Clock e Heartbeat)
lamport_clock = 0
msg_counter = 0
server_rank = None

context = zmq.Context()
rep_socket = context.socket(zmq.REP)
rep_socket.connect(BROKER_ADDR)

pub_socket = context.socket(zmq.PUB)
pub_socket.connect(PROXY_PUB_ADDR)

ref_socket = context.socket(zmq.REQ)
ref_socket.connect(REF_ADDR)

# Parte 4: relógio físico ajustável (Berkeley) e coordenador eleito
clock_offset = CLOCK_SKEW
coordinator = None
sync_counter = 0
lamport_lock = threading.Lock()
ref_lock = threading.Lock()
election_lock = threading.Lock()

def now_time():
    return time.time() + clock_offset

def now_ts():
    return int(now_time())

def update_lamport(recv_clock):
    global lamport_clock
    with lamport_lock:
        lamport_clock = max(lamport_clock, recv_clock) + 1

def increment_lamport():
    global lamport_clock
    with lamport_lock:
        lamport_clock += 1
        return lamport_clock

def send_heartbeat():
    clock = increment_lamport()
    msg = {
        "type": "HEARTBEAT_REQ",
        "sender": SERVER_ID,
        "timestamp": now_ts(),
        "lamport_clock": clock,
        "payload": {"name": SERVER_ID}
    }
    with ref_lock:
        ref_socket.send(msgpack.packb(msg))
        raw_rep = ref_socket.recv()
    rep = msgpack.unpackb(raw_rep, raw=False)
    update_lamport(rep.get("lamport_clock", 0))
    print(f"[{SERVER_ID}] Heartbeat enviado ao Servico de Referencia.", flush=True)

# Obter Rank no início
time.sleep(2)
increment_lamport()
ref_socket.send(msgpack.packb({
    "type": "REGISTER_RANK_REQ",
    "sender": SERVER_ID,
    "timestamp": now_ts(),
    "lamport_clock": lamport_clock,
    "payload": {"name": SERVER_ID}
}))
rank_rep = msgpack.unpackb(ref_socket.recv(), raw=False)
update_lamport(rank_rep.get("lamport_clock", 0))
server_rank = rank_rep.get("payload", {}).get("rank")
print(f"[{SERVER_ID}] Servidor ativo. Rank atribuído: {server_rank}", flush=True)

def log_event(action, msg_data):
    print("=" * 60, flush=True)
    print(f"[{time.strftime('%H:%M:%S')}] [{SERVER_ID}] [{action}]", flush=True)
    print(f"Tipo: {msg_data.get('type')}", flush=True)
    print(f"Origem: {msg_data.get('sender')}", flush=True)
    print(f"Relogio Logico (Lamport): {msg_data.get('lamport_clock')}", flush=True)
    print(f"Timestamp: {msg_data.get('timestamp')}", flush=True)
    print(f"Payload: {msg_data.get('payload')}", flush=True)
    print("=" * 60, flush=True)

# ======================= PARTE 4: ELEIÇÃO E SINCRONIZAÇÃO =======================

def peer_request(name, req_type, payload):
    """REQ para outro servidor. Retorna a resposta ou None se indisponível."""
    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.RCVTIMEO, PEER_TIMEOUT_MS)
    sock.setsockopt(zmq.SNDTIMEO, PEER_TIMEOUT_MS)
    try:
        sock.connect(f"tcp://{name}:{PEER_PORT}")
        msg = {
            "type": req_type,
            "sender": SERVER_ID,
            "timestamp": now_ts(),
            "lamport_clock": increment_lamport(),
            "payload": payload
        }
        log_event("ENVIO", msg)
        sock.send(msgpack.packb(msg))
        rep = msgpack.unpackb(sock.recv(), raw=False)
        update_lamport(rep.get("lamport_clock", 0))
        log_event("RECEBIMENTO", rep)
        return rep
    except zmq.ZMQError:
        print(f"[{SERVER_ID}] Servidor '{name}' indisponivel ({req_type}).", flush=True)
        return None
    finally:
        sock.close()

def ref_list_servers():
    with ref_lock:
        ref_socket.send(msgpack.packb({
            "type": "LIST_SERVERS_REQ",
            "sender": SERVER_ID,
            "timestamp": now_ts(),
            "lamport_clock": increment_lamport(),
            "payload": {}
        }))
        rep = msgpack.unpackb(ref_socket.recv(), raw=False)
    update_lamport(rep.get("lamport_clock", 0))
    return rep.get("payload", {}).get("servers", [])

# Socket PUB exclusivo para anunciar o coordenador (só é usado dentro de election_lock)
announce_socket = context.socket(zmq.PUB)
announce_socket.connect(PROXY_PUB_ADDR)

def announce_coordinator():
    global coordinator
    coordinator = SERVER_ID
    msg = {
        "type": "COORDINATOR_PUB",
        "sender": SERVER_ID,
        "timestamp": now_ts(),
        "lamport_clock": increment_lamport(),
        "payload": {"coordinator": SERVER_ID}
    }
    log_event("ENVIO", {**msg, "type": f"PUB [{SERVERS_TOPIC}]"})
    announce_socket.send_multipart([SERVERS_TOPIC.encode("utf-8"), msgpack.packb(msg)])
    print(f"[{SERVER_ID}] Eleito como COORDENADOR.", flush=True)

def start_election():
    """Algoritmo do valentão: pede eleição aos servidores de rank maior."""
    if not election_lock.acquire(blocking=False):
        return  # já existe uma eleição em andamento neste servidor
    try:
        print(f"[{SERVER_ID}] Iniciando eleicao (rank {server_rank}).", flush=True)
        higher = [s for s in ref_list_servers() if s["rank"] > server_rank]
        for s in sorted(higher, key=lambda x: x["rank"]):
            rep = peer_request(s["name"], "ELECTION_REQ", {"rank": server_rank})
            if rep and rep.get("payload", {}).get("status") == "OK":
                print(f"[{SERVER_ID}] '{s['name']}' (rank maior) assumiu a eleicao.", flush=True)
                return
        announce_coordinator()
    except Exception as e:
        print(f"[{SERVER_ID}] Erro na eleicao: {e}", flush=True)
    finally:
        election_lock.release()

def sync_clock():
    """Berkeley: pede a hora correta ao coordenador e ajusta o relógio local."""
    global clock_offset
    if coordinator is None:
        start_election()
        return
    if coordinator == SERVER_ID:
        print(f"[{SERVER_ID}] Sou o coordenador: relogio de referencia ({now_ts()}).", flush=True)
        return
    t0 = time.time()
    rep = peer_request(coordinator, "CLOCK_REQ", {"local_time": now_time()})
    t1 = time.time()
    if rep is None:
        print(f"[{SERVER_ID}] Coordenador '{coordinator}' indisponivel. Pedindo eleicao.", flush=True)
        start_election()
        return
    coord_time = rep.get("payload", {}).get("time")
    if coord_time is None:
        return
    correct = coord_time + (t1 - t0) / 2          # compensa metade do RTT
    adjust = correct - now_time()
    clock_offset += adjust
    print(f"[{SERVER_ID}] Relogio sincronizado com '{coordinator}'. Ajuste: {adjust:+.3f}s", flush=True)

def peer_loop():
    """Atende REQs de outros servidores: relógio (coordenador) e eleição."""
    sock = context.socket(zmq.REP)
    sock.bind(f"tcp://*:{PEER_PORT}")
    while True:
        request = msgpack.unpackb(sock.recv(), raw=False)
        update_lamport(request.get("lamport_clock", 0))
        log_event("RECEBIMENTO", request)
        req_type = request.get("type")
        start_new_election = False

        if req_type == "CLOCK_REQ":
            reply_payload = {"status": "OK", "time": now_time()}
        elif req_type == "ELECTION_REQ":
            reply_payload = {"status": "OK"}
            start_new_election = True
        else:
            reply_payload = {"status": "ERROR", "msg": "Requisicao desconhecida"}

        response = {
            "type": f"{req_type}_REP",
            "sender": SERVER_ID,
            "timestamp": now_ts(),
            "lamport_clock": increment_lamport(),
            "payload": reply_payload
        }
        log_event("ENVIO", response)
        sock.send(msgpack.packb(response))
        if start_new_election:
            threading.Thread(target=start_election, daemon=True).start()

def servers_listener():
    """Assina o tópico 'servers' e guarda o nome do coordenador eleito."""
    global coordinator
    sub = context.socket(zmq.SUB)
    sub.connect(PROXY_SUB_ADDR)
    sub.setsockopt_string(zmq.SUBSCRIBE, SERVERS_TOPIC)
    while True:
        topic, raw = sub.recv_multipart()
        msg = msgpack.unpackb(raw, raw=False)
        update_lamport(msg.get("lamport_clock", 0))
        log_event("RECEBIMENTO", {**msg, "type": f"PUB [{topic.decode()}]"})
        coordinator = msg.get("payload", {}).get("coordinator")
        print(f"[{SERVER_ID}] Coordenador atual: {coordinator}", flush=True)

threading.Thread(target=peer_loop, daemon=True).start()
threading.Thread(target=servers_listener, daemon=True).start()
time.sleep(1)  # dá tempo da assinatura do tópico propagar pelo proxy
threading.Thread(target=start_election, daemon=True).start()

while True:
    raw_msg = rep_socket.recv()
    request = msgpack.unpackb(raw_msg, raw=False)
    
    update_lamport(request.get("lamport_clock", 0))
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
                "server_timestamp": now_ts(),
                "lamport_clock": lamport_clock
            }
            messages.append(msg_record)
            save_json(MESSAGES_FILE, messages)

            pub_clock = increment_lamport()
            pub_packet = {
                "channel": target_channel,
                "sender": sender,
                "text": msg_text,
                "timestamp_sent": request.get("timestamp"),
                "lamport_clock": pub_clock
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

    msg_counter += 1
    if msg_counter >= 10:
        send_heartbeat()
        msg_counter = 0

    send_clock = increment_lamport()
    response = {
        "type": f"{req_type}_REP",
        "sender": SERVER_ID,
        "timestamp": now_ts(),
        "lamport_clock": send_clock,
        "payload": reply_payload
    }
    log_event("ENVIO", response)
    rep_socket.send(msgpack.packb(response))

    # Parte 4: a cada 15 mensagens, sincroniza o relógio com o coordenador
    # (feito depois do reply para não atrasar o cliente caso ocorra uma eleição)
    sync_counter += 1
    if sync_counter >= SYNC_EVERY:
        sync_counter = 0
        sync_clock()