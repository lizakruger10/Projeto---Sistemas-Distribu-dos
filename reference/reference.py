import time
import zmq
import msgpack

def main():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5559")

    # Mapeamento: { "server_name": {"rank": int, "last_heartbeat": float} }
    registered_servers = {}
    next_rank = 1

    print("[Servico de Referencia] Ativo na porta 5559.", flush=True)

    while True:
        raw_msg = socket.recv()
        request = msgpack.unpackb(raw_msg, raw=False)
        
        req_type = request.get("type")
        sender = request.get("sender")
        payload = request.get("payload", {})
        current_time = time.time()

        # Limpeza periódica de servidores inativos (sem heartbeat há mais de 30s)
        dead_servers = [s for s, info in registered_servers.items() if current_time - info["last_heartbeat"] > 30]
        for ds in dead_servers:
            print(f"[Referencia] Removendo servidor inativo: {ds}", flush=True)
            del registered_servers[ds]

        reply_payload = {}

        # 1. Atribuir ou consultar Rank do Servidor
        if req_type == "REGISTER_RANK_REQ":
            server_name = payload.get("name", sender)
            if server_name not in registered_servers:
                registered_servers[server_name] = {
                    "rank": next_rank,
                    "last_heartbeat": current_time
                }
                print(f"[Referencia] Registrado: {server_name} com Rank {next_rank}", flush=True)
                next_rank += 1
            
            rank_assigned = registered_servers[server_name]["rank"]
            reply_payload = {"status": "OK", "rank": rank_assigned}

        # 2. Fornecer Lista de Servidores
        elif req_type == "LIST_SERVERS_REQ":
            server_list = [{"name": s, "rank": info["rank"]} for s, info in registered_servers.items()]
            reply_payload = {"status": "OK", "servers": server_list}

        # 3. Heartbeat (a partir da parte 4 não retorna mais a hora)
        elif req_type == "HEARTBEAT_REQ":
            server_name = payload.get("name", sender)
            if server_name in registered_servers:
                registered_servers[server_name]["last_heartbeat"] = current_time
            else:
                registered_servers[server_name] = {
                    "rank": next_rank,
                    "last_heartbeat": current_time
                }
                next_rank += 1

            reply_payload = {"status": "OK"}

        else:
            reply_payload = {"status": "ERROR", "msg": "Requisicao desconhecida"}

        response = {
            "type": f"{req_type}_REP",
            "sender": "reference_service",
            "timestamp": int(time.time()),
            "payload": reply_payload
        }
        socket.send(msgpack.packb(response))

if __name__ == "__main__":
    main()