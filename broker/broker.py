import zmq

def main():
    context = zmq.Context()
    
    # Frontend escuta os clientes (REQ) na porta 5555
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5555")
    
    # Backend escuta os servidores (REP) na porta 5556
    backend = context.socket(zmq.DEALER)
    backend.bind("tcp://*:5556")
    
    print("[Broker] ROUTER-DEALER rodando. Frontend: 5555 | Backend: 5556", flush=True)
    
    # Canaliza mensagens entre ROUTER e DEALER sem bloquear
    zmq.proxy(frontend, backend)

if __name__ == "__main__":
    main()