import zmq

def main():
    context = zmq.Context()
    
    # XSUB: Recebe publicações dos Servidores na porta 5557
    xsub = context.socket(zmq.XSUB)
    xsub.bind("tcp://*:5557")
    
    # XPUB: Distribui as mensagens para os Bots (SUB) na porta 5558
    xpub = context.socket(zmq.XPUB)
    xpub.bind("tcp://*:5558")
    
    print("[Proxy Pub/Sub] XSUB: 5557 | XPUB: 5558 ativo.", flush=True)
    
    # Roteia os tópicos e mensagens sem bloquear
    zmq.proxy(xsub, xpub)

if __name__ == "__main__":
    main()