# Sistema de troca de mensagem instantânea (estilo BBS/IRC)

Projeto de Sistemas Distribuídos. Bots (usuários) fazem login, criam e listam canais, se inscrevem e publicam mensagens. Os servidores guardam tudo em disco.

**Executar:** `docker compose up --build` (não precisa de interação, os bots agem sozinhos). Cada processo mostra no terminal as mensagens que envia e recebe.

## Escolhas

| Item | Escolha |
|---|---|
| Linguagens | Python e Node.js. Cada uma tem servidor e bot, com 2 servidores e 2 bots de cada (8 containers). |
| Troca de mensagens | ZeroMQ (REQ/REP, PUB/SUB) |
| Serialização | MessagePack (binário) |
| Persistência | Arquivos JSON no disco, 1 conjunto por servidor, sem compartilhar arquivo entre eles |

## Componentes

- **Broker** (ROUTER/DEALER, portas 5555 e 5556): recebe os pedidos dos bots e distribui entre os servidores em round-robin.
- **Proxy Pub/Sub** (XSUB 5557 e XPUB 5558): os servidores publicam nos canais e os bots assinam.
- **Serviço de referência** (porta 5559): dá o rank dos servidores, guarda a lista de ativos e recebe os heartbeats.
- **Servidores** e **bots**, em Python e em Node.js.

**Bots:** fazem login, criam um canal se houver menos de 5, se inscrevem em até 3 canais e, em loop, escolhem um canal e enviam 10 mensagens com 1 s de intervalo. Ao receber uma publicação, mostram canal, autor, texto e os timestamps de envio e recebimento.

## Parte 1: login de usuário e criação de canais

O bot faz login, lista os canais e cria canais novos por REQ/REP, passando pelo broker. Cada pedido tem uma resposta.

- **Login:** o bot envia `LOGIN_REQ` só com o nome do usuário (sem senha, por ser um protótipo). O servidor responde `OK` com uma mensagem de boas-vindas, ou `ERROR` se o nome vier vazio. O servidor grava o login com o timestamp.
- **Criação de canal:** o bot envia `CREATE_CHANNEL_REQ` com o nome. O servidor responde `ERROR` se o nome for vazio ou se o canal já existir, e `OK` caso contrário.
- **Listagem:** `LIST_CHANNELS_REQ` devolve uma lista só com os nomes dos canais.
- **Quando criar canal:** o bot cria um canal novo se existirem menos de 5. O nome é `canal-<bot>-<número aleatório>`, para não repetir entre bots.
- **Canais iniciais:** todo servidor começa com `geral` e `telemetria`.
- **Persistência:** o servidor guarda os logins (com timestamp) e os canais em JSON, e cada servidor tem os seus arquivos.

## Parte 2: troca de mensagens e armazenamento das publicações

### Escolhas para a troca de mensagens

- **Requisições (REQ/REP):** o bot usa um socket REQ ligado ao broker, que repassa o pedido a um servidor (socket REP) e devolve a resposta. O padrão REQ/REP garante uma resposta para cada pedido, então o bot sabe se deu certo ou errado (`status: OK` ou `ERROR`).
- **Publicações (PUB/SUB):** o servidor publica no proxy (XSUB, porta 5557) e os bots assinam pelo proxy (XPUB, porta 5558). O **nome do canal é o tópico** da mensagem. Cada bot assina vários canais usando um único socket SUB, e a escolha dos canais é feita só no bot.
- **Fluxo de uma publicação:** o bot envia `PUB_MSG_REQ` com canal e texto. O servidor grava a mensagem em disco, publica no tópico do canal e responde ao bot com o status.
- **Formato:** todas as mensagens REQ/REP têm `type`, `sender`, `timestamp`, `lamport_clock` e `payload`. Os tipos seguem o padrão `LOGIN_REQ` e `LOGIN_REQ_REP`. A publicação é enviada em duas partes: o tópico (nome do canal) e um pacote com `channel`, `sender`, `text`, `timestamp_sent` e `lamport_clock`. Assim, o timestamp de envio vai em todas as mensagens.
- **Serialização:** MessagePack, por ser binário e ter biblioteca em Python e em Node.js. Com isso, servidores e bots das duas linguagens se entendem.
- **Recebimento no bot:** ao receber uma publicação, o bot mostra o canal, o autor, o texto, o timestamp de envio e o timestamp de recebimento.
- **Erros tratados:** login sem nome, canal com nome vazio ou já existente, e publicação sem canal ou sem texto.

### Escolhas para o armazenamento

- **Formato:** arquivos JSON. O enunciado só proíbe JSON na troca de mensagens, e para o disco o formato é livre. JSON foi escolhido por ser simples e legível, e porque dá para recuperar tudo relendo o arquivo quando o servidor reinicia.
- **Um conjunto por servidor:** os arquivos ficam em `DATA_DIR` (`/app/data`) e têm o nome do servidor, como `server_py_1_messages.json`. Os servidores não compartilham arquivos.
- **O que é guardado:**
  - `logins`: usuário e timestamp do login.
  - `channels`: lista de canais, que começa com `geral` e `telemetria`.
  - `messages`: canal, autor, texto, timestamp de envio do bot, timestamp do servidor e relógio lógico. Cada publicação é gravada antes de ser publicada no canal.
- **Recuperação:** ao iniciar, o servidor lê os três arquivos, então nada se perde entre execuções.

## Parte 3: relógios e heartbeat

**Relógio lógico (Lamport):** bots e servidores têm um contador que começa em 0. Ele é incrementado antes de cada envio e vai junto na mensagem (`lamport_clock`). Ao receber uma mensagem, o processo guarda o maior valor entre o seu contador e o recebido, mais 1.

**Serviço de referência:** é um processo à parte, que só conversa com os servidores.
- **Rank:** ao iniciar, o servidor envia `REGISTER_RANK_REQ` e recebe o rank. Os ranks são crescentes (1, 2, 3...), e o mesmo nome nunca recebe dois ranks.
- **Lista de servidores:** `LIST_SERVERS_REQ` devolve o nome e o rank de todos os servidores ativos.
- **Heartbeat:** a cada 10 mensagens de clientes recebidas, o servidor envia `HEARTBEAT_REQ`. Se um servidor ficar mais de 30 s sem enviar, o serviço o remove da lista.
- **Relógio físico:** cada servidor pode ter o relógio adiantado ou atrasado. A variável `CLOCK_SKEW` no `docker-compose.yaml` cria essa defasagem de propósito, para dar para ver a sincronização funcionando. Na parte 3 a hora vinha da referência junto com o heartbeat, e na parte 4 isso passou para o coordenador.

## Parte 4: eleição e sincronização do relógio

Os servidores escolhem um coordenador entre si, e ele passa a fornecer a hora aos outros.

- **Comunicação entre servidores:** cada servidor tem um socket REP na porta 5560 para atender os outros, que se conectam pelo nome do container.
- **Eleição (algoritmo do valentão):** quem inicia a eleição envia `ELECTION_REQ` aos servidores de rank maior, consultando a lista na referência. Se algum responde `OK`, ele assume a eleição e repete o processo. Se ninguém responde, o servidor se declara coordenador. Toda eleição começa quando o servidor liga ou quando o coordenador não responde.
- **Anúncio:** o eleito publica o seu nome no tópico `servers`. Todos assinam esse tópico e guardam o coordenador numa variável.
- **Sincronização do relógio:** a cada 15 mensagens, o servidor envia `CLOCK_REQ` ao coordenador e recebe a hora correta. Ele compensa metade do tempo de ida e volta (RTT) e ajusta o seu relógio. Se o coordenador não responde, o servidor pede uma nova eleição.
- **Referência:** o heartbeat não devolve mais a hora.
- **Adaptação do Berkeley:** o coordenador só informa a sua própria hora, e o ajuste é feito pelos servidores. Não há média das horas de todos os servidores.

## Parte 5: Consistência e replicação

**Problema:** com o round-robin, cada servidor guardava só uma parte das mensagens. Se um caísse, perdia-se parte do histórico. Agora todos os servidores guardam todos os dados.

**Método escolhido:** replicação ativa (multi-master) com consistência eventual. O servidor que recebe o pedido do cliente aplica a mudança, responde ao cliente e a repassa aos outros servidores. Foi escolhida por combinar com o round-robin e não mudar a arquitetura. No modelo primário-backup, o broker teria de mandar todas as escritas ao primário.

**Como foi implementado** (só `server.py` e `server.js` foram alterados):
- Replica logins, canais e mensagens usando o canal servidor↔servidor da parte 4 e a lista de servidores da referência.
- Cada login e mensagem tem um `id` único, para ignorar duplicatas.
- **`REPLICATE_REQ`:** depois de gravar, o servidor de origem envia o registro aos outros em segundo plano, sem atrasar o bot. Quem recebe só grava e não republica no Pub/Sub, para o bot não receber a mensagem duas vezes.
- **`SYNC_REQ`:** ao iniciar e a cada 20 s, cada servidor manda os `id`s que tem e recebe só o que falta. Assim, quem caiu e voltou recupera o histórico.
- **Ordem:** as mensagens ficam ordenadas por (relógio de Lamport, id), então todos os servidores têm o mesmo histórico na mesma ordem.
- **Falhas:** um servidor que não responde é ignorado por 10 s e depois recuperado pelo `SYNC_REQ`.

**Mudanças em relação ao método teórico:**
- Em vez de ordem total das escritas (multicast ordenado ou consenso), foi usada consistência eventual com ordem determinística, o que basta para um histórico de mensagens.
- Foi acrescentada a sincronização periódica, porque enviar a todos não cobre servidores que estavam fora do ar.
