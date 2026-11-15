const zmq = require('zeromq');
const { encode, decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const SERVER_ID = process.env.SERVER_ID || 'server_node_1';
const BROKER_ADDR = process.env.BROKER_ADDR || 'tcp://broker:5556';
const PROXY_PUB_ADDR = process.env.PROXY_PUB_ADDR || 'tcp://proxy:5557';
const REF_ADDR = process.env.REF_ADDR || 'tcp://reference:5559';
const DATA_DIR = process.env.DATA_DIR || '/app/data';
const PROXY_SUB_ADDR = process.env.PROXY_SUB_ADDR || 'tcp://proxy:5558';
const PEER_PORT = process.env.PEER_PORT || '5560';          // porta REP para comunicação servidor<->servidor
const CLOCK_SKEW = parseFloat(process.env.CLOCK_SKEW || '0'); // defasagem inicial do relógio (para testar a sincronização)
const SERVERS_TOPIC = 'servers';
const SYNC_EVERY = 15;                                      // sincroniza o relógio a cada 15 mensagens
const PEER_TIMEOUT_MS = 2000;
const SYNC_INTERVAL = 20000;                               // Parte 5: intervalo da sincronização (anti-entropia) entre réplicas
const PEER_DOWN_MS = 10000;                                 // Parte 5: tempo sem tentar replicar para um servidor que não respondeu

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

const LOGINS_FILE = path.join(DATA_DIR, `${SERVER_ID}_logins.json`);
const CHANNELS_FILE = path.join(DATA_DIR, `${SERVER_ID}_channels.json`);
const MESSAGES_FILE = path.join(DATA_DIR, `${SERVER_ID}_messages.json`);

function loadData() {
  let logins = [], channels = ['geral', 'telemetria'], messages = [];
  if (fs.existsSync(LOGINS_FILE)) logins = JSON.parse(fs.readFileSync(LOGINS_FILE, 'utf-8'));
  if (fs.existsSync(CHANNELS_FILE)) channels = JSON.parse(fs.readFileSync(CHANNELS_FILE, 'utf-8'));
  if (fs.existsSync(MESSAGES_FILE)) messages = JSON.parse(fs.readFileSync(MESSAGES_FILE, 'utf-8'));
  return { logins, channels, messages };
}

function saveJson(filepath, data) {
  fs.writeFileSync(filepath, JSON.stringify(data, null, 2));
}

const { logins, channels, messages } = loadData();

// Parte 5: todo registro tem um id único (permite eliminar duplicatas ao replicar)
for (const rec of [...logins, ...messages]) {
  if (!rec.id) rec.id = crypto.randomUUID();
}
const loginIds = new Set(logins.map(l => l.id));
const messageIds = new Set(messages.map(m => m.id));

// mesma ordem em todos os servidores: relógio lógico e, em caso de empate, o id
function messageCmp(a, b) {
  const d = (a.lamport_clock || 0) - (b.lamport_clock || 0);
  if (d !== 0) return d;
  return a.id < b.id ? -1 : (a.id > b.id ? 1 : 0);
}
messages.sort(messageCmp);

function addLogin(rec) {
  if (loginIds.has(rec.id)) return false;
  loginIds.add(rec.id);
  logins.push(rec);
  saveJson(LOGINS_FILE, logins);
  return true;
}

function addChannel(name) {
  if (channels.includes(name)) return false;
  channels.push(name);
  saveJson(CHANNELS_FILE, channels);
  return true;
}

function addMessage(rec) {
  if (messageIds.has(rec.id)) return false;
  messageIds.add(rec.id);
  messages.push(rec);
  messages.sort(messageCmp);
  saveJson(MESSAGES_FILE, messages);
  return true;
}

let lamportClock = 0;
let msgCounter = 0;
let serverRank = null;

// Parte 4: relógio físico ajustável (Berkeley) e coordenador eleito
let clockOffset = CLOCK_SKEW;
let coordinator = null;
let syncCounter = 0;
let electionRunning = false;
const nowTime = () => Date.now() / 1000 + clockOffset;
const nowTs = () => Math.floor(nowTime());

function updateLamport(recvClock) {
  lamportClock = Math.max(lamportClock, recvClock || 0) + 1;
}

function incrementLamport() {
  lamportClock += 1;
  return lamportClock;
}

function logEvent(action, msgData) {
  const timeStr = new Date().toISOString().split('T')[1].slice(0, 8);
  console.log('='.repeat(60));
  console.log(`[${timeStr}] [${SERVER_ID}] [${action}]`);
  console.log(`Tipo: ${msgData.type}`);
  console.log(`Origem: ${msgData.sender}`);
  console.log(`Relogio Logico (Lamport): ${msgData.lamport_clock}`);
  console.log(`Timestamp: ${msgData.timestamp}`);
  let payloadStr = JSON.stringify(msgData.payload);
  if (payloadStr && payloadStr.length > 500) payloadStr = `${payloadStr.slice(0, 500)}... (${payloadStr.length} caracteres)`;
  console.log(`Payload: ${payloadStr}`);
  console.log('='.repeat(60));
}

const refSocket = new zmq.Request();
let refChain = Promise.resolve();
// O socket REQ não aceita chamadas concorrentes: serializa heartbeat/lista/registro
function refCall(msg) {
  const p = refChain.then(async () => {
    await refSocket.send(encode(msg));
    const [buf] = await refSocket.receive();
    return decode(buf);
  });
  refChain = p.catch(() => {});
  return p;
}

async function sendHeartbeat() {
  const clock = incrementLamport();
  const msg = {
    type: 'HEARTBEAT_REQ',
    sender: SERVER_ID,
    timestamp: nowTs(),
    lamport_clock: clock,
    payload: { name: SERVER_ID }
  };
  const rep = await refCall(msg);
  updateLamport(rep.lamport_clock);
  console.log(`[${SERVER_ID}] Heartbeat enviado ao Servico de Referencia.`);
}

// ======================= PARTE 4: ELEIÇÃO E SINCRONIZAÇÃO =======================

// REQ para outro servidor. Retorna a resposta ou null se indisponível.
async function peerRequest(name, reqType, payload) {
  const sock = new zmq.Request({ linger: 0, sendTimeout: PEER_TIMEOUT_MS, receiveTimeout: PEER_TIMEOUT_MS });
  try {
    sock.connect(`tcp://${name}:${PEER_PORT}`);
    const msg = {
      type: reqType,
      sender: SERVER_ID,
      timestamp: nowTs(),
      lamport_clock: incrementLamport(),
      payload
    };
    logEvent('ENVIO', msg);
    await sock.send(encode(msg));
    const [buf] = await sock.receive();
    const rep = decode(buf);
    updateLamport(rep.lamport_clock);
    logEvent('RECEBIMENTO', rep);
    return rep;
  } catch (err) {
    console.log(`[${SERVER_ID}] Servidor '${name}' indisponivel (${reqType}).`);
    return null;
  } finally {
    sock.close();
  }
}

// Socket PUB exclusivo para anunciar o coordenador
const announceSocket = new zmq.Publisher();

async function announceCoordinator() {
  coordinator = SERVER_ID;
  const msg = {
    type: 'COORDINATOR_PUB',
    sender: SERVER_ID,
    timestamp: nowTs(),
    lamport_clock: incrementLamport(),
    payload: { coordinator: SERVER_ID }
  };
  logEvent('ENVIO', { ...msg, type: `PUB [${SERVERS_TOPIC}]` });
  await announceSocket.send([SERVERS_TOPIC, encode(msg)]);
  console.log(`[${SERVER_ID}] Eleito como COORDENADOR.`);
}

// Algoritmo do valentão: pede eleição aos servidores de rank maior
async function startElection() {
  if (electionRunning) return;
  electionRunning = true;
  try {
    console.log(`[${SERVER_ID}] Iniciando eleicao (rank ${serverRank}).`);
    const listRep = await refCall({
      type: 'LIST_SERVERS_REQ',
      sender: SERVER_ID,
      timestamp: nowTs(),
      lamport_clock: incrementLamport(),
      payload: {}
    });
    updateLamport(listRep.lamport_clock);
    const higher = (listRep.payload?.servers || [])
      .filter(s => s.rank > serverRank)
      .sort((a, b) => a.rank - b.rank);
    for (const s of higher) {
      const rep = await peerRequest(s.name, 'ELECTION_REQ', { rank: serverRank });
      if (rep && rep.payload?.status === 'OK') {
        console.log(`[${SERVER_ID}] '${s.name}' (rank maior) assumiu a eleicao.`);
        return;
      }
    }
    await announceCoordinator();
  } catch (err) {
    console.error(`[${SERVER_ID}] Erro na eleicao:`, err.message);
  } finally {
    electionRunning = false;
  }
}

// Berkeley: pede a hora correta ao coordenador e ajusta o relógio local
async function syncClock() {
  if (coordinator === null) {
    await startElection();
    return;
  }
  if (coordinator === SERVER_ID) {
    console.log(`[${SERVER_ID}] Sou o coordenador: relogio de referencia (${nowTs()}).`);
    return;
  }
  const t0 = Date.now() / 1000;
  const rep = await peerRequest(coordinator, 'CLOCK_REQ', { local_time: nowTime() });
  const t1 = Date.now() / 1000;
  if (rep === null) {
    console.log(`[${SERVER_ID}] Coordenador '${coordinator}' indisponivel. Pedindo eleicao.`);
    await startElection();
    return;
  }
  const coordTime = rep.payload?.time;
  if (coordTime === undefined) return;
  const correct = coordTime + (t1 - t0) / 2; // compensa metade do RTT
  const adjust = correct - nowTime();
  clockOffset += adjust;
  console.log(`[${SERVER_ID}] Relogio sincronizado com '${coordinator}'. Ajuste: ${adjust >= 0 ? '+' : ''}${adjust.toFixed(3)}s`);
}

// ======================= PARTE 5: REPLICAÇÃO =======================
// Replicação ativa (multi-master): o servidor que recebe a requisição do cliente
// aplica localmente e repassa o registro a todos os outros servidores (REPLICATE_REQ).
// Para recuperar o que foi perdido (servidor novo/reiniciado ou falha de rede), cada
// servidor também faz sincronização periódica (SYNC_REQ) trocando apenas o que falta.

const replicationQueue = [];
const peerDownUntil = {};
let replicating = false;

function replicate(kind, record) {
  replicationQueue.push({ kind, record });
  if (!replicating) drainReplication();
}

async function otherServers() {
  try {
    const rep = await refCall({
      type: 'LIST_SERVERS_REQ',
      sender: SERVER_ID,
      timestamp: nowTs(),
      lamport_clock: incrementLamport(),
      payload: {}
    });
    updateLamport(rep.lamport_clock);
    return (rep.payload?.servers || []).map(s => s.name).filter(n => n !== SERVER_ID);
  } catch (err) {
    console.error(`[${SERVER_ID}] Erro ao listar servidores:`, err.message);
    return [];
  }
}

async function drainReplication() {
  replicating = true;
  try {
    while (replicationQueue.length > 0) {
      const { kind, record } = replicationQueue.shift();
      for (const name of await otherServers()) {
        if ((peerDownUntil[name] || 0) > Date.now()) continue; // será recuperado pela sincronização periódica
        const rep = await peerRequest(name, 'REPLICATE_REQ', { kind, record });
        if (rep === null) peerDownUntil[name] = Date.now() + PEER_DOWN_MS;
      }
    }
  } catch (err) {
    console.error(`[${SERVER_ID}] Erro na replicacao:`, err.message);
  } finally {
    replicating = false;
  }
}

function handleReplicate(payload) {
  const { kind, record } = payload;
  if (kind === 'message' && record) addMessage(record);
  else if (kind === 'login' && record) addLogin(record);
  else if (kind === 'channel' && record) addChannel(record);
  else return { status: 'ERROR', msg: 'Replicacao invalida' };
  return { status: 'OK' };
}

function handleSync(payload) {
  const haveLogins = new Set(payload.login_ids || []);
  const haveMessages = new Set(payload.message_ids || []);
  for (const ch of payload.channels || []) addChannel(ch);
  return {
    status: 'OK',
    logins: logins.filter(l => !haveLogins.has(l.id)),
    messages: messages.filter(m => !haveMessages.has(m.id)),
    channels: [...channels]
  };
}

async function syncWithPeers() {
  for (const name of await otherServers()) {
    const rep = await peerRequest(name, 'SYNC_REQ', {
      login_ids: logins.map(l => l.id),
      message_ids: messages.map(m => m.id),
      channels: [...channels]
    });
    if (!rep || rep.payload?.status !== 'OK') continue;
    let novos = 0;
    for (const l of rep.payload.logins || []) if (addLogin(l)) novos++;
    for (const m of rep.payload.messages || []) if (addMessage(m)) novos++;
    for (const c of rep.payload.channels || []) if (addChannel(c)) novos++;
    if (novos > 0) console.log(`[${SERVER_ID}] Sincronizacao com '${name}': ${novos} registro(s) recebido(s).`);
  }
}

async function runSyncLoop() {
  await new Promise(res => setTimeout(res, 3000)); // espera os outros servidores se registrarem
  while (true) {
    try {
      await syncWithPeers();
    } catch (err) {
      console.error(`[${SERVER_ID}] Erro na sincronizacao:`, err.message);
    }
    await new Promise(res => setTimeout(res, SYNC_INTERVAL));
  }
}

// Atende REQs de outros servidores: relógio (coordenador), eleição e replicação
async function runPeerServer() {
  const sock = new zmq.Reply();
  await sock.bind(`tcp://*:${PEER_PORT}`);
  for await (const [buf] of sock) {
    let startNewElection = false;
    let reqType = 'UNKNOWN';
    try {
      const request = decode(buf);
      updateLamport(request.lamport_clock);
      logEvent('RECEBIMENTO', request);
      reqType = request.type;
      let replyPayload;
      if (reqType === 'CLOCK_REQ') {
        replyPayload = { status: 'OK', time: nowTime() };
      } else if (reqType === 'ELECTION_REQ') {
        replyPayload = { status: 'OK' };
        startNewElection = true;
      } else if (reqType === 'REPLICATE_REQ') {
        replyPayload = handleReplicate(request.payload || {});
      } else if (reqType === 'SYNC_REQ') {
        replyPayload = handleSync(request.payload || {});
      } else {
        replyPayload = { status: 'ERROR', msg: 'Requisicao desconhecida' };
      }
      const response = {
        type: `${reqType}_REP`,
        sender: SERVER_ID,
        timestamp: nowTs(),
        lamport_clock: incrementLamport(),
        payload: replyPayload
      };
      logEvent('ENVIO', response);
      await sock.send(encode(response));
    } catch (err) {
      console.error(`[${SERVER_ID}] Erro no atendimento de servidor:`, err.message);
    }
    if (startNewElection) startElection();
  }
}

// Assina o tópico 'servers' e guarda o nome do coordenador eleito
async function runServersListener() {
  const sub = new zmq.Subscriber();
  sub.connect(PROXY_SUB_ADDR);
  sub.subscribe(SERVERS_TOPIC);
  for await (const [topic, buf] of sub) {
    const msg = decode(buf);
    updateLamport(msg.lamport_clock);
    logEvent('RECEBIMENTO', { ...msg, type: `PUB [${topic.toString()}]` });
    coordinator = msg.payload?.coordinator;
    console.log(`[${SERVER_ID}] Coordenador atual: ${coordinator}`);
  }
}

async function runServer() {
  const repSocket = new zmq.Reply();
  repSocket.connect(BROKER_ADDR);

  const pubSocket = new zmq.Publisher();
  pubSocket.connect(PROXY_PUB_ADDR);

  refSocket.connect(REF_ADDR);

  // Registro de Rank
  await new Promise(res => setTimeout(res, 2000));
  const initClock = incrementLamport();
  const rankRep = await refCall({
    type: 'REGISTER_RANK_REQ',
    sender: SERVER_ID,
    timestamp: nowTs(),
    lamport_clock: initClock,
    payload: { name: SERVER_ID }
  });
  updateLamport(rankRep.lamport_clock);
  serverRank = rankRep.payload?.rank;
  console.log(`[${SERVER_ID}] Servidor Node.js ativo. Rank atribuido: ${serverRank}`);

  // Parte 4: canais servidor<->servidor, assinatura do tópico 'servers' e eleição inicial
  announceSocket.connect(PROXY_PUB_ADDR);
  runPeerServer().catch(console.error);
  runServersListener().catch(console.error);
  runSyncLoop().catch(console.error);
  await new Promise(res => setTimeout(res, 1000)); // assinatura propagar pelo proxy
  startElection();

  for await (const [msgBuf] of repSocket) {
    try {
      const request = decode(msgBuf);
      updateLamport(request.lamport_clock);
      logEvent('RECEBIMENTO', request);

      const reqType = request.type;
      const sender = request.sender;
      const payload = request.payload || {};
      let replyPayload = {};

      if (reqType === 'LOGIN_REQ') {
        const user = payload.user;
        if (user) {
          const loginRec = { id: crypto.randomUUID(), user, timestamp: request.timestamp };
          addLogin(loginRec);
          replicate('login', loginRec);
          replyPayload = { status: 'OK', msg: `Bem-vindo ${user}` };
        } else {
          replyPayload = { status: 'ERROR', msg: 'Usuario invalido' };
        }

      } else if (reqType === 'CREATE_CHANNEL_REQ') {
        const chName = payload.channel;
        if (chName && !channels.includes(chName)) {
          addChannel(chName);
          replicate('channel', chName);
          replyPayload = { status: 'OK', msg: `Canal '${chName}' criado` };
        } else {
          replyPayload = { status: 'ERROR', msg: 'Canal invalido ou existente' };
        }

      } else if (reqType === 'LIST_CHANNELS_REQ') {
        replyPayload = { status: 'OK', channels };

      } else if (reqType === 'PUB_MSG_REQ') {
        const targetChannel = payload.channel;
        const msgText = payload.message;

        if (targetChannel && msgText) {
          const msgRecord = {
            id: crypto.randomUUID(),
            channel: targetChannel,
            sender,
            text: msgText,
            sent_timestamp: request.timestamp,
            server_timestamp: nowTs(),
            lamport_clock: lamportClock
          };
          addMessage(msgRecord);
          replicate('message', msgRecord);

          const pubClock = incrementLamport();
          const pubPacket = {
            channel: targetChannel,
            sender,
            text: msgText,
            timestamp_sent: request.timestamp,
            lamport_clock: pubClock
          };
          await pubSocket.send([targetChannel, encode(pubPacket)]);

          replyPayload = { status: 'OK', msg: 'Mensagem publicada' };
        } else {
          replyPayload = { status: 'ERROR', msg: 'Dados de publicacao invalidos' };
        }

      } else {
        replyPayload = { status: 'ERROR', msg: 'Requisicao desconhecida' };
      }

      msgCounter += 1;
      if (msgCounter >= 10) {
        await sendHeartbeat();
        msgCounter = 0;
      }

      const sendClock = incrementLamport();
      const response = {
        type: `${reqType}_REP`,
        sender: SERVER_ID,
        timestamp: nowTs(),
        lamport_clock: sendClock,
        payload: replyPayload
      };

      logEvent('ENVIO', response);
      await repSocket.send(encode(response));

      // Parte 4: a cada 15 mensagens, sincroniza o relógio com o coordenador
      // (depois do reply para não atrasar o cliente caso ocorra uma eleição)
      syncCounter += 1;
      if (syncCounter >= SYNC_EVERY) {
        syncCounter = 0;
        await syncClock();
      }

    } catch (err) {
      console.error(`[${SERVER_ID}] Erro no processamento:`, err.message);
    }
  }
}

runServer().catch(console.error);
