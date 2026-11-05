const zmq = require('zeromq');
const { encode, decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');

const SERVER_ID = process.env.SERVER_ID || 'server_node_1';
const BROKER_ADDR = process.env.BROKER_ADDR || 'tcp://broker:5556';
const PROXY_PUB_ADDR = process.env.PROXY_PUB_ADDR || 'tcp://proxy:5557';
const REF_ADDR = process.env.REF_ADDR || 'tcp://reference:5559';
const DATA_DIR = process.env.DATA_DIR || '/app/data';

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

let lamportClock = 0;
let msgCounter = 0;
let serverRank = null;

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
  console.log(`Payload: ${JSON.stringify(msgData.payload)}`);
  console.log('='.repeat(60));
}

async function sendHeartbeat(refSocket) {
  const clock = incrementLamport();
  const msg = {
    type: 'HEARTBEAT_REQ',
    sender: SERVER_ID,
    timestamp: Math.floor(Date.now() / 1000),
    lamport_clock: clock,
    payload: { name: SERVER_ID }
  };
  await refSocket.send(encode(msg));
  const [repBuf] = await refSocket.receive();
  const rep = decode(repBuf);
  updateLamport(rep.lamport_clock);
  console.log(`[${SERVER_ID}] Heartbeat enviado. Hora Fisica: ${rep.payload?.physical_time}`);
}

async function runServer() {
  const repSocket = new zmq.Reply();
  repSocket.connect(BROKER_ADDR);

  const pubSocket = new zmq.Publisher();
  pubSocket.connect(PROXY_PUB_ADDR);

  const refSocket = new zmq.Request();
  refSocket.connect(REF_ADDR);

  // Registro de Rank
  await new Promise(res => setTimeout(res, 2000));
  const initClock = incrementLamport();
  await refSocket.send(encode({
    type: 'REGISTER_RANK_REQ',
    sender: SERVER_ID,
    timestamp: Math.floor(Date.now() / 1000),
    lamport_clock: initClock,
    payload: { name: SERVER_ID }
  }));
  const [rankBuf] = await refSocket.receive();
  const rankRep = decode(rankBuf);
  updateLamport(rankRep.lamport_clock);
  serverRank = rankRep.payload?.rank;
  console.log(`[${SERVER_ID}] Servidor Node.js ativo. Rank atribuido: ${serverRank}`);

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
          logins.push({ user, timestamp: request.timestamp });
          saveJson(LOGINS_FILE, logins);
          replyPayload = { status: 'OK', msg: `Bem-vindo ${user}` };
        } else {
          replyPayload = { status: 'ERROR', msg: 'Usuario invalido' };
        }

      } else if (reqType === 'CREATE_CHANNEL_REQ') {
        const chName = payload.channel;
        if (chName && !channels.includes(chName)) {
          channels.push(chName);
          saveJson(CHANNELS_FILE, channels);
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
            channel: targetChannel,
            sender,
            text: msgText,
            sent_timestamp: request.timestamp,
            server_timestamp: Math.floor(Date.now() / 1000),
            lamport_clock: lamportClock
          };
          messages.push(msgRecord);
          saveJson(MESSAGES_FILE, messages);

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
        await sendHeartbeat(refSocket);
        msgCounter = 0;
      }

      const sendClock = incrementLamport();
      const response = {
        type: `${reqType}_REP`,
        sender: SERVER_ID,
        timestamp: Math.floor(Date.now() / 1000),
        lamport_clock: sendClock,
        payload: replyPayload
      };

      logEvent('ENVIO', response);
      await repSocket.send(encode(response));

    } catch (err) {
      console.error(`[${SERVER_ID}] Erro no processamento:`, err.message);
    }
  }
}

runServer().catch(console.error);