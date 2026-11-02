const zmq = require('zeromq');
const { encode, decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');

const SERVER_ID = process.env.SERVER_ID || 'server_node_1';
const BROKER_ADDR = process.env.BROKER_ADDR || 'tcp://broker:5556';
const DATA_DIR = process.env.DATA_DIR || '/app/data';

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

const LOGINS_FILE = path.join(DATA_DIR, `${SERVER_ID}_logins.json`);
const CHANNELS_FILE = path.join(DATA_DIR, `${SERVER_ID}_channels.json`);

function loadData() {
  let logins = [];
  let channels = ['geral', 'telemetria'];

  if (fs.existsSync(LOGINS_FILE)) {
    logins = JSON.parse(fs.readFileSync(LOGINS_FILE, 'utf-8'));
  }
  if (fs.existsSync(CHANNELS_FILE)) {
    channels = JSON.parse(fs.readFileSync(CHANNELS_FILE, 'utf-8'));
  }
  return { logins, channels };
}

function saveLogins(data) {
  fs.writeFileSync(LOGINS_FILE, JSON.stringify(data, null, 2));
}

function saveChannels(data) {
  fs.writeFileSync(CHANNELS_FILE, JSON.stringify(data, null, 2));
}

const { logins, channels } = loadData();

function logEvent(action, msgData) {
  const timeStr = new Date().toISOString().split('T')[1].slice(0, 8);
  console.log('='.repeat(60));
  console.log(`[${timeStr}] [${SERVER_ID}] [${action}]`);
  console.log(`Tipo: ${msgData.type}`);
  console.log(`Origem: ${msgData.sender}`);
  console.log(`Timestamp Envio: ${msgData.timestamp}`);
  console.log(`Payload: ${JSON.stringify(msgData.payload)}`);
  console.log('='.repeat(60));
}

async function runServer() {
  const socket = new zmq.Reply();
  socket.connect(BROKER_ADDR);
  console.log(`[${SERVER_ID}] Servidor Node.js ativo em ${BROKER_ADDR}`);

  for await (const [msgBuf] of socket) {
    const request = decode(msgBuf);
    logEvent('RECEBIMENTO', request);

    const reqType = request.type;
    const payload = request.payload || {};
    let replyPayload = {};

    if (reqType === 'LOGIN_REQ') {
      const user = payload.user;
      if (!user) {
        replyPayload = { status: 'ERROR', msg: 'Usuario invalido' };
      } else {
        logins.push({ user, timestamp: request.timestamp });
        saveLogins(logins);
        replyPayload = { status: 'OK', msg: `Bem-vindo ${user}` };
      }
    } else if (reqType === 'CREATE_CHANNEL_REQ') {
      const channelName = payload.channel;
      if (!channelName) {
        replyPayload = { status: 'ERROR', msg: 'Nome de canal invalido' };
      } else if (channels.includes(channelName)) {
        replyPayload = { status: 'ERROR', msg: `Canal '${channelName}' ja existe` };
      } else {
        channels.push(channelName);
        saveChannels(channels);
        replyPayload = { status: 'OK', msg: `Canal '${channelName}' criado` };
      }
    } else if (reqType === 'LIST_CHANNELS_REQ') {
      replyPayload = { status: 'OK', channels };
    } else {
      replyPayload = { status: 'ERROR', msg: 'Requisicao desconhecida' };
    }

    const response = {
      type: `${reqType}_REP`,
      sender: SERVER_ID,
      timestamp: Math.floor(Date.now() / 1000),
      payload: replyPayload
    };

    logEvent('ENVIO', response);
    await socket.send(encode(response));
  }
}

runServer().catch(console.error);