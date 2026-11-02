const zmq = require('zeromq');
const { encode, decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');

const SERVER_ID = process.env.SERVER_ID || 'server_node_1';
const BROKER_ADDR = process.env.BROKER_ADDR || 'tcp://broker:5556';
const PROXY_PUB_ADDR = process.env.PROXY_PUB_ADDR || 'tcp://proxy:5557';
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
  const repSocket = new zmq.Reply();
  repSocket.connect(BROKER_ADDR);

  const pubSocket = new zmq.Publisher();
  pubSocket.connect(PROXY_PUB_ADDR);

  console.log(`[${SERVER_ID}] Servidor Node.js ativo. Broker: ${BROKER_ADDR} | Proxy PUB: ${PROXY_PUB_ADDR}`);

  for await (const [msgBuf] of repSocket) {
    try {
      const request = decode(msgBuf);
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
            server_timestamp: Math.floor(Date.now() / 1000)
          };
          messages.push(msgRecord);
          saveJson(MESSAGES_FILE, messages);

          const pubPacket = {
            channel: targetChannel,
            sender,
            text: msgText,
            timestamp_sent: request.timestamp
          };
          await pubSocket.send([targetChannel, encode(pubPacket)]);

          replyPayload = { status: 'OK', msg: 'Mensagem publicada' };
        } else {
          replyPayload = { status: 'ERROR', msg: 'Dados de publicacao invalidos' };
        }

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
      await repSocket.send(encode(response));

    } catch (err) {
      console.error(`[${SERVER_ID}] Erro no processamento:`, err.message);
    }
  }
}

runServer().catch(console.error);