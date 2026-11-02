const zmq = require('zeromq');
const { pack, unpack } = require('@msgpack/msgpack');

const BOT_ID = process.env.BOT_ID || 'bot_node_1';
const BROKER_ADDR = process.env.BROKER_ADDR || 'tcp://broker:5555';

const sleep = ms => new Promise(res => setTimeout(res, ms));

function logEvent(action, msgData) {
  const timeStr = new Date().toISOString().split('T')[1].slice(0, 8);
  console.log('='.repeat(60));
  console.log(`[${timeStr}] [${BOT_ID}] [${action}]`);
  console.log(`Tipo: ${msgData.type}`);
  console.log(`Origem: ${msgData.sender}`);
  console.log(`Timestamp: ${msgData.timestamp}`);
  console.log(`Payload: ${JSON.stringify(msgData.payload)}`);
  console.log('='.repeat(60));
}

async function runBot() {
  const socket = new zmq.Request();
  socket.connect(BROKER_ADDR);

  async function sendRequest(reqType, payload) {
    const msg = {
      type: reqType,
      sender: BOT_ID,
      timestamp: Math.floor(Date.now() / 1000),
      payload
    };

    logEvent('ENVIO', msg);
    await socket.send(pack(msg));

    const [replyBuf] = await socket.receive();
    const reply = unpack(replyBuf);
    logEvent('RECEBIMENTO', reply);
    return reply;
  }

  await sleep(2000);

  // 1. Login
  let loginRep = await sendRequest('LOGIN_REQ', { user: BOT_ID });
  while (loginRep.payload?.status !== 'OK') {
    await sleep(2000);
    loginRep = await sendRequest('LOGIN_REQ', { user: BOT_ID });
  }

  await sleep(1000);

  // 2. Listar canais
  await sendRequest('LIST_CHANNELS_REQ', {});
  await sleep(1000);

  // 3. Criar canal
  const newChannel = `canal-${BOT_ID}`;
  await sendRequest('CREATE_CHANNEL_REQ', { channel: newChannel });
  await sleep(1000);

  // 4. Listar canais novamente
  await sendRequest('LIST_CHANNELS_REQ', {});

  console.log(`[${BOT_ID}] Parte 1 executada com sucesso.`);
  while (true) {
    await sleep(10000);
  }
}

runBot().catch(console.error);