const zmq = require('zeromq');
const { encode, decode } = require('@msgpack/msgpack');

const BOT_ID = process.env.BOT_ID || 'radar_transito_01';
const BROKER_ADDR = process.env.BROKER_ADDR || 'tcp://broker:5555';
const PROXY_SUB_ADDR = process.env.PROXY_SUB_ADDR || 'tcp://proxy:5558';

const subscribedChannels = new Set();
let lamportClock = 0;

function updateLamport(recvClock) {
  lamportClock = Math.max(lamportClock, recvClock || 0) + 1;
}

function incrementLamport() {
  lamportClock += 1;
  return lamportClock;
}

const sleep = ms => new Promise(res => setTimeout(res, ms));

function logEvent(action, msgData) {
  const timeStr = new Date().toISOString().split('T')[1].slice(0, 8);
  console.log('='.repeat(60));
  console.log(`[${timeStr}] [${BOT_ID}] [${action}]`);
  console.log(`Tipo: ${msgData.type || msgData.channel}`);
  console.log(`Origem: ${msgData.sender}`);
  console.log(`Relogio Logico (Lamport): ${msgData.lamport_clock}`);
  console.log(`Timestamp: ${msgData.timestamp || msgData.timestamp_sent}`);
  console.log(`Payload: ${JSON.stringify(msgData.payload || msgData.text)}`);
  console.log('='.repeat(60));
}

async function startSubListener(subSocket) {
  for await (const [topicBuf, msgBuf] of subSocket) {
    try {
      const channel = topicBuf.toString();
      const msg = decode(msgBuf);
      updateLamport(msg.lamport_clock);
      const recvTime = Math.floor(Date.now() / 1000);
      const timeStr = new Date().toISOString().split('T')[1].slice(0, 8);

      console.log('*'.repeat(60));
      console.log(`[${timeStr}] [${BOT_ID}] [PUB_RECEIVED]`);
      console.log(`Canal: ${channel}`);
      console.log(`Autor: ${msg.sender}`);
      console.log(`Texto: ${msg.text}`);
      console.log(`Lamport Clock: ${lamportClock}`);
      console.log(`Timestamp Envio: ${msg.timestamp_sent}`);
      console.log(`Timestamp Recebimento: ${recvTime}`);
      console.log('*'.repeat(60));
    } catch (err) {
      console.error(`[${BOT_ID}] Erro SUB:`, err.message);
    }
  }
}

async function runBot() {
  const reqSocket = new zmq.Request();
  reqSocket.connect(BROKER_ADDR);

  const subSocket = new zmq.Subscriber();
  subSocket.connect(PROXY_SUB_ADDR);

  startSubListener(subSocket).catch(console.error);

  async function sendRequest(reqType, payload) {
    const clock = incrementLamport();
    const msg = {
      type: reqType,
      sender: BOT_ID,
      timestamp: Math.floor(Date.now() / 1000),
      lamport_clock: clock,
      payload
    };
    logEvent('ENVIO', msg);
    await reqSocket.send(encode(msg));
    const [replyBuf] = await reqSocket.receive();
    const reply = decode(replyBuf);
    updateLamport(reply.lamport_clock);
    logEvent('RECEBIMENTO', reply);
    return reply;
  }

  await sleep(2000);
  await sendRequest('LOGIN_REQ', { user: BOT_ID });
  await sleep(1000);

  while (true) {
    const rep = await sendRequest('LIST_CHANNELS_REQ', {});
    const availableChannels = rep.payload?.channels || [];

    if (availableChannels.length < 5) {
      const newCh = `canal-${BOT_ID}-${Math.floor(Math.random() * 900) + 100}`;
      await sendRequest('CREATE_CHANNEL_REQ', { channel: newCh });
      await sleep(1000);
    }

    for (const ch of availableChannels) {
      if (subscribedChannels.size >= 3) break;
      if (!subscribedChannels.has(ch)) {
        subSocket.subscribe(ch);
        subscribedChannels.add(ch);
        console.log(`[${BOT_ID}] Inscrito no canal: ${ch}`);
      }
    }

    if (availableChannels.length > 0) {
      const targetCh = availableChannels[Math.floor(Math.random() * availableChannels.length)];
      console.log(`[${BOT_ID}] Disparando 10 mensagens no canal '${targetCh}'...`);

      for (let i = 1; i <= 10; i++) {
        const speed = Math.floor(Math.random() * 40) + 50;
        const payloadText = `Metrica #${i}: Veiculo a ${speed} km/h`;

        await sendRequest('PUB_MSG_REQ', {
          channel: targetCh,
          message: payloadText
        });
        await sleep(1000);
      }
    }

    await sleep(3000);
  }
}

runBot().catch(console.error);