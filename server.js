// =============================================================================
//  PONTE SICRO  -  HiveMQ Cloud (MQTT/TLS)  <-->  WebSocket  <-->  App Inventor
// -----------------------------------------------------------------------------
//  - Assina a telemetria das placas no HiveMQ e repassa aos apps via WebSocket
//  - Recebe comandos liga/desliga do app e publica no HiveMQ
//  - Serve a página (public/index.html) que o WebViewer do App Inventor carrega
//
//  Variáveis de ambiente (configurar no Render):
//    MQTT_HOST     -> ex.: xxxxxxxx.s1.eu.hivemq.cloud
//    MQTT_PORT     -> 8883 (padrão TLS do HiveMQ Cloud)
//    MQTT_USER     -> usuário MQTT do cluster
//    MQTT_PASS     -> senha MQTT do cluster
//    BRIDGE_TOKEN  -> (opcional) segredo compartilhado exigido dos apps
// =============================================================================

const http = require('http');
const fs   = require('fs');
const path = require('path');
const mqtt = require('mqtt');
const { WebSocketServer } = require('ws');

const PORT         = process.env.PORT || 3000;          // Render injeta PORT
const MQTT_HOST    = process.env.MQTT_HOST;
const MQTT_PORT    = process.env.MQTT_PORT || 8883;
const MQTT_USER    = process.env.MQTT_USER;
const MQTT_PASS    = process.env.MQTT_PASS;
const BRIDGE_TOKEN = process.env.BRIDGE_TOKEN || '';     // "" = sem autenticação

// -----------------------------------------------------------------------------
//  Conexão MQTT com o HiveMQ Cloud (TLS)
// -----------------------------------------------------------------------------
const mqttUrl = `mqtts://${MQTT_HOST}:${MQTT_PORT}`;
const mqttClient = mqtt.connect(mqttUrl, {
  username: MQTT_USER,
  password: MQTT_PASS,
  reconnectPeriod: 3000,
  clean: true,
});

mqttClient.on('connect', () => {
  console.log('[MQTT] conectado ao HiveMQ');
  // Telemetria consolidada (JSON) e presença de cada placa
  mqttClient.subscribe('sicro/+/telemetria');
  mqttClient.subscribe('sicro/+/online');
});
mqttClient.on('reconnect', () => console.log('[MQTT] reconectando...'));
mqttClient.on('error', (e) => console.error('[MQTT] erro:', e.message));

// -----------------------------------------------------------------------------
//  Servidor HTTP: serve a página do painel e um health check
// -----------------------------------------------------------------------------
const server = http.createServer((req, res) => {
  const urlPath = req.url.split('?')[0];

  if (urlPath === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }

  if (urlPath === '/' || urlPath === '/index.html') {
    const file = path.join(__dirname, 'public', 'index.html');
    fs.readFile(file, (err, data) => {
      if (err) { res.writeHead(500); res.end('erro ao ler index.html'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
});

// -----------------------------------------------------------------------------
//  Servidor WebSocket (mesma porta, caminho /ws)
// -----------------------------------------------------------------------------
const wss = new WebSocketServer({ server, path: '/ws' });

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  wss.clients.forEach((c) => { if (c.readyState === 1) c.send(msg); });
}

// MQTT -> WebSocket (telemetria/presença vão para todos os apps conectados)
mqttClient.on('message', (topic, payload) => {
  const parts = topic.split('/');     // sicro / <board> / <leaf>
  const board = parts[1] || '';
  const leaf  = parts[2] || '';
  const text  = payload.toString();

  if (leaf === 'telemetria') {
    let data;
    try { data = JSON.parse(text); } catch { data = {}; }
    broadcast({ type: 'telemetria', board, data });
  } else if (leaf === 'online') {
    broadcast({ type: 'online', board, online: text === '1' });
  }
});

// WebSocket -> MQTT (comandos liga/desliga vindos do app)
wss.on('connection', (ws, req) => {
  // Autenticação opcional por token na query string (?token=...)
  const url   = new URL(req.url, 'http://localhost');
  const token = url.searchParams.get('token') || '';
  if (BRIDGE_TOKEN && token !== BRIDGE_TOKEN) {
    ws.close(1008, 'token invalido');
    return;
  }

  console.log('[WS] app conectado');
  ws.send(JSON.stringify({ type: 'hello', msg: 'ponte SICRO conectada' }));

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }

    if (m.type === 'cmd' && m.board) {
      if (m.action === 'liga') {
        mqttClient.publish(`sicro/${m.board}/cmd/liga`, 'LIGA');
        console.log(`[WS->MQTT] ${m.board} LIGA`);
      } else if (m.action === 'desliga') {
        mqttClient.publish(`sicro/${m.board}/cmd/desliga`, 'DESLIGA');
        console.log(`[WS->MQTT] ${m.board} DESLIGA`);
      }
    }
  });

  ws.on('close', () => console.log('[WS] app desconectado'));
});

server.listen(PORT, () => console.log(`[HTTP] ponte ouvindo na porta ${PORT}`));
