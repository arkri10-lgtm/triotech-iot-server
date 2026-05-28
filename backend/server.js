import Fastify from "fastify";
import websocket from "@fastify/websocket";
import mqtt from "mqtt";
import crypto from "node:crypto";

const app = Fastify({ logger: true });

await app.register(websocket);

const PORT = Number(process.env.PORT || 3000);
const API_TOKEN = process.env.API_TOKEN || "";
const DEVICE_OFFLINE_GRACE_MS = Number(process.env.DEVICE_OFFLINE_GRACE_MS || 120000);

const mqttClient = mqtt.connect(process.env.MQTT_URL || "mqtt://emqx:1883", {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  reconnectPeriod: 2000
});

const latestState = {};
const devices = {};
const alarmLog = [];
const wsClients = new Set();
const dashboardSessions = new Map();

const dashboardHtml = String.raw`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Snjallhus Device Overview</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f8fb;
      --panel: #ffffff;
      --line: #d7dde7;
      --text: #172033;
      --muted: #647086;
      --alarm: #ffe1e1;
      --alarm-line: #cf3030;
      --ok: #157347;
      --warn: #a15c00;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: Arial, Helvetica, sans-serif;
      font-size: 14px;
    }

    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 22px;
      background: var(--panel);
      border-bottom: 1px solid var(--line);
    }

    h1 {
      margin: 0;
      font-size: 20px;
      font-weight: 700;
    }

    .toolbar {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }

    input {
      width: min(420px, 70vw);
      height: 34px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 0 10px;
      font: inherit;
    }

    button {
      height: 34px;
      border: 1px solid #1f6feb;
      border-radius: 6px;
      background: #1f6feb;
      color: #fff;
      padding: 0 12px;
      font: inherit;
      cursor: pointer;
    }

    main {
      padding: 18px 22px 28px;
    }

    .status {
      display: flex;
      gap: 16px;
      flex-wrap: wrap;
      margin-bottom: 14px;
      color: var(--muted);
    }

    .status strong {
      color: var(--text);
    }

    .table-wrap {
      overflow: auto;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1180px;
    }

    th,
    td {
      border-bottom: 1px solid var(--line);
      padding: 9px 10px;
      text-align: left;
      white-space: nowrap;
    }

    th {
      position: sticky;
      top: 0;
      background: #eef2f7;
      font-size: 12px;
      color: #3d4a5f;
      text-transform: uppercase;
      letter-spacing: 0;
    }

    tr.alarm {
      background: var(--alarm);
    }

    tr.alarm td:first-child {
      border-left: 4px solid var(--alarm-line);
    }

    tr.offline {
      background: #fff8df;
    }

    tr.offline td:first-child {
      border-left: 4px solid #d98c00;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 2px 8px;
      border-radius: 999px;
      background: #edf2f7;
      color: var(--text);
      font-size: 12px;
      font-weight: 700;
    }

    .badge.ok {
      background: #ddf4e8;
      color: var(--ok);
    }

    .badge.alarm {
      background: #ffd6d6;
      color: #a80000;
    }

    .badge.warn {
      background: #fff1cc;
      color: var(--warn);
    }

    section {
      margin-top: 18px;
    }

    h2 {
      margin: 0 0 10px;
      font-size: 16px;
    }

    .empty {
      padding: 16px;
      color: var(--muted);
    }
  </style>
</head>
<body>
  <header>
    <h1>Snjallhus Device Overview</h1>
    <div class="toolbar">
      <input id="tokenInput" type="password" autocomplete="off" placeholder="API token">
      <button id="saveToken">Save</button>
      <button id="refresh">Refresh</button>
    </div>
  </header>

  <main>
    <div class="status">
      <span>API: <strong id="apiStatus">waiting</strong></span>
      <span>WebSocket: <strong id="wsStatus">waiting</strong></span>
      <span>Devices: <strong id="deviceCount">0</strong></span>
      <span>Last update: <strong id="lastUpdate">never</strong></span>
    </div>

    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Device ID</th>
            <th>Status</th>
            <th>Last seen</th>
            <th>Temp</th>
            <th>Humidity</th>
            <th>Power</th>
            <th>BLE installed</th>
            <th>BLE connection</th>
            <th>Low temp</th>
            <th>High temp</th>
            <th>High humidity</th>
            <th>Interval</th>
            <th>Alarm</th>
          </tr>
        </thead>
        <tbody id="deviceRows">
          <tr><td colspan="13" class="empty">No device data loaded.</td></tr>
        </tbody>
      </table>
    </div>

    <section>
      <h2>Latest Alarms</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Device ID</th>
              <th>Alarm</th>
              <th>Payload</th>
              <th>Topic</th>
            </tr>
          </thead>
          <tbody id="alarmRows">
            <tr><td colspan="5" class="empty">No active alarm events received.</td></tr>
          </tbody>
        </table>
      </div>
    </section>
  </main>

  <script>
    const tokenInput = document.getElementById("tokenInput");
    const saveToken = document.getElementById("saveToken");
    const refresh = document.getElementById("refresh");
    const apiStatus = document.getElementById("apiStatus");
    const wsStatus = document.getElementById("wsStatus");
    const deviceCount = document.getElementById("deviceCount");
    const lastUpdate = document.getElementById("lastUpdate");
    const deviceRows = document.getElementById("deviceRows");
    const alarmRows = document.getElementById("alarmRows");

    let ws = null;
    tokenInput.value = localStorage.getItem("snjallhus_api_token") || "";

    function token() {
      return localStorage.getItem("snjallhus_api_token") || "";
    }

    function authHeaders() {
      return { Authorization: "Bearer " + token() };
    }

    function fmt(value, suffix) {
      if (value === null || value === undefined || value === "") return "";
      return String(value) + (suffix || "");
    }

    function fmtTime(value) {
      if (!value) return "";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString();
    }

    function fmtAge(seconds) {
      if (seconds === null || seconds === undefined) return "";
      if (seconds < 60) return seconds + "s ago";
      const minutes = Math.floor(seconds / 60);
      if (minutes < 60) return minutes + "m ago";
      const hours = Math.floor(minutes / 60);
      return hours + "h ago";
    }

    function badge(value, kind) {
      const span = document.createElement("span");
      span.className = "badge " + (kind || "");
      span.textContent = value || "";
      return span;
    }

    function cell(row, value) {
      const td = document.createElement("td");
      if (value instanceof Node) {
        td.appendChild(value);
      } else {
        td.textContent = value || "";
      }
      row.appendChild(td);
    }

    function renderDevices(devices) {
      deviceRows.textContent = "";
      deviceCount.textContent = String(devices.length);

      if (!devices.length) {
        const row = document.createElement("tr");
        cell(row, "No devices received yet.");
        row.firstChild.colSpan = 13;
        row.firstChild.className = "empty";
        deviceRows.appendChild(row);
        return;
      }

      for (const device of devices) {
        const row = document.createElement("tr");
        if (device.alarm_state === "ALARM") {
          row.className = "alarm";
        } else if (device.is_offline) {
          row.className = "offline";
        }

        const status = device.connection_state || device.status;
        const statusKind = device.is_offline ? "warn" : (status === "online" ? "ok" : "warn");

        cell(row, device.device_id);
        cell(row, badge(status, statusKind));
        cell(row, fmtTime(device.last_seen) + (device.seconds_since_seen !== null ? " (" + fmtAge(device.seconds_since_seen) + ")" : ""));
        cell(row, fmt(device.temperature, " C"));
        cell(row, fmt(device.humidity, " %"));
        cell(row, device.power_source);
        cell(row, device.ble_power_monitor_installed === true ? "yes" : (device.ble_power_monitor_installed === false ? "no" : ""));
        cell(row, device.ble_power_monitor_connection || "");
        cell(row, fmt(device.low_temperature, " C"));
        cell(row, fmt(device.high_temperature, " C"));
        cell(row, fmt(device.high_humidity, " %"));
        cell(row, fmt(device.telemetry_interval_sec, " s"));
        cell(row, device.alarm_state ? badge(device.alarm_state, device.alarm_state === "ALARM" ? "alarm" : "ok") : "");
        deviceRows.appendChild(row);
      }
    }

    function renderAlarms(alarms) {
      alarmRows.textContent = "";

      if (!alarms.length) {
        const row = document.createElement("tr");
        cell(row, "No active alarm events received.");
        row.firstChild.colSpan = 5;
        row.firstChild.className = "empty";
        alarmRows.appendChild(row);
        return;
      }

      for (const alarm of alarms.slice(0, 100)) {
        const row = document.createElement("tr");
        cell(row, fmtTime(alarm.created_at));
        cell(row, alarm.device_id);
        cell(row, alarm.alarm_type);
        cell(row, alarm.payload);
        cell(row, alarm.source_topic);
        alarmRows.appendChild(row);
      }
    }

    function renderState(state) {
      renderDevices(state.devices || []);
      renderAlarms(state.alarms || []);
      lastUpdate.textContent = new Date().toLocaleTimeString();
    }

    async function loadState() {
      if (!token()) {
        apiStatus.textContent = "missing token";
        return;
      }

      try {
        const response = await fetch("/api/v1/state", { headers: authHeaders() });
        if (!response.ok) throw new Error("HTTP " + response.status);
        const data = await response.json();
        apiStatus.textContent = "connected";
        renderState(data);
      } catch (error) {
        apiStatus.textContent = error.message;
      }
    }

    async function createSession() {
      const response = await fetch("/api/v1/session", {
        method: "POST",
        headers: authHeaders()
      });

      if (!response.ok) throw new Error("Session HTTP " + response.status);
    }

    function connectWs() {
      if (ws) ws.close();

      if (!token()) {
        wsStatus.textContent = "missing token";
        return;
      }

      const scheme = location.protocol === "https:" ? "wss" : "ws";
      const url = scheme + "://" + location.host + "/api/v1/ws";
      ws = new WebSocket(url);

      ws.addEventListener("open", () => {
        wsStatus.textContent = "connected";
      });

      ws.addEventListener("message", (event) => {
        const message = JSON.parse(event.data);
        if (message.type === "state") {
          renderState(message.data || {});
        }
      });

      ws.addEventListener("close", () => {
        wsStatus.textContent = "disconnected";
        setTimeout(connectWs, 3000);
      });

      ws.addEventListener("error", () => {
        wsStatus.textContent = "error";
      });
    }

    saveToken.addEventListener("click", () => {
      localStorage.setItem("snjallhus_api_token", tokenInput.value.trim());
      createSession()
        .then(() => {
          loadState();
          connectWs();
        })
        .catch((error) => {
          apiStatus.textContent = error.message;
          wsStatus.textContent = "not connected";
        });
    });

    refresh.addEventListener("click", loadState);

    if (token()) {
      createSession()
        .then(() => {
          loadState();
          connectWs();
        })
        .catch(() => {
          loadState();
          wsStatus.textContent = "press save";
        });
    } else {
      apiStatus.textContent = "enter token";
      wsStatus.textContent = "enter token";
    }
  </script>
</body>
</html>`;

function isAuthorizedToken(value) {
  return Boolean(API_TOKEN) && value === API_TOKEN;
}

function createDashboardSession() {
  const sessionId = crypto.randomUUID();
  dashboardSessions.set(sessionId, Date.now() + 12 * 60 * 60 * 1000);
  return sessionId;
}

function isAuthorizedSession(sessionId) {
  const expiresAt = dashboardSessions.get(sessionId);

  if (!expiresAt) {
    return false;
  }

  if (Date.now() > expiresAt) {
    dashboardSessions.delete(sessionId);
    return false;
  }

  return true;
}

function parseCookies(cookieHeader = "") {
  const cookies = {};

  for (const part of cookieHeader.split(";")) {
    const [name, ...valueParts] = part.trim().split("=");
    if (!name) continue;
    cookies[name] = decodeURIComponent(valueParts.join("="));
  }

  return cookies;
}

function checkAuth(req, reply, done) {
  const auth = req.headers.authorization || "";
  const token = auth.replace(/^Bearer\s+/i, "");
  if (!isAuthorizedToken(token)) {
    reply.code(401).send({ error: "Unauthorized" });
    return;
  }
  done();
}

function numberOrNull(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function getDevice(deviceId) {
  if (!devices[deviceId]) {
    devices[deviceId] = {
      device_id: deviceId,
      status: "unknown",
      last_seen: null,
      temperature: null,
      humidity: null,
      power_source: null,
      ble_power_monitor_installed: false,
      ble_power_monitor_connection: null,
      telemetry_interval_sec: null,
      low_temperature: null,
      high_temperature: null,
      high_humidity: null,
      alarm_state: "OK",
      alarms: {}
    };
  }

  return devices[deviceId];
}

function getDeviceView(device, now = Date.now()) {
  const lastSeenMs = device.last_seen ? Date.parse(device.last_seen) : NaN;
  const hasLastSeen = Number.isFinite(lastSeenMs);
  const secondsSinceSeen = hasLastSeen ? Math.max(0, Math.floor((now - lastSeenMs) / 1000)) : null;
  const telemetryIntervalMs = Number.isFinite(device.telemetry_interval_sec)
    ? Math.max(0, device.telemetry_interval_sec * 1000)
    : 0;
  const offlineAfterMs = telemetryIntervalMs + DEVICE_OFFLINE_GRACE_MS;
  const isOffline = !hasLastSeen || now - lastSeenMs > offlineAfterMs;

  if (isOffline) {
    return {
      ...device,
      status: "offline",
      connection_state: "offline",
      is_offline: true,
      seconds_since_seen: secondsSinceSeen,
      offline_after_seconds: Math.floor(offlineAfterMs / 1000),
      temperature: null,
      humidity: null,
      power_source: "",
      ble_power_monitor_installed: null,
      ble_power_monitor_connection: "",
      telemetry_interval_sec: null,
      low_temperature: null,
      high_temperature: null,
      high_humidity: null,
      alarm_state: "",
      alarms: {}
    };
  }

  return {
    ...device,
    connection_state: device.status,
    is_offline: false,
    seconds_since_seen: secondsSinceSeen,
    offline_after_seconds: Math.floor(offlineAfterMs / 1000)
  };
}

function getDashboardState() {
  const now = Date.now();

  return {
    raw: latestState,
    devices: Object.values(devices).map((device) => getDeviceView(device, now)),
    alarms: alarmLog
  };
}

function addAlarmEvent(deviceId, alarmType, payload, topic) {
  alarmLog.unshift({
    device_id: deviceId,
    alarm_type: alarmType,
    payload,
    source_topic: topic,
    created_at: new Date().toISOString()
  });

  if (alarmLog.length > 100) {
    alarmLog.length = 100;
  }
}

function publishWebSocketState() {
  const message = JSON.stringify({
    type: "state",
    data: getDashboardState()
  });

  for (const socket of wsClients) {
    if (socket.readyState === 1) {
      socket.send(message);
    }
  }
}

function handleSnjalliMessage(topic, payloadText) {
  const parts = topic.split("/");

  if (parts.length < 3 || parts[0] !== "snjalli") {
    return;
  }

  const deviceId = parts[1];
  const group = parts[2];
  const tag = parts.slice(3).join("/");
  const device = getDevice(deviceId);

  device.last_seen = new Date().toISOString();

  if (group === "status") {
    device.status = payloadText;
  }

  if (group === "tele" && tag === "temperature") {
    device.temperature = numberOrNull(payloadText);
  }

  if (group === "tele" && tag === "humidity") {
    device.humidity = numberOrNull(payloadText);
  }

  if (group === "power" && tag === "source") {
    device.power_source = payloadText;
  }

  if (group === "ble" && tag === "power_monitor_installed") {
    device.ble_power_monitor_installed = payloadText === "yes";
  }

  if (group === "ble" && tag === "power_monitor_connection") {
    device.ble_power_monitor_connection = payloadText;
  }

  if (group === "state" && tag === "telemetry_interval_sec") {
    device.telemetry_interval_sec = numberOrNull(payloadText);
  }

  if (group === "state" && tag === "low_temperature") {
    device.low_temperature = numberOrNull(payloadText);
  }

  if (group === "state" && tag === "high_temperature") {
    device.high_temperature = numberOrNull(payloadText);
  }

  if (group === "state" && tag === "high_humidity") {
    device.high_humidity = numberOrNull(payloadText);
  }

  if (group === "alarm" && tag === "state") {
    device.alarm_state = payloadText;
  }

  if (group === "alarm" && tag && tag !== "state") {
    device.alarms[tag] = payloadText;

    if (payloadText === "ACTIVE") {
      addAlarmEvent(deviceId, tag, payloadText, topic);
    }
  }
}

mqttClient.on("connect", () => {
  app.log.info("MQTT connected");

  mqttClient.subscribe([
    "hottub/#",
    "triotech/#",
    "alarm/#",
    "snjalli/#"
  ]);
});

mqttClient.on("message", (topic, payload) => {
  const payloadText = payload.toString();

  latestState[topic] = payloadText;

  if (topic.startsWith("snjalli/")) {
    handleSnjalliMessage(topic, payloadText);
  }

  publishWebSocketState();
});

app.get("/health", async () => {
  return {
    ok: true,
    mqttConnected: mqttClient.connected
  };
});

app.get("/", async (req, reply) => {
  return reply.type("text/html").send(dashboardHtml);
});

app.get("/dashboard", async (req, reply) => {
  return reply.type("text/html").send(dashboardHtml);
});

app.post("/api/v1/session", { preHandler: checkAuth }, async (req, reply) => {
  const sessionId = createDashboardSession();

  reply.header(
    "Set-Cookie",
    `snjallhus_session=${encodeURIComponent(sessionId)}; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200`
  );

  return { ok: true };
});

app.get("/api/v1/state", { preHandler: checkAuth }, async () => {
  return getDashboardState();
});

app.get("/api/v1/devices", { preHandler: checkAuth }, async () => {
  return getDashboardState().devices;
});

app.get("/api/v1/alarms", { preHandler: checkAuth }, async () => {
  return alarmLog;
});

app.post("/api/v1/mqtt/publish", { preHandler: checkAuth }, async (req) => {
  const { topic, message } = req.body || {};

  if (!topic || message === undefined) {
    return {
      ok: false,
      error: "Missing topic or message"
    };
  }

  mqttClient.publish(topic, String(message), { qos: 1 });

  return {
    ok: true,
    topic,
    message: String(message)
  };
});

app.get("/api/v1/ws", { websocket: true }, (socket, req) => {
  const cookies = parseCookies(req.headers.cookie || "");

  if (!isAuthorizedSession(cookies.snjallhus_session || "")) {
    socket.close(1008, "Unauthorized");
    return;
  }

  wsClients.add(socket);

  socket.send(JSON.stringify({
    type: "state",
    data: getDashboardState()
  }));

  socket.on("close", () => {
    wsClients.delete(socket);
  });
});

setInterval(publishWebSocketState, 10000);

app.listen({ port: PORT, host: "0.0.0.0" });
