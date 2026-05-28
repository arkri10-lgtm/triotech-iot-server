import Fastify from "fastify";
import websocket from "@fastify/websocket";
import mqtt from "mqtt";
import crypto from "node:crypto";
import pg from "pg";

const app = Fastify({ logger: true });
const { Pool } = pg;

await app.register(websocket);

const PORT = Number(process.env.PORT || 3000);
const API_TOKEN = process.env.API_TOKEN || "";
const DEVICE_OFFLINE_GRACE_MS = Number(process.env.DEVICE_OFFLINE_GRACE_MS || 120000);
const DATABASE_URL = process.env.DATABASE_URL || "";

const db = DATABASE_URL ? new Pool({ connectionString: DATABASE_URL }) : null;

const mqttClient = mqtt.connect(process.env.MQTT_URL || "mqtt://emqx:1883", {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  reconnectPeriod: 2000
});

const latestState = {};
const devices = {};
const deviceContacts = {};
const deviceSettings = {};
let recentAlarms = [];
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

    .row-input {
      width: 150px;
      height: 30px;
      padding: 0 8px;
      background: #fff;
    }

    .address-input {
      width: 260px;
    }

    .setting-input {
      width: 86px;
    }

    .save-row {
      height: 30px;
      min-width: 58px;
      padding: 0 10px;
    }

    .save-row.saved {
      border-color: var(--ok);
      background: var(--ok);
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
      min-width: 1760px;
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
            <th>Phone</th>
            <th>Address</th>
            <th>Save</th>
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
            <th>Save settings</th>
            <th>Alarm</th>
          </tr>
        </thead>
        <tbody id="deviceRows">
          <tr><td colspan="17" class="empty">No device data loaded.</td></tr>
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
              <th>Cleared</th>
              <th>Device ID</th>
              <th>Alarm</th>
              <th>Topic</th>
            </tr>
          </thead>
          <tbody id="alarmRows">
            <tr><td colspan="5" class="empty">No alarm history received.</td></tr>
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
    const pendingContactEdits = new Map();
    const pendingSettingEdits = new Map();
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
      return date.toLocaleString("en-GB", {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false
      });
    }

    function fmtClock(value) {
      return value.toLocaleTimeString("en-GB", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false
      });
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

    function contactInput(value, extraClass) {
      const input = document.createElement("input");
      input.className = "row-input " + (extraClass || "");
      input.value = value || "";
      return input;
    }

    function settingInput(value, activeValue, extraClass) {
      const input = document.createElement("input");
      input.className = "row-input setting-input " + (extraClass || "");
      input.type = "number";
      input.step = "0.1";
      input.value = value === null || value === undefined ? "" : String(value);
      input.title = "Active value: " + fmt(activeValue, "");
      return input;
    }

    function setPendingContact(deviceId, field, value) {
      const pending = pendingContactEdits.get(deviceId) || {};
      pending[field] = value;
      pendingContactEdits.set(deviceId, pending);
    }

    function setPendingSetting(deviceId, field, value) {
      const pending = pendingSettingEdits.get(deviceId) || {};
      pending[field] = value;
      pendingSettingEdits.set(deviceId, pending);
    }

    async function saveContact(deviceId, phoneInput, addressInput, button) {
      button.disabled = true;
      button.textContent = "Saving";
      button.classList.remove("saved");

      try {
        const response = await fetch("/api/v1/devices/" + encodeURIComponent(deviceId) + "/contact", {
          method: "PATCH",
          headers: {
            ...authHeaders(),
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            phone_number: phoneInput.value.trim(),
            address: addressInput.value.trim()
          })
        });

        if (!response.ok) throw new Error("HTTP " + response.status);

        pendingContactEdits.delete(deviceId);
        button.textContent = "Saved";
        button.classList.add("saved");
        setTimeout(() => {
          button.textContent = "Save";
          button.classList.remove("saved");
        }, 1200);
      } catch (error) {
        button.textContent = "Error";
        apiStatus.textContent = "contact save " + error.message;
      } finally {
        button.disabled = false;
      }
    }

    async function saveSettings(deviceId, lowInput, highInput, humidityInput, intervalInput, button) {
      button.disabled = true;
      button.textContent = "Saving";
      button.classList.remove("saved");

      try {
        const response = await fetch("/api/v1/devices/" + encodeURIComponent(deviceId) + "/settings", {
          method: "PATCH",
          headers: {
            ...authHeaders(),
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            low_temperature: lowInput.value,
            high_temperature: highInput.value,
            high_humidity: humidityInput.value,
            telemetry_interval_sec: intervalInput.value
          })
        });

        if (!response.ok) {
          const body = await response.json().catch(() => ({}));
          throw new Error(body.error || ("HTTP " + response.status));
        }

        pendingSettingEdits.delete(deviceId);
        button.textContent = "Saved";
        button.classList.add("saved");
        setTimeout(() => {
          button.textContent = "Save";
          button.classList.remove("saved");
        }, 1200);
      } catch (error) {
        button.textContent = "Error";
        apiStatus.textContent = "settings save " + error.message;
      } finally {
        button.disabled = false;
      }
    }

    function renderDevices(devices) {
      deviceRows.textContent = "";
      deviceCount.textContent = String(devices.length);

      if (!devices.length) {
        const row = document.createElement("tr");
        cell(row, "No devices received yet.");
        row.firstChild.colSpan = 17;
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
        const pendingContact = pendingContactEdits.get(device.device_id) || {};
        const phoneInput = contactInput(pendingContact.phone_number ?? device.phone_number, "phone-input");
        const addressInput = contactInput(pendingContact.address ?? device.address, "address-input");
        const saveButton = document.createElement("button");
        saveButton.className = "save-row";
        saveButton.textContent = "Save";
        phoneInput.addEventListener("input", () => setPendingContact(device.device_id, "phone_number", phoneInput.value));
        addressInput.addEventListener("input", () => setPendingContact(device.device_id, "address", addressInput.value));
        saveButton.addEventListener("click", () => saveContact(device.device_id, phoneInput, addressInput, saveButton));
        const pendingSettings = pendingSettingEdits.get(device.device_id) || {};
        const lowInput = settingInput(pendingSettings.low_temperature ?? device.desired_low_temperature ?? device.low_temperature, device.low_temperature, "low-temp-input");
        const highInput = settingInput(pendingSettings.high_temperature ?? device.desired_high_temperature ?? device.high_temperature, device.high_temperature, "high-temp-input");
        const humidityInput = settingInput(pendingSettings.high_humidity ?? device.desired_high_humidity ?? device.high_humidity, device.high_humidity, "high-humidity-input");
        const intervalInput = settingInput(pendingSettings.telemetry_interval_sec ?? device.desired_telemetry_interval_sec ?? device.telemetry_interval_sec, device.telemetry_interval_sec, "interval-input");
        intervalInput.step = "1";
        const settingsButton = document.createElement("button");
        settingsButton.className = "save-row";
        settingsButton.textContent = "Save";
        lowInput.addEventListener("input", () => setPendingSetting(device.device_id, "low_temperature", lowInput.value));
        highInput.addEventListener("input", () => setPendingSetting(device.device_id, "high_temperature", highInput.value));
        humidityInput.addEventListener("input", () => setPendingSetting(device.device_id, "high_humidity", humidityInput.value));
        intervalInput.addEventListener("input", () => setPendingSetting(device.device_id, "telemetry_interval_sec", intervalInput.value));
        settingsButton.addEventListener("click", () => saveSettings(device.device_id, lowInput, highInput, humidityInput, intervalInput, settingsButton));

        cell(row, device.device_id);
        cell(row, phoneInput);
        cell(row, addressInput);
        cell(row, saveButton);
        cell(row, badge(status, statusKind));
        cell(row, fmtTime(device.last_seen) + (device.seconds_since_seen !== null ? " (" + fmtAge(device.seconds_since_seen) + ")" : ""));
        cell(row, fmt(device.temperature, " C"));
        cell(row, fmt(device.humidity, " %"));
        cell(row, device.power_source);
        cell(row, device.ble_power_monitor_installed === true ? "yes" : (device.ble_power_monitor_installed === false ? "no" : ""));
        cell(row, device.ble_power_monitor_connection || "");
        cell(row, lowInput);
        cell(row, highInput);
        cell(row, humidityInput);
        cell(row, intervalInput);
        cell(row, settingsButton);
        cell(row, device.alarm_state ? badge(device.alarm_state, device.alarm_state === "ALARM" ? "alarm" : "ok") : "");
        deviceRows.appendChild(row);
      }
    }

    function renderAlarms(alarms) {
      alarmRows.textContent = "";

      if (!alarms.length) {
        const row = document.createElement("tr");
        cell(row, "No alarm history received.");
        row.firstChild.colSpan = 5;
        row.firstChild.className = "empty";
        alarmRows.appendChild(row);
        return;
      }

      for (const alarm of alarms.slice(0, 100)) {
        const row = document.createElement("tr");
        cell(row, fmtTime(alarm.created_at));
        cell(row, fmtTime(alarm.cleared_at));
        cell(row, alarm.device_id);
        cell(row, alarm.alarm_type);
        cell(row, alarm.source_topic);
        alarmRows.appendChild(row);
      }
    }

    function isEditingContact() {
      return document.activeElement && document.activeElement.classList.contains("row-input");
    }

    function renderState(state) {
      if (!isEditingContact()) {
        renderDevices(state.devices || []);
      }
      renderAlarms(state.alarms || []);
      lastUpdate.textContent = fmtClock(new Date());
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

function optionalNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function textLimit(value, maxLength) {
  return String(value || "").trim().slice(0, maxLength);
}

function contactForDevice(deviceId) {
  return deviceContacts[deviceId] || {
    phone_number: "",
    address: "",
    contact_updated_at: null
  };
}

function settingsForDevice(deviceId) {
  return deviceSettings[deviceId] || {
    desired_low_temperature: null,
    desired_high_temperature: null,
    desired_high_humidity: null,
    desired_telemetry_interval_sec: null,
    settings_updated_at: null
  };
}

function getDevice(deviceId) {
  if (!devices[deviceId]) {
    const contact = contactForDevice(deviceId);
    const settings = settingsForDevice(deviceId);

    devices[deviceId] = {
      device_id: deviceId,
      phone_number: contact.phone_number,
      address: contact.address,
      contact_updated_at: contact.contact_updated_at,
      desired_low_temperature: settings.desired_low_temperature,
      desired_high_temperature: settings.desired_high_temperature,
      desired_high_humidity: settings.desired_high_humidity,
      desired_telemetry_interval_sec: settings.desired_telemetry_interval_sec,
      settings_updated_at: settings.settings_updated_at,
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

function applyContactToDevice(deviceId, contact) {
  deviceContacts[deviceId] = contact;

  const device = getDevice(deviceId);
  device.phone_number = contact.phone_number;
  device.address = contact.address;
  device.contact_updated_at = contact.contact_updated_at;

  return device;
}

function applySettingsToDevice(deviceId, settings) {
  deviceSettings[deviceId] = settings;

  const device = getDevice(deviceId);
  device.desired_low_temperature = settings.desired_low_temperature;
  device.desired_high_temperature = settings.desired_high_temperature;
  device.desired_high_humidity = settings.desired_high_humidity;
  device.desired_telemetry_interval_sec = settings.desired_telemetry_interval_sec;
  device.settings_updated_at = settings.settings_updated_at;

  return device;
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
    alarms: recentAlarms
  };
}

async function initDatabase() {
  if (!db) {
    app.log.warn("DATABASE_URL is not configured; alarm history will stay in memory only");
    return;
  }

  await db.query(`
    CREATE TABLE IF NOT EXISTS device_alarm_log (
      id BIGSERIAL PRIMARY KEY,
      customer_id TEXT,
      device_id TEXT NOT NULL,
      alarm_type TEXT NOT NULL,
      alarm_value TEXT,
      payload TEXT NOT NULL,
      source_topic TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'ACTIVE',
      message TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      cleared_at TIMESTAMPTZ
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS device_contact_info (
      device_id TEXT PRIMARY KEY,
      phone_number TEXT NOT NULL DEFAULT '',
      address TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS device_desired_settings (
      device_id TEXT PRIMARY KEY,
      low_temperature NUMERIC,
      high_temperature NUMERIC,
      high_humidity NUMERIC,
      telemetry_interval_sec INTEGER,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS device_alarm_log_created_idx
    ON device_alarm_log (created_at DESC)
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS device_alarm_log_active_idx
    ON device_alarm_log (device_id, alarm_type)
    WHERE status = 'ACTIVE' AND cleared_at IS NULL
  `);
}

async function refreshDeviceContacts() {
  if (!db) {
    return deviceContacts;
  }

  const result = await db.query(`
    SELECT
      device_id,
      phone_number,
      address,
      updated_at
    FROM device_contact_info
    ORDER BY device_id
  `);

  for (const key of Object.keys(deviceContacts)) {
    delete deviceContacts[key];
  }

  for (const row of result.rows) {
    applyContactToDevice(row.device_id, {
      phone_number: row.phone_number || "",
      address: row.address || "",
      contact_updated_at: row.updated_at
    });
  }

  return deviceContacts;
}

async function saveDeviceContact(deviceId, phoneNumber, address) {
  const contact = {
    phone_number: textLimit(phoneNumber, 80),
    address: textLimit(address, 240),
    contact_updated_at: new Date().toISOString()
  };

  if (!db) {
    applyContactToDevice(deviceId, contact);
    return contact;
  }

  const result = await db.query(
    `
      INSERT INTO device_contact_info (
        device_id,
        phone_number,
        address
      )
      VALUES ($1, $2, $3)
      ON CONFLICT (device_id)
      DO UPDATE SET
        phone_number = EXCLUDED.phone_number,
        address = EXCLUDED.address,
        updated_at = now()
      RETURNING device_id, phone_number, address, updated_at
    `,
    [deviceId, contact.phone_number, contact.address]
  );

  const savedContact = {
    phone_number: result.rows[0].phone_number || "",
    address: result.rows[0].address || "",
    contact_updated_at: result.rows[0].updated_at
  };

  applyContactToDevice(deviceId, savedContact);
  return savedContact;
}

async function refreshDeviceSettings() {
  if (!db) {
    return deviceSettings;
  }

  const result = await db.query(`
    SELECT
      device_id,
      low_temperature,
      high_temperature,
      high_humidity,
      telemetry_interval_sec,
      updated_at
    FROM device_desired_settings
    ORDER BY device_id
  `);

  for (const key of Object.keys(deviceSettings)) {
    delete deviceSettings[key];
  }

  for (const row of result.rows) {
    applySettingsToDevice(row.device_id, {
      desired_low_temperature: row.low_temperature === null ? null : Number(row.low_temperature),
      desired_high_temperature: row.high_temperature === null ? null : Number(row.high_temperature),
      desired_high_humidity: row.high_humidity === null ? null : Number(row.high_humidity),
      desired_telemetry_interval_sec: row.telemetry_interval_sec === null ? null : Number(row.telemetry_interval_sec),
      settings_updated_at: row.updated_at
    });
  }

  return deviceSettings;
}

function validateDesiredSettings(settings) {
  const errors = [];

  if (settings.low_temperature !== null && (settings.low_temperature < -50 || settings.low_temperature > 80)) {
    errors.push("Low temperature must be between -50 and 80");
  }

  if (settings.high_temperature !== null && (settings.high_temperature < -50 || settings.high_temperature > 80)) {
    errors.push("High temperature must be between -50 and 80");
  }

  if (
    settings.low_temperature !== null &&
    settings.high_temperature !== null &&
    settings.low_temperature >= settings.high_temperature
  ) {
    errors.push("Low temperature must be lower than high temperature");
  }

  if (settings.high_humidity !== null && (settings.high_humidity < 0 || settings.high_humidity > 100)) {
    errors.push("High humidity must be between 0 and 100");
  }

  if (
    settings.telemetry_interval_sec !== null &&
    (!Number.isInteger(settings.telemetry_interval_sec) ||
      settings.telemetry_interval_sec < 5 ||
      settings.telemetry_interval_sec > 86400)
  ) {
    errors.push("Interval must be a whole number from 5 to 86400 seconds");
  }

  return errors;
}

async function saveDeviceSettings(deviceId, values) {
  const settings = {
    low_temperature: optionalNumber(values.low_temperature),
    high_temperature: optionalNumber(values.high_temperature),
    high_humidity: optionalNumber(values.high_humidity),
    telemetry_interval_sec: optionalNumber(values.telemetry_interval_sec)
  };

  if (settings.telemetry_interval_sec !== null) {
    settings.telemetry_interval_sec = Math.round(settings.telemetry_interval_sec);
  }

  const errors = validateDesiredSettings(settings);

  if (errors.length > 0) {
    const error = new Error(errors.join("; "));
    error.statusCode = 400;
    throw error;
  }

  const desiredSettings = {
    desired_low_temperature: settings.low_temperature,
    desired_high_temperature: settings.high_temperature,
    desired_high_humidity: settings.high_humidity,
    desired_telemetry_interval_sec: settings.telemetry_interval_sec,
    settings_updated_at: new Date().toISOString()
  };

  if (!db) {
    applySettingsToDevice(deviceId, desiredSettings);
    return desiredSettings;
  }

  const result = await db.query(
    `
      INSERT INTO device_desired_settings (
        device_id,
        low_temperature,
        high_temperature,
        high_humidity,
        telemetry_interval_sec
      )
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (device_id)
      DO UPDATE SET
        low_temperature = EXCLUDED.low_temperature,
        high_temperature = EXCLUDED.high_temperature,
        high_humidity = EXCLUDED.high_humidity,
        telemetry_interval_sec = EXCLUDED.telemetry_interval_sec,
        updated_at = now()
      RETURNING
        device_id,
        low_temperature,
        high_temperature,
        high_humidity,
        telemetry_interval_sec,
        updated_at
    `,
    [
      deviceId,
      settings.low_temperature,
      settings.high_temperature,
      settings.high_humidity,
      settings.telemetry_interval_sec
    ]
  );

  const savedSettings = {
    desired_low_temperature: result.rows[0].low_temperature === null ? null : Number(result.rows[0].low_temperature),
    desired_high_temperature: result.rows[0].high_temperature === null ? null : Number(result.rows[0].high_temperature),
    desired_high_humidity: result.rows[0].high_humidity === null ? null : Number(result.rows[0].high_humidity),
    desired_telemetry_interval_sec: result.rows[0].telemetry_interval_sec === null ? null : Number(result.rows[0].telemetry_interval_sec),
    settings_updated_at: result.rows[0].updated_at
  };

  applySettingsToDevice(deviceId, savedSettings);
  return savedSettings;
}

async function refreshRecentAlarms() {
  if (!db) {
    return recentAlarms;
  }

  const result = await db.query(`
    SELECT
      id,
      device_id,
      alarm_type,
      alarm_value,
      payload,
      source_topic,
      status,
      message,
      created_at,
      cleared_at
    FROM device_alarm_log
    ORDER BY created_at DESC, id DESC
    LIMIT 100
  `);

  recentAlarms = result.rows;
  return recentAlarms;
}

function addMemoryAlarmEvent(deviceId, alarmType, payload, topic, alarmValue = null) {
  recentAlarms.unshift({
    id: `memory-${Date.now()}`,
    device_id: deviceId,
    alarm_type: alarmType,
    alarm_value: alarmValue,
    payload,
    source_topic: topic,
    status: payload,
    message: null,
    created_at: new Date().toISOString(),
    cleared_at: null
  });

  if (recentAlarms.length > 100) {
    recentAlarms.length = 100;
  }
}

function currentAlarmValue(device, alarmType) {
  if (alarmType === "high_temperature" || alarmType === "low_temperature") {
    return device.temperature === null || device.temperature === undefined ? null : String(device.temperature);
  }

  if (alarmType === "high_humidity") {
    return device.humidity === null || device.humidity === undefined ? null : String(device.humidity);
  }

  if (alarmType === "mains_lost") {
    return device.power_source || null;
  }

  if (alarmType === "ble_power_lost") {
    return device.ble_power_monitor_connection || null;
  }

  return null;
}

async function addAlarmEvent(deviceId, alarmType, payload, topic, alarmValue = null) {
  if (!db) {
    addMemoryAlarmEvent(deviceId, alarmType, payload, topic, alarmValue);
    return;
  }

  const result = await db.query(
    `
      INSERT INTO device_alarm_log (
        device_id,
        alarm_type,
        alarm_value,
        payload,
        source_topic,
        status,
        message
      )
      SELECT $1, $2, $3, $4, $5, 'ACTIVE', $6
      WHERE NOT EXISTS (
        SELECT 1
        FROM device_alarm_log
        WHERE device_id = $1
          AND alarm_type = $2
          AND status = 'ACTIVE'
          AND cleared_at IS NULL
      )
      RETURNING id
    `,
    [
      deviceId,
      alarmType,
      alarmValue,
      payload,
      topic,
      `${alarmType} alarm active`
    ]
  );

  if (result.rowCount > 0) {
    await refreshRecentAlarms();
  }
}

async function clearAlarmEvent(deviceId, alarmType) {
  if (!db) {
    for (const alarm of recentAlarms) {
      if (
        alarm.device_id === deviceId &&
        alarm.alarm_type === alarmType &&
        alarm.status === "ACTIVE" &&
        !alarm.cleared_at
      ) {
        alarm.status = "OK";
        alarm.cleared_at = new Date().toISOString();
        alarm.message = `${alarmType} alarm cleared`;
        break;
      }
    }
    return;
  }

  const result = await db.query(
    `
      UPDATE device_alarm_log
      SET
        status = 'OK',
        cleared_at = now(),
        message = $3
      WHERE device_id = $1
        AND alarm_type = $2
        AND status = 'ACTIVE'
        AND cleared_at IS NULL
    `,
    [deviceId, alarmType, `${alarmType} alarm cleared`]
  );

  if (result.rowCount > 0) {
    await refreshRecentAlarms();
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

async function handleSnjalliMessage(topic, payloadText) {
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
    const previousPayload = device.alarms[tag];
    device.alarms[tag] = payloadText;

    if (payloadText === "ACTIVE" && previousPayload !== "ACTIVE") {
      await addAlarmEvent(deviceId, tag, payloadText, topic, currentAlarmValue(device, tag));
    }

    if (payloadText === "OK" && previousPayload === "ACTIVE") {
      await clearAlarmEvent(deviceId, tag);
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

mqttClient.on("message", async (topic, payload) => {
  const payloadText = payload.toString();

  latestState[topic] = payloadText;

  try {
    if (topic.startsWith("snjalli/")) {
      await handleSnjalliMessage(topic, payloadText);
    }
  } catch (error) {
    app.log.error({ error, topic }, "Failed to handle MQTT message");
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

app.patch("/api/v1/devices/:deviceId/contact", { preHandler: checkAuth }, async (req, reply) => {
  const deviceId = String(req.params.deviceId || "").trim();

  if (!/^[A-Za-z0-9_-]+$/.test(deviceId)) {
    reply.code(400);
    return {
      ok: false,
      error: "Invalid device ID"
    };
  }

  const { phone_number: phoneNumber = "", address = "" } = req.body || {};
  const contact = await saveDeviceContact(deviceId, phoneNumber, address);

  publishWebSocketState();

  return {
    ok: true,
    device_id: deviceId,
    ...contact
  };
});

app.patch("/api/v1/devices/:deviceId/settings", { preHandler: checkAuth }, async (req, reply) => {
  const deviceId = String(req.params.deviceId || "").trim();

  if (!/^[A-Za-z0-9_-]+$/.test(deviceId)) {
    reply.code(400);
    return {
      ok: false,
      error: "Invalid device ID"
    };
  }

  try {
    const settings = await saveDeviceSettings(deviceId, req.body || {});

    publishWebSocketState();

    return {
      ok: true,
      device_id: deviceId,
      ...settings
    };
  } catch (error) {
    reply.code(error.statusCode || 500);
    return {
      ok: false,
      error: error.message
    };
  }
});

app.get("/api/v1/alarms", { preHandler: checkAuth }, async () => {
  return refreshRecentAlarms();
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

await initDatabase();
await refreshDeviceContacts();
await refreshDeviceSettings();
await refreshRecentAlarms();

app.listen({ port: PORT, host: "0.0.0.0" });
