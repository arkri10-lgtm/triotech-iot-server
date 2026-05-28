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
const DEVICE_API_TOKEN = process.env.DEVICE_API_TOKEN || "";
const DEVICE_OFFLINE_GRACE_MS = Number(process.env.DEVICE_OFFLINE_GRACE_MS || 120000);
const DATABASE_URL = process.env.DATABASE_URL || "";
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || "";
const ADMIN_INITIAL_PASSWORD = process.env.ADMIN_INITIAL_PASSWORD || "";
const DEFAULT_CUSTOMER_ID = process.env.DEFAULT_CUSTOMER_ID || "customer-1";
const DEFAULT_CUSTOMER_NAME = process.env.DEFAULT_CUSTOMER_NAME || "Customer 1";
const SESSION_MAX_AGE_MS = 12 * 60 * 60 * 1000;
const ALLOWED_SUBSCRIPTION_STATUSES = new Set(["active", "trialing", "grace"]);
const VALID_SUBSCRIPTION_STATUSES = new Set(["active", "trialing", "grace", "past_due", "suspended", "canceled"]);
const VALID_SUBSCRIPTION_PLANS = new Set(["monthly", "yearly", "trial", "manual"]);

const db = DATABASE_URL ? new Pool({ connectionString: DATABASE_URL }) : null;

const mqttClient = mqtt.connect(process.env.MQTT_URL || "mqtt://emqx:1883", {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  reconnectPeriod: 2000
});

const latestState = {};
const devices = {};
const deviceRegistry = {};
const deviceContacts = {};
const deviceSettings = {};
let recentAlarms = [];
const wsClients = new Map();
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

    .nav-links {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-left: auto;
    }

    .nav-links a {
      color: var(--text);
      text-decoration: none;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 7px 10px;
      background: #fff;
    }

    .nav-links a.active {
      border-color: #3050c8;
      color: #1f3fb0;
      font-weight: 700;
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
      width: 12ch;
      height: 30px;
      padding: 0 6px;
      background: #fff;
    }

    .phone-input {
      width: 9ch;
    }

    .address-input {
      width: 29ch;
    }

    .setting-input {
      width: 6ch;
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

    .filter-bar {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 10px;
      color: var(--muted);
    }

    .filter-bar label {
      color: var(--text);
      font-weight: 700;
    }

    .filter-input {
      width: min(520px, 70vw);
    }

    .auth-input {
      width: 220px;
    }

    .user-info {
      color: var(--muted);
      font-size: 13px;
    }

    .password-panel {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 14px;
      padding: 10px;
      background: #fff8df;
      border: 1px solid #f0ce84;
      border-radius: 8px;
    }

    .password-panel strong {
      color: var(--warn);
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
      min-width: 1460px;
    }

    th,
    td {
      border-bottom: 1px solid var(--line);
      padding: 8px 6px;
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

    .sort-button {
      appearance: none;
      border: 0;
      background: transparent;
      color: inherit;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 4px;
      height: auto;
      padding: 0;
      font: inherit;
      font-weight: 700;
      text-align: left;
      text-transform: inherit;
    }

    .sort-indicator {
      color: var(--muted);
      min-width: 1ch;
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

    [hidden] {
      display: none !important;
    }

    .with-unit {
      display: flex;
      align-items: center;
      gap: 4px;
      white-space: nowrap;
    }

    .unit {
      color: var(--muted);
      font-size: 12px;
    }
  </style>
</head>
<body>
  <header>
    <h1>Snjallhus Device Overview</h1>
    <nav class="nav-links" aria-label="Main pages">
      <a id="devicesLink" href="/dashboard">Devices</a>
      <a id="alarmsLink" href="/alarms">Alarm log</a>
    </nav>
    <div class="toolbar">
      <span id="userInfo" class="user-info"></span>
      <input id="emailInput" class="auth-input" type="email" autocomplete="username" placeholder="Email">
      <input id="passwordInput" class="auth-input" type="password" autocomplete="current-password" placeholder="Password">
      <button id="loginButton">Login</button>
      <button id="logoutButton" hidden>Logout</button>
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

    <section id="passwordChangeSection" class="password-panel" hidden>
      <strong>Password change required</strong>
      <input id="currentPasswordInput" class="auth-input" type="password" autocomplete="current-password" placeholder="Current password">
      <input id="newPasswordInput" class="auth-input" type="password" autocomplete="new-password" placeholder="New password">
      <input id="confirmPasswordInput" class="auth-input" type="password" autocomplete="new-password" placeholder="Confirm new password">
      <button id="changePasswordButton">Change password</button>
      <span id="passwordChangeStatus"></span>
    </section>

    <section id="deviceSection">
      <div class="filter-bar">
        <label for="deviceFilter">Filter</label>
        <input id="deviceFilter" class="filter-input" type="search" autocomplete="off" placeholder="Search device ID, address, phone, status, power, alarm...">
        <button id="clearDeviceFilter" type="button">Clear</button>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th><button class="sort-button" data-sort-key="device_id" data-sort-type="text">Device ID <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="phone_number" data-sort-type="text">Phone <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="address" data-sort-type="text">Address <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="connection_state" data-sort-type="text">Status <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="last_seen" data-sort-type="time">Last seen <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="temperature" data-sort-type="number">Temp (C) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="humidity" data-sort-type="number">Humidity (%) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="power_source" data-sort-type="text">Power <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="ble_power_monitor_installed" data-sort-type="text">BLE installed <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="ble_power_monitor_connection" data-sort-type="text">BLE connection <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="low_temperature_display" data-sort-type="number">Low temp (C) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="high_temperature_display" data-sort-type="number">High temp (C) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="high_humidity_display" data-sort-type="number">High humidity (%) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="telemetry_interval_display" data-sort-type="number">Interval (s) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="alarm_state" data-sort-type="text">Alarm <span class="sort-indicator"></span></button></th>
              <th>Save</th>
            </tr>
          </thead>
          <tbody id="deviceRows">
            <tr><td colspan="16" class="empty">No device data loaded.</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="alarmSection">
      <h2>Alarm Log</h2>
      <div class="filter-bar">
        <label for="alarmFilter">Filter</label>
        <input id="alarmFilter" class="filter-input" type="search" autocomplete="off" placeholder="Search time, device ID, alarm, topic, status...">
        <button id="clearAlarmFilter" type="button">Clear</button>
        <span>Shown: <strong id="alarmCount">0</strong></span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th><button class="sort-button" data-sort-key="created_at" data-sort-type="time">Time <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="cleared_at" data-sort-type="time">Cleared <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="device_id" data-sort-type="text">Device ID <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="alarm_type" data-sort-type="text">Alarm <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="source_topic" data-sort-type="text">Topic <span class="sort-indicator"></span></button></th>
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
    const emailInput = document.getElementById("emailInput");
    const passwordInput = document.getElementById("passwordInput");
    const loginButton = document.getElementById("loginButton");
    const logoutButton = document.getElementById("logoutButton");
    const refresh = document.getElementById("refresh");
    const apiStatus = document.getElementById("apiStatus");
    const wsStatus = document.getElementById("wsStatus");
    const userInfo = document.getElementById("userInfo");
    const passwordChangeSection = document.getElementById("passwordChangeSection");
    const currentPasswordInput = document.getElementById("currentPasswordInput");
    const newPasswordInput = document.getElementById("newPasswordInput");
    const confirmPasswordInput = document.getElementById("confirmPasswordInput");
    const changePasswordButton = document.getElementById("changePasswordButton");
    const passwordChangeStatus = document.getElementById("passwordChangeStatus");
    const deviceCount = document.getElementById("deviceCount");
    const lastUpdate = document.getElementById("lastUpdate");
    const deviceRows = document.getElementById("deviceRows");
    const alarmRows = document.getElementById("alarmRows");
    const deviceSection = document.getElementById("deviceSection");
    const alarmSection = document.getElementById("alarmSection");
    const devicesLink = document.getElementById("devicesLink");
    const alarmsLink = document.getElementById("alarmsLink");
    const deviceFilter = document.getElementById("deviceFilter");
    const clearDeviceFilter = document.getElementById("clearDeviceFilter");
    const alarmFilter = document.getElementById("alarmFilter");
    const clearAlarmFilter = document.getElementById("clearAlarmFilter");
    const alarmCount = document.getElementById("alarmCount");

    let ws = null;
    const pendingContactEdits = new Map();
    const pendingSettingEdits = new Map();
    const isAlarmPage = location.pathname.startsWith("/alarms");
    const sortState = { key: "device_id", type: "text", direction: "asc" };
    const alarmSortState = { key: "created_at", type: "time", direction: "desc" };
    let latestDevices = [];
    let latestAlarms = [];
    let currentUser = null;
    emailInput.value = localStorage.getItem("snjallhus_email") || "";
    deviceFilter.value = localStorage.getItem("snjallhus_device_filter") || "";
    alarmFilter.value = localStorage.getItem("snjallhus_alarm_filter") || "";

    deviceSection.hidden = isAlarmPage;
    alarmSection.hidden = !isAlarmPage;
    devicesLink.classList.toggle("active", !isAlarmPage);
    alarmsLink.classList.toggle("active", isAlarmPage);

    function authHeaders() {
      return {};
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

    function displaySetting(device, desiredKey, activeKey) {
      return device[desiredKey] ?? device[activeKey];
    }

    function sortValue(device, key) {
      if (key === "low_temperature_display") {
        return displaySetting(device, "desired_low_temperature", "low_temperature");
      }

      if (key === "high_temperature_display") {
        return displaySetting(device, "desired_high_temperature", "high_temperature");
      }

      if (key === "high_humidity_display") {
        return displaySetting(device, "desired_high_humidity", "high_humidity");
      }

      if (key === "telemetry_interval_display") {
        return displaySetting(device, "desired_telemetry_interval_sec", "telemetry_interval_sec");
      }

      if (key === "connection_state") {
        return device.connection_state || device.status;
      }

      if (key === "ble_power_monitor_installed") {
        return device.ble_power_monitor_installed === true ? "yes" : (device.ble_power_monitor_installed === false ? "no" : "");
      }

      return device[key];
    }

    function compareDevices(a, b) {
      const direction = sortState.direction === "desc" ? -1 : 1;
      const aValue = sortValue(a, sortState.key);
      const bValue = sortValue(b, sortState.key);
      const aEmpty = aValue === null || aValue === undefined || aValue === "";
      const bEmpty = bValue === null || bValue === undefined || bValue === "";

      if (aEmpty && bEmpty) return 0;
      if (aEmpty) return 1;
      if (bEmpty) return -1;

      let result = 0;

      if (sortState.type === "number") {
        result = Number(aValue) - Number(bValue);
      } else if (sortState.type === "time") {
        result = Date.parse(aValue) - Date.parse(bValue);
      } else {
        result = String(aValue).localeCompare(String(bValue), undefined, {
          numeric: true,
          sensitivity: "base"
        });
      }

      if (!Number.isFinite(result)) {
        result = 0;
      }

      return result * direction;
    }

    function sortedDevices(devices) {
      return [...devices].sort(compareDevices);
    }

    function searchText(value) {
      return String(value ?? "").toLowerCase();
    }

    function deviceFilterText(device) {
      const alarmText = Object.entries(device.alarms || {})
        .map(([alarm, state]) => alarm + " " + state)
        .join(" ");

      return [
        device.device_id,
        device.phone_number,
        device.address,
        device.connection_state || device.status,
        device.status,
        device.temperature,
        device.humidity,
        device.power_source,
        sortValue(device, "ble_power_monitor_installed"),
        device.ble_power_monitor_connection,
        displaySetting(device, "desired_low_temperature", "low_temperature"),
        displaySetting(device, "desired_high_temperature", "high_temperature"),
        displaySetting(device, "desired_high_humidity", "high_humidity"),
        displaySetting(device, "desired_telemetry_interval_sec", "telemetry_interval_sec"),
        device.alarm_state,
        alarmText
      ].map(searchText).join(" ");
    }

    function filteredDevices(devices) {
      const terms = deviceFilter.value.trim().toLowerCase().split(/\s+/).filter(Boolean);

      if (!terms.length) {
        return devices;
      }

      return devices.filter((device) => {
        const haystack = deviceFilterText(device);
        return terms.every((term) => haystack.includes(term));
      });
    }

    function updateSortIndicators() {
      document.querySelectorAll("#deviceSection .sort-button").forEach((button) => {
        const indicator = button.querySelector(".sort-indicator");
        const active = button.dataset.sortKey === sortState.key;
        button.setAttribute("aria-sort", active ? (sortState.direction === "asc" ? "ascending" : "descending") : "none");
        indicator.textContent = active ? (sortState.direction === "asc" ? "^" : "v") : "";
      });
    }

    function alarmSortValue(alarm, key) {
      return alarm[key];
    }

    function compareAlarms(a, b) {
      const direction = alarmSortState.direction === "desc" ? -1 : 1;
      const aValue = alarmSortValue(a, alarmSortState.key);
      const bValue = alarmSortValue(b, alarmSortState.key);
      const aEmpty = aValue === null || aValue === undefined || aValue === "";
      const bEmpty = bValue === null || bValue === undefined || bValue === "";

      if (aEmpty && bEmpty) return 0;
      if (aEmpty) return 1;
      if (bEmpty) return -1;

      let result = 0;

      if (alarmSortState.type === "number") {
        result = Number(aValue) - Number(bValue);
      } else if (alarmSortState.type === "time") {
        result = Date.parse(aValue) - Date.parse(bValue);
      } else {
        result = String(aValue).localeCompare(String(bValue), undefined, {
          numeric: true,
          sensitivity: "base"
        });
      }

      if (!Number.isFinite(result)) {
        result = 0;
      }

      return result * direction;
    }

    function sortedAlarms(alarms) {
      return [...alarms].sort(compareAlarms);
    }

    function alarmFilterText(alarm) {
      return [
        alarm.created_at,
        fmtTime(alarm.created_at),
        alarm.cleared_at,
        fmtTime(alarm.cleared_at),
        alarm.device_id,
        alarm.alarm_type,
        alarm.alarm_value,
        alarm.payload,
        alarm.source_topic,
        alarm.status,
        alarm.message
      ].map(searchText).join(" ");
    }

    function filteredAlarms(alarms) {
      const terms = alarmFilter.value.trim().toLowerCase().split(/\s+/).filter(Boolean);

      if (!terms.length) {
        return alarms;
      }

      return alarms.filter((alarm) => {
        const haystack = alarmFilterText(alarm);
        return terms.every((term) => haystack.includes(term));
      });
    }

    function updateAlarmSortIndicators() {
      document.querySelectorAll("#alarmSection .sort-button").forEach((button) => {
        const indicator = button.querySelector(".sort-indicator");
        const active = button.dataset.sortKey === alarmSortState.key;
        button.setAttribute("aria-sort", active ? (alarmSortState.direction === "asc" ? "ascending" : "descending") : "none");
        indicator.textContent = active ? (alarmSortState.direction === "asc" ? "^" : "v") : "";
      });
    }

    function contactInput(value, extraClass) {
      const input = document.createElement("input");
      input.className = "row-input " + (extraClass || "");
      input.value = value || "";
      return input;
    }

    function formatSettingValue(value, decimals) {
      if (value === null || value === undefined || value === "") return "";
      const number = Number(value);
      if (!Number.isFinite(number)) return String(value);
      return decimals === null ? String(Math.round(number)) : number.toFixed(decimals);
    }

    function settingInput(value, activeValue, extraClass, decimals, unit) {
      const input = document.createElement("input");
      input.className = "row-input setting-input " + (extraClass || "");
      input.type = "text";
      input.inputMode = "decimal";
      input.maxLength = 4;
      input.value = formatSettingValue(value, decimals);
      input.title = "Active value: " + fmt(activeValue, unit ? " " + unit : "");
      return input;
    }

    function settingWithUnit(input, unit) {
      const wrap = document.createElement("span");
      const suffix = document.createElement("span");
      wrap.className = "with-unit";
      suffix.className = "unit";
      suffix.textContent = unit;
      wrap.append(input, suffix);
      return wrap;
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

    async function patchJson(url, body) {
      const response = await fetch(url, {
        method: "PATCH",
        headers: {
          ...authHeaders(),
          "Content-Type": "application/json"
        },
        body: JSON.stringify(body)
      });

      if (!response.ok) {
        const responseBody = await response.json().catch(() => ({}));
        throw new Error(responseBody.error || ("HTTP " + response.status));
      }

      return response.json();
    }

    async function saveRow(deviceId, phoneInput, addressInput, lowInput, highInput, humidityInput, intervalInput, button) {
      button.disabled = true;
      button.textContent = "Saving";
      button.classList.remove("saved");

      try {
        const contactChanged = pendingContactEdits.has(deviceId);
        const settingsChanged = pendingSettingEdits.has(deviceId);

        if (settingsChanged) {
          await patchJson("/api/v1/devices/" + encodeURIComponent(deviceId) + "/settings", {
            low_temperature: lowInput.value,
            high_temperature: highInput.value,
            high_humidity: humidityInput.value,
            telemetry_interval_sec: intervalInput.value
          });
          pendingSettingEdits.delete(deviceId);
        }

        if (contactChanged) {
          await patchJson("/api/v1/devices/" + encodeURIComponent(deviceId) + "/contact", {
            phone_number: phoneInput.value.trim(),
            address: addressInput.value.trim()
          });
          pendingContactEdits.delete(deviceId);
        }

        button.textContent = "Saved";
        button.classList.add("saved");
        setTimeout(() => {
          button.textContent = "Save";
          button.classList.remove("saved");
        }, 1200);
      } catch (error) {
        button.textContent = "Error";
        apiStatus.textContent = "save " + error.message;
      } finally {
        button.disabled = false;
      }
    }

    function renderDevices(devices) {
      deviceRows.textContent = "";
      const visibleDevices = filteredDevices(devices);
      deviceCount.textContent = visibleDevices.length === devices.length
        ? String(devices.length)
        : visibleDevices.length + " / " + devices.length;
      updateSortIndicators();

      if (!devices.length) {
        const row = document.createElement("tr");
        cell(row, "No devices received yet.");
        row.firstChild.colSpan = 16;
        row.firstChild.className = "empty";
        deviceRows.appendChild(row);
        return;
      }

      if (!visibleDevices.length) {
        const row = document.createElement("tr");
        cell(row, "No devices match the current filter.");
        row.firstChild.colSpan = 16;
        row.firstChild.className = "empty";
        deviceRows.appendChild(row);
        return;
      }

      for (const device of sortedDevices(visibleDevices)) {
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
        phoneInput.addEventListener("input", () => setPendingContact(device.device_id, "phone_number", phoneInput.value));
        addressInput.addEventListener("input", () => setPendingContact(device.device_id, "address", addressInput.value));
        const pendingSettings = pendingSettingEdits.get(device.device_id) || {};
        const lowInput = settingInput(pendingSettings.low_temperature ?? device.desired_low_temperature ?? device.low_temperature, device.low_temperature, "low-temp-input", 1, "C");
        const highInput = settingInput(pendingSettings.high_temperature ?? device.desired_high_temperature ?? device.high_temperature, device.high_temperature, "high-temp-input", 1, "C");
        const humidityInput = settingInput(pendingSettings.high_humidity ?? device.desired_high_humidity ?? device.high_humidity, device.high_humidity, "high-humidity-input", 1, "%");
        const intervalInput = settingInput(pendingSettings.telemetry_interval_sec ?? device.desired_telemetry_interval_sec ?? device.telemetry_interval_sec, device.telemetry_interval_sec, "interval-input", null, "s");
        const rowSaveButton = document.createElement("button");
        rowSaveButton.className = "save-row";
        rowSaveButton.textContent = "Save";
        lowInput.addEventListener("input", () => setPendingSetting(device.device_id, "low_temperature", lowInput.value));
        highInput.addEventListener("input", () => setPendingSetting(device.device_id, "high_temperature", highInput.value));
        humidityInput.addEventListener("input", () => setPendingSetting(device.device_id, "high_humidity", humidityInput.value));
        intervalInput.addEventListener("input", () => setPendingSetting(device.device_id, "telemetry_interval_sec", intervalInput.value));
        rowSaveButton.addEventListener("click", () => {
          saveRow(device.device_id, phoneInput, addressInput, lowInput, highInput, humidityInput, intervalInput, rowSaveButton);
        });

        cell(row, device.device_id);
        cell(row, phoneInput);
        cell(row, addressInput);
        cell(row, badge(status, statusKind));
        cell(row, fmtTime(device.last_seen) + (device.seconds_since_seen !== null ? " (" + fmtAge(device.seconds_since_seen) + ")" : ""));
        cell(row, fmt(device.temperature, " C"));
        cell(row, fmt(device.humidity, " %"));
        cell(row, device.power_source);
        cell(row, device.ble_power_monitor_installed === true ? "yes" : (device.ble_power_monitor_installed === false ? "no" : ""));
        cell(row, device.ble_power_monitor_connection || "");
        cell(row, settingWithUnit(lowInput, "C"));
        cell(row, settingWithUnit(highInput, "C"));
        cell(row, settingWithUnit(humidityInput, "%"));
        cell(row, settingWithUnit(intervalInput, "s"));
        cell(row, device.alarm_state ? badge(device.alarm_state, device.alarm_state === "ALARM" ? "alarm" : "ok") : "");
        cell(row, rowSaveButton);
        deviceRows.appendChild(row);
      }
    }

    function renderAlarms(alarms) {
      alarmRows.textContent = "";
      const visibleAlarms = filteredAlarms(alarms);
      alarmCount.textContent = visibleAlarms.length === alarms.length
        ? String(alarms.length)
        : visibleAlarms.length + " / " + alarms.length;
      updateAlarmSortIndicators();

      if (!alarms.length) {
        const row = document.createElement("tr");
        cell(row, "No alarm history received.");
        row.firstChild.colSpan = 5;
        row.firstChild.className = "empty";
        alarmRows.appendChild(row);
        return;
      }

      if (!visibleAlarms.length) {
        const row = document.createElement("tr");
        cell(row, "No alarms match the current filter.");
        row.firstChild.colSpan = 5;
        row.firstChild.className = "empty";
        alarmRows.appendChild(row);
        return;
      }

      for (const alarm of sortedAlarms(visibleAlarms).slice(0, 100)) {
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
      const devices = state.devices || [];
      const alarms = state.alarms || [];
      latestDevices = devices;
      latestAlarms = alarms;
      deviceCount.textContent = String(devices.length);

      if (!isAlarmPage && !isEditingContact()) {
        renderDevices(devices);
      }

      if (isAlarmPage) {
        renderAlarms(alarms);
      }

      lastUpdate.textContent = fmtClock(new Date());
    }

    async function loadState() {
      if (!currentUser) {
        apiStatus.textContent = "not logged in";
        return;
      }

      try {
        const response = await fetch("/api/v1/state", { headers: authHeaders() });
        if (response.status === 401) {
          setCurrentUser(null);
          throw new Error("not logged in");
        }
        if (!response.ok) throw new Error("HTTP " + response.status);
        const data = await response.json();
        apiStatus.textContent = "connected";
        renderState(data);
      } catch (error) {
        apiStatus.textContent = error.message;
      }
    }

    function setCurrentUser(user) {
      currentUser = user;
      const loggedIn = Boolean(user);

      emailInput.hidden = loggedIn;
      passwordInput.hidden = loggedIn;
      loginButton.hidden = loggedIn;
      logoutButton.hidden = !loggedIn;
      userInfo.textContent = loggedIn ? user.email + " (" + user.role + ")" : "";
      passwordChangeSection.hidden = !loggedIn || !user.must_change_password;

      if (!loggedIn) {
        apiStatus.textContent = "not logged in";
        wsStatus.textContent = "not connected";
        latestDevices = [];
        latestAlarms = [];
        renderDevices([]);
        renderAlarms([]);
      }
    }

    async function login() {
      apiStatus.textContent = "logging in";
      const response = await fetch("/api/v1/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: emailInput.value.trim(),
          password: passwordInput.value
        })
      });

      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.error || ("HTTP " + response.status));
      }

      const body = await response.json();
      localStorage.setItem("snjallhus_email", emailInput.value.trim());
      passwordInput.value = "";
      setCurrentUser(body.user);
      await loadState();
      connectWs();
    }

    async function logout() {
      await fetch("/api/v1/logout", { method: "POST" }).catch(() => {});
      if (ws) ws.close();
      ws = null;
      setCurrentUser(null);
    }

    async function loadMe() {
      const response = await fetch("/api/v1/me");

      if (!response.ok) {
        setCurrentUser(null);
        return;
      }

      const body = await response.json();
      setCurrentUser(body.user);
      await loadState();
      connectWs();
    }

    async function changePassword() {
      if (newPasswordInput.value !== confirmPasswordInput.value) {
        throw new Error("New passwords do not match");
      }

      passwordChangeStatus.textContent = "saving";
      const response = await fetch("/api/v1/change-password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          current_password: currentPasswordInput.value,
          new_password: newPasswordInput.value,
          new_password_confirm: confirmPasswordInput.value
        })
      });

      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.error || ("HTTP " + response.status));
      }

      const body = await response.json();
      currentPasswordInput.value = "";
      newPasswordInput.value = "";
      confirmPasswordInput.value = "";
      passwordChangeStatus.textContent = "changed";
      setCurrentUser(body.user);
      setTimeout(() => {
        passwordChangeStatus.textContent = "";
      }, 1500);
    }

    function connectWs() {
      if (ws) ws.close();

      if (!currentUser) {
        wsStatus.textContent = "not logged in";
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
        if (currentUser) {
          setTimeout(connectWs, 3000);
        }
      });

      ws.addEventListener("error", () => {
        wsStatus.textContent = "error";
      });
    }

    loginButton.addEventListener("click", () => {
      login().catch((error) => {
        apiStatus.textContent = error.message;
        wsStatus.textContent = "not connected";
      });
    });

    passwordInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        loginButton.click();
      }
    });

    logoutButton.addEventListener("click", () => {
      logout();
    });

    changePasswordButton.addEventListener("click", () => {
      changePassword().catch((error) => {
        passwordChangeStatus.textContent = error.message;
      });
    });

    refresh.addEventListener("click", loadState);

    deviceFilter.addEventListener("input", () => {
      localStorage.setItem("snjallhus_device_filter", deviceFilter.value);
      renderDevices(latestDevices);
    });

    clearDeviceFilter.addEventListener("click", () => {
      deviceFilter.value = "";
      localStorage.removeItem("snjallhus_device_filter");
      renderDevices(latestDevices);
      deviceFilter.focus();
    });

    alarmFilter.addEventListener("input", () => {
      localStorage.setItem("snjallhus_alarm_filter", alarmFilter.value);
      renderAlarms(latestAlarms);
    });

    clearAlarmFilter.addEventListener("click", () => {
      alarmFilter.value = "";
      localStorage.removeItem("snjallhus_alarm_filter");
      renderAlarms(latestAlarms);
      alarmFilter.focus();
    });

    document.querySelectorAll("#deviceSection .sort-button").forEach((button) => {
      button.addEventListener("click", () => {
        const key = button.dataset.sortKey;

        if (sortState.key === key) {
          sortState.direction = sortState.direction === "asc" ? "desc" : "asc";
        } else {
          sortState.key = key;
          sortState.type = button.dataset.sortType || "text";
          sortState.direction = "asc";
        }

        renderDevices(latestDevices);
      });
    });

    document.querySelectorAll("#alarmSection .sort-button").forEach((button) => {
      button.addEventListener("click", () => {
        const key = button.dataset.sortKey;

        if (alarmSortState.key === key) {
          alarmSortState.direction = alarmSortState.direction === "asc" ? "desc" : "asc";
        } else {
          alarmSortState.key = key;
          alarmSortState.type = button.dataset.sortType || "text";
          alarmSortState.direction = alarmSortState.type === "time" ? "desc" : "asc";
        }

        renderAlarms(latestAlarms);
      });
    });

    loadMe();
  </script>
</body>
</html>`;

function isAuthorizedToken(value) {
  return Boolean(API_TOKEN) && value === API_TOKEN;
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(String(password), salt, 64).toString("hex");
  return `scrypt:${salt}:${hash}`;
}

function verifyPassword(password, storedHash) {
  const parts = String(storedHash || "").split(":");

  if (parts.length !== 3 || parts[0] !== "scrypt") {
    return false;
  }

  const [, salt, hash] = parts;
  const expected = Buffer.from(hash, "hex");
  const actual = crypto.scryptSync(String(password), salt, expected.length);

  return expected.length === actual.length && crypto.timingSafeEqual(expected, actual);
}

function publicUser(user) {
  if (!user) return null;

  return {
    id: user.id,
    email: user.email,
    role: user.role,
    customer_id: user.customer_id,
    must_change_password: Boolean(user.must_change_password),
    subscription: user.subscription || null
  };
}

function apiTokenUser() {
  return {
    id: "api-token-admin",
    email: "api-token-admin",
    role: "admin",
    customer_id: null,
    must_change_password: false
  };
}

function isAdminUser(user) {
  return Boolean(user && user.role === "admin");
}

function subscriptionFromRow(row) {
  if (!row || row.role === "admin") {
    return null;
  }

  return {
    status: row.subscription_status || "active",
    plan: row.subscription_plan || "monthly",
    paid_until: row.paid_until ? new Date(row.paid_until).toISOString() : null,
    login_enabled: row.login_enabled !== false
  };
}

function userFromRow(row) {
  return {
    id: row.id,
    customer_id: row.customer_id,
    email: row.email,
    role: row.role,
    must_change_password: row.must_change_password,
    subscription: subscriptionFromRow(row)
  };
}

function subscriptionAllowsLogin(subscription) {
  if (!subscription) {
    return false;
  }

  if (!subscription.login_enabled) {
    return false;
  }

  if (!ALLOWED_SUBSCRIPTION_STATUSES.has(subscription.status)) {
    return false;
  }

  if (!subscription.paid_until) {
    return true;
  }

  return Date.parse(subscription.paid_until) >= Date.now();
}

function userCanUseDashboard(user) {
  if (!user) {
    return false;
  }

  if (isAdminUser(user)) {
    return true;
  }

  return subscriptionAllowsLogin(user.subscription);
}

async function refreshUserForAccess(user) {
  if (!user || user.id === "api-token-admin" || !db) {
    return user;
  }

  const result = await db.query(
    `
      SELECT
        u.id,
        u.customer_id,
        u.email,
        u.role,
        u.must_change_password,
        u.active,
        c.subscription_status,
        c.subscription_plan,
        c.paid_until,
        c.login_enabled
      FROM app_users u
      LEFT JOIN customers c ON c.id = u.customer_id
      WHERE u.id = $1
      LIMIT 1
    `,
    [user.id]
  );

  const row = result.rows[0];

  if (!row || !row.active) {
    return null;
  }

  return userFromRow(row);
}

function createDashboardSession(user) {
  const sessionId = crypto.randomUUID();
  dashboardSessions.set(sessionId, {
    user,
    expiresAt: Date.now() + SESSION_MAX_AGE_MS
  });
  return sessionId;
}

function getSessionUser(req) {
  const cookies = parseCookies(req.headers.cookie || "");
  const sessionId = cookies.snjallhus_session || "";
  const session = dashboardSessions.get(sessionId);

  if (!session) {
    return null;
  }

  if (Date.now() > session.expiresAt) {
    dashboardSessions.delete(sessionId);
    return null;
  }

  return session.user;
}

function getRequestUser(req) {
  const sessionUser = getSessionUser(req);

  if (sessionUser) {
    return sessionUser;
  }

  const auth = req.headers.authorization || "";
  const token = auth.replace(/^Bearer\s+/i, "");

  if (isAuthorizedToken(token)) {
    return apiTokenUser();
  }

  return null;
}

function shouldUseSecureCookie(req) {
  const forwardedProto = String(req.headers["x-forwarded-proto"] || "").toLowerCase();
  const host = String(req.headers.host || "").toLowerCase();
  return forwardedProto === "https" || host === "api.snjallhus.com" || host.endsWith(".snjallhus.com");
}

function sessionCookie(sessionId, req) {
  const secure = shouldUseSecureCookie(req) ? "; Secure" : "";
  return `snjallhus_session=${encodeURIComponent(sessionId)}; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200${secure}`;
}

function clearSessionCookie(req) {
  const secure = shouldUseSecureCookie(req) ? "; Secure" : "";
  return `snjallhus_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0${secure}`;
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

async function checkAuth(req, reply) {
  const requestUser = getRequestUser(req);
  const user = await refreshUserForAccess(requestUser);

  if (!user) {
    reply.code(401).send({ error: "Unauthorized" });
    return;
  }

  if (!userCanUseDashboard(user)) {
    reply.code(402).send({
      error: "Subscription inactive",
      subscription: user.subscription || null
    });
    return;
  }

  req.user = user;
}

function checkDeviceAuth(req, reply, done) {
  const tokenHeader = req.headers["x-device-token"];
  const token = Array.isArray(tokenHeader) ? tokenHeader[0] : tokenHeader;

  if (!DEVICE_API_TOKEN) {
    reply.code(503).send({ error: "Device API token is not configured" });
    return;
  }

  if (token !== DEVICE_API_TOKEN) {
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

function isValidDeviceId(deviceId) {
  return /^[A-Za-z0-9_-]+$/.test(deviceId);
}

function desiredSettingsResponse(deviceId) {
  const settings = settingsForDevice(deviceId);
  const hasSettings = settings.settings_updated_at !== null ||
    settings.desired_low_temperature !== null ||
    settings.desired_high_temperature !== null ||
    settings.desired_high_humidity !== null ||
    settings.desired_telemetry_interval_sec !== null;

  return {
    ok: true,
    device_id: deviceId,
    has_settings: hasSettings,
    low_temperature: settings.desired_low_temperature,
    high_temperature: settings.desired_high_temperature,
    high_humidity: settings.desired_high_humidity,
    telemetry_interval_sec: settings.desired_telemetry_interval_sec,
    updated_at: settings.settings_updated_at
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

function deviceOfflineInfo(device, now = Date.now()) {
  const lastSeenMs = device.last_seen ? Date.parse(device.last_seen) : NaN;
  const hasLastSeen = Number.isFinite(lastSeenMs);
  const secondsSinceSeen = hasLastSeen ? Math.max(0, Math.floor((now - lastSeenMs) / 1000)) : null;
  const telemetryIntervalMs = Number.isFinite(device.telemetry_interval_sec)
    ? Math.max(0, device.telemetry_interval_sec * 1000)
    : 0;
  const offlineAfterMs = telemetryIntervalMs + DEVICE_OFFLINE_GRACE_MS;

  return {
    hasLastSeen,
    secondsSinceSeen,
    offlineAfterMs,
    isOffline: !hasLastSeen || now - lastSeenMs > offlineAfterMs
  };
}

function deviceHasActiveAlarm(device) {
  return Object.values(device.alarms || {}).includes("ACTIVE");
}

function getDeviceView(device, now = Date.now()) {
  const { isOffline, secondsSinceSeen, offlineAfterMs } = deviceOfflineInfo(device, now);

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

function getDashboardState(user = apiTokenUser()) {
  const now = Date.now();
  const deviceViews = Object.values(devices).map((device) => getDeviceView(device, now));

  return {
    raw: filterRawStateForUser(user),
    devices: filterDevicesForUser(user, deviceViews),
    alarms: filterAlarmsForUser(user, recentAlarms)
  };
}

async function initDatabase() {
  if (!db) {
    app.log.warn("DATABASE_URL is not configured; alarm history will stay in memory only");
    return;
  }

  await db.query(`
    CREATE TABLE IF NOT EXISTS customers (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      subscription_status TEXT NOT NULL DEFAULT 'active',
      subscription_plan TEXT NOT NULL DEFAULT 'monthly',
      paid_until TIMESTAMPTZ,
      login_enabled BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await db.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS subscription_status TEXT NOT NULL DEFAULT 'active'");
  await db.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS subscription_plan TEXT NOT NULL DEFAULT 'monthly'");
  await db.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS paid_until TIMESTAMPTZ");
  await db.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS login_enabled BOOLEAN NOT NULL DEFAULT true");

  await db.query(`
    CREATE TABLE IF NOT EXISTS app_users (
      id TEXT PRIMARY KEY,
      customer_id TEXT REFERENCES customers(id),
      email TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'customer',
      must_change_password BOOLEAN NOT NULL DEFAULT true,
      active BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS devices (
      device_id TEXT PRIMARY KEY,
      customer_id TEXT REFERENCES customers(id),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

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

  await db.query(`
    CREATE INDEX IF NOT EXISTS app_users_customer_idx
    ON app_users (customer_id)
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS devices_customer_idx
    ON devices (customer_id)
  `);

  await db.query(
    `
      INSERT INTO customers (id, name)
      VALUES ($1, $2)
      ON CONFLICT (id) DO NOTHING
    `,
    [DEFAULT_CUSTOMER_ID, DEFAULT_CUSTOMER_NAME]
  );

  await db.query(
    `
      INSERT INTO devices (device_id, customer_id)
      SELECT DISTINCT device_id, $1
      FROM (
        SELECT device_id FROM device_contact_info
        UNION
        SELECT device_id FROM device_desired_settings
        UNION
        SELECT device_id FROM device_alarm_log
      ) existing_devices
      WHERE device_id IS NOT NULL AND device_id <> ''
      ON CONFLICT (device_id) DO NOTHING
    `,
    [DEFAULT_CUSTOMER_ID]
  );
}

async function seedInitialAuthData() {
  if (!db) {
    return;
  }

  if (!ADMIN_EMAIL || !ADMIN_INITIAL_PASSWORD) {
    app.log.warn("ADMIN_EMAIL or ADMIN_INITIAL_PASSWORD is not configured; no initial admin user was seeded");
    return;
  }

  const email = normalizeEmail(ADMIN_EMAIL);
  const existing = await db.query(
    "SELECT id FROM app_users WHERE lower(email) = lower($1)",
    [email]
  );

  if (existing.rowCount > 0) {
    return;
  }

  await db.query(
    `
      INSERT INTO app_users (
        id,
        customer_id,
        email,
        password_hash,
        role,
        must_change_password,
        active
      )
      VALUES ($1, NULL, $2, $3, 'admin', true, true)
    `,
    [crypto.randomUUID(), email, hashPassword(ADMIN_INITIAL_PASSWORD)]
  );

  app.log.info({ email }, "Seeded initial admin user");
}

async function refreshDeviceRegistry() {
  if (!db) {
    return deviceRegistry;
  }

  const result = await db.query(`
    SELECT device_id, customer_id
    FROM devices
    ORDER BY device_id
  `);

  for (const key of Object.keys(deviceRegistry)) {
    delete deviceRegistry[key];
  }

  for (const row of result.rows) {
    deviceRegistry[row.device_id] = {
      device_id: row.device_id,
      customer_id: row.customer_id
    };
  }

  return deviceRegistry;
}

async function ensureDeviceRegistered(deviceId) {
  if (!isValidDeviceId(deviceId)) {
    return null;
  }

  if (deviceRegistry[deviceId]) {
    return deviceRegistry[deviceId];
  }

  if (!db) {
    deviceRegistry[deviceId] = {
      device_id: deviceId,
      customer_id: DEFAULT_CUSTOMER_ID
    };
    return deviceRegistry[deviceId];
  }

  const result = await db.query(
    `
      INSERT INTO devices (device_id, customer_id)
      VALUES ($1, $2)
      ON CONFLICT (device_id)
      DO UPDATE SET device_id = EXCLUDED.device_id
      RETURNING device_id, customer_id
    `,
    [deviceId, DEFAULT_CUSTOMER_ID]
  );

  deviceRegistry[deviceId] = {
    device_id: result.rows[0].device_id,
    customer_id: result.rows[0].customer_id
  };

  return deviceRegistry[deviceId];
}

function customerFromRow(row) {
  return {
    id: row.id,
    name: row.name,
    subscription_status: row.subscription_status,
    subscription_plan: row.subscription_plan,
    paid_until: row.paid_until ? new Date(row.paid_until).toISOString() : null,
    login_enabled: row.login_enabled !== false,
    updated_at: row.updated_at
  };
}

async function listCustomers() {
  if (!db) {
    return [];
  }

  const result = await db.query(`
    SELECT
      id,
      name,
      subscription_status,
      subscription_plan,
      paid_until,
      login_enabled,
      updated_at
    FROM customers
    ORDER BY name, id
  `);

  return result.rows.map(customerFromRow);
}

async function saveCustomerSubscription(customerId, values) {
  if (!db) {
    const error = new Error("Database is not configured");
    error.statusCode = 503;
    throw error;
  }

  const status = String(values.subscription_status || "").trim().toLowerCase();
  const plan = String(values.subscription_plan || "").trim().toLowerCase();
  const loginEnabled = values.login_enabled === undefined
    ? true
    : values.login_enabled === true || values.login_enabled === "true";
  const paidUntilInput = values.paid_until === undefined || values.paid_until === null
    ? null
    : String(values.paid_until).trim();

  if (!VALID_SUBSCRIPTION_STATUSES.has(status)) {
    const error = new Error("Invalid subscription status");
    error.statusCode = 400;
    throw error;
  }

  if (!VALID_SUBSCRIPTION_PLANS.has(plan)) {
    const error = new Error("Invalid subscription plan");
    error.statusCode = 400;
    throw error;
  }

  let paidUntil = null;

  if (paidUntilInput) {
    const parsed = new Date(paidUntilInput);

    if (Number.isNaN(parsed.getTime())) {
      const error = new Error("Invalid paid_until date");
      error.statusCode = 400;
      throw error;
    }

    paidUntil = parsed.toISOString();
  }

  const result = await db.query(
    `
      UPDATE customers
      SET
        subscription_status = $2,
        subscription_plan = $3,
        paid_until = $4,
        login_enabled = $5,
        updated_at = now()
      WHERE id = $1
      RETURNING
        id,
        name,
        subscription_status,
        subscription_plan,
        paid_until,
        login_enabled,
        updated_at
    `,
    [customerId, status, plan, paidUntil, loginEnabled]
  );

  if (result.rowCount === 0) {
    const error = new Error("Customer not found");
    error.statusCode = 404;
    throw error;
  }

  return customerFromRow(result.rows[0]);
}

function topicDeviceId(topic) {
  const parts = String(topic || "").split("/");

  if (parts[0] === "snjalli" && parts[1]) {
    return parts[1];
  }

  if (parts[0] === "alarm" && parts[1]) {
    return parts[1];
  }

  return null;
}

function canAccessDevice(user, deviceId) {
  if (!user || !deviceId) {
    return false;
  }

  if (isAdminUser(user)) {
    return true;
  }

  const registry = deviceRegistry[deviceId];
  return Boolean(registry && registry.customer_id === user.customer_id);
}

function filterRawStateForUser(user) {
  if (isAdminUser(user)) {
    return latestState;
  }

  const filtered = {};

  for (const [topic, value] of Object.entries(latestState)) {
    const deviceId = topicDeviceId(topic);

    if (canAccessDevice(user, deviceId)) {
      filtered[topic] = value;
    }
  }

  return filtered;
}

function filterDevicesForUser(user, deviceViews) {
  if (isAdminUser(user)) {
    return deviceViews;
  }

  return deviceViews.filter((device) => canAccessDevice(user, device.device_id));
}

function filterAlarmsForUser(user, alarms) {
  if (isAdminUser(user)) {
    return alarms;
  }

  return alarms.filter((alarm) => canAccessDevice(user, alarm.device_id));
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
      customer_id,
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
    customer_id: deviceRegistry[deviceId]?.customer_id || DEFAULT_CUSTOMER_ID,
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

  if (alarmType === "device_offline") {
    return device.last_seen || null;
  }

  return null;
}

async function addAlarmEvent(deviceId, alarmType, payload, topic, alarmValue = null) {
  if (!db) {
    addMemoryAlarmEvent(deviceId, alarmType, payload, topic, alarmValue);
    return;
  }

  const customerId = deviceRegistry[deviceId]?.customer_id || DEFAULT_CUSTOMER_ID;
  const result = await db.query(
    `
      INSERT INTO device_alarm_log (
        customer_id,
        device_id,
        alarm_type,
        alarm_value,
        payload,
        source_topic,
        status,
        message
      )
      SELECT $1, $2, $3, $4, $5, $6, 'ACTIVE', $7
      WHERE NOT EXISTS (
        SELECT 1
        FROM device_alarm_log
        WHERE device_id = $2
          AND alarm_type = $3
          AND status = 'ACTIVE'
          AND cleared_at IS NULL
      )
      RETURNING id
    `,
    [
      customerId,
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

async function checkOfflineDevices() {
  const now = Date.now();
  let changed = false;

  for (const device of Object.values(devices)) {
    const { hasLastSeen, isOffline } = deviceOfflineInfo(device, now);
    const offlineAlarmActive = device.alarms.device_offline === "ACTIVE";

    if (hasLastSeen && isOffline && !offlineAlarmActive) {
      device.status = "offline";
      device.alarm_state = "ALARM";
      device.alarms.device_offline = "ACTIVE";

      await addAlarmEvent(
        device.device_id,
        "device_offline",
        "ACTIVE",
        `backend/${device.device_id}/alarm/device_offline`,
        currentAlarmValue(device, "device_offline")
      );

      changed = true;
    }

    if (!isOffline && offlineAlarmActive) {
      device.alarms.device_offline = "OK";
      await clearAlarmEvent(device.device_id, "device_offline");

      if (!deviceHasActiveAlarm(device)) {
        device.alarm_state = "OK";
      }

      changed = true;
    }
  }

  if (changed) {
    publishWebSocketState();
  }
}

function publishWebSocketState() {
  for (const [socket, user] of wsClients) {
    if (socket.readyState === 1) {
      const message = JSON.stringify({
        type: "state",
        data: getDashboardState(user)
      });
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
  await ensureDeviceRegistered(deviceId);
  const device = getDevice(deviceId);

  device.last_seen = new Date().toISOString();

  if (device.alarms.device_offline === "ACTIVE") {
    device.alarms.device_offline = "OK";
    await clearAlarmEvent(deviceId, "device_offline");

    if (!deviceHasActiveAlarm(device)) {
      device.alarm_state = "OK";
    }
  }

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

app.get("/alarms", async (req, reply) => {
  return reply.type("text/html").send(dashboardHtml);
});

app.post("/api/v1/login", async (req, reply) => {
  if (!db) {
    reply.code(503);
    return {
      ok: false,
      error: "Database login is not configured"
    };
  }

  const email = normalizeEmail(req.body?.email);
  const password = String(req.body?.password || "");

  if (!email || !password) {
    reply.code(400);
    return {
      ok: false,
      error: "Email and password are required"
    };
  }

  const result = await db.query(
    `
      SELECT
        u.id,
        u.customer_id,
        u.email,
        u.password_hash,
        u.role,
        u.must_change_password,
        u.active,
        c.subscription_status,
        c.subscription_plan,
        c.paid_until,
        c.login_enabled
      FROM app_users u
      LEFT JOIN customers c ON c.id = u.customer_id
      WHERE lower(email) = lower($1)
      LIMIT 1
    `,
    [email]
  );

  const row = result.rows[0];

  if (!row || !row.active || !verifyPassword(password, row.password_hash)) {
    reply.code(401);
    return {
      ok: false,
      error: "Invalid email or password"
    };
  }

  const user = userFromRow(row);

  if (!userCanUseDashboard(user)) {
    reply.code(402);
    return {
      ok: false,
      error: "Subscription inactive",
      subscription: user.subscription || null
    };
  }

  const sessionId = createDashboardSession(user);

  reply.header("Set-Cookie", sessionCookie(sessionId, req));

  return {
    ok: true,
    user: publicUser(user)
  };
});

app.post("/api/v1/logout", { preHandler: checkAuth }, async (req, reply) => {
  const cookies = parseCookies(req.headers.cookie || "");

  if (cookies.snjallhus_session) {
    dashboardSessions.delete(cookies.snjallhus_session);
  }

  reply.header("Set-Cookie", clearSessionCookie(req));

  return { ok: true };
});

app.get("/api/v1/me", { preHandler: checkAuth }, async (req) => {
  return {
    ok: true,
    user: publicUser(req.user)
  };
});

app.post("/api/v1/change-password", { preHandler: checkAuth }, async (req, reply) => {
  if (!db) {
    reply.code(503);
    return {
      ok: false,
      error: "Database login is not configured"
    };
  }

  if (req.user.id === "api-token-admin") {
    reply.code(400);
    return {
      ok: false,
      error: "The API token admin cannot change password"
    };
  }

  const currentPassword = String(req.body?.current_password || "");
  const newPassword = String(req.body?.new_password || "");
  const newPasswordConfirm = String(req.body?.new_password_confirm || "");

  if (newPassword.length < 10) {
    reply.code(400);
    return {
      ok: false,
      error: "New password must be at least 10 characters"
    };
  }

  if (newPassword !== newPasswordConfirm) {
    reply.code(400);
    return {
      ok: false,
      error: "New passwords do not match"
    };
  }

  const result = await db.query(
    `
      SELECT id, password_hash
      FROM app_users
      WHERE id = $1 AND active = true
      LIMIT 1
    `,
    [req.user.id]
  );

  const row = result.rows[0];

  if (!row || !verifyPassword(currentPassword, row.password_hash)) {
    reply.code(401);
    return {
      ok: false,
      error: "Current password is not correct"
    };
  }

  await db.query(
    `
      UPDATE app_users
      SET password_hash = $2,
          must_change_password = false,
          updated_at = now()
      WHERE id = $1
    `,
    [req.user.id, hashPassword(newPassword)]
  );

  req.user.must_change_password = false;

  return {
    ok: true,
    user: publicUser(req.user)
  };
});

app.post("/api/v1/session", { preHandler: checkAuth }, async (req, reply) => {
  const sessionId = createDashboardSession(req.user);

  reply.header("Set-Cookie", sessionCookie(sessionId, req));

  return {
    ok: true,
    user: publicUser(req.user)
  };
});

app.get("/api/v1/state", { preHandler: checkAuth }, async (req) => {
  return getDashboardState(req.user);
});

app.get("/api/v1/devices", { preHandler: checkAuth }, async (req) => {
  return getDashboardState(req.user).devices;
});

app.get("/api/v1/admin/customers", { preHandler: checkAuth }, async (req, reply) => {
  if (!isAdminUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  return {
    ok: true,
    customers: await listCustomers()
  };
});

app.patch("/api/v1/admin/customers/:customerId/subscription", { preHandler: checkAuth }, async (req, reply) => {
  if (!isAdminUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  const customerId = String(req.params.customerId || "").trim();

  if (!customerId) {
    reply.code(400);
    return {
      ok: false,
      error: "Customer ID is required"
    };
  }

  try {
    const customer = await saveCustomerSubscription(customerId, req.body || {});

    return {
      ok: true,
      customer
    };
  } catch (error) {
    reply.code(error.statusCode || 500);
    return {
      ok: false,
      error: error.message
    };
  }
});

app.get("/api/v1/devices/:deviceId/settings/device", { preHandler: checkDeviceAuth }, async (req, reply) => {
  const deviceId = String(req.params.deviceId || "").trim();

  if (!isValidDeviceId(deviceId)) {
    reply.code(400);
    return {
      ok: false,
      error: "Invalid device ID"
    };
  }

  await ensureDeviceRegistered(deviceId);
  return desiredSettingsResponse(deviceId);
});

app.patch("/api/v1/devices/:deviceId/contact", { preHandler: checkAuth }, async (req, reply) => {
  const deviceId = String(req.params.deviceId || "").trim();

  if (!isValidDeviceId(deviceId)) {
    reply.code(400);
    return {
      ok: false,
      error: "Invalid device ID"
    };
  }

  if (isAdminUser(req.user)) {
    await ensureDeviceRegistered(deviceId);
  }

  if (!canAccessDevice(req.user, deviceId)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
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

  if (!isValidDeviceId(deviceId)) {
    reply.code(400);
    return {
      ok: false,
      error: "Invalid device ID"
    };
  }

  if (isAdminUser(req.user)) {
    await ensureDeviceRegistered(deviceId);
  }

  if (!canAccessDevice(req.user, deviceId)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
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

app.get("/api/v1/alarms", { preHandler: checkAuth }, async (req) => {
  const alarms = await refreshRecentAlarms();
  return filterAlarmsForUser(req.user, alarms);
});

app.post("/api/v1/mqtt/publish", { preHandler: checkAuth }, async (req, reply) => {
  if (!isAdminUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

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
  const sessionUser = getSessionUser(req);

  (async () => {
    const user = await refreshUserForAccess(sessionUser);

    if (!user) {
      socket.close(1008, "Unauthorized");
      return;
    }

    if (!userCanUseDashboard(user)) {
      socket.close(1008, "Subscription inactive");
      return;
    }

    wsClients.set(socket, user);

    socket.send(JSON.stringify({
      type: "state",
      data: getDashboardState(user)
    }));

    socket.on("close", () => {
      wsClients.delete(socket);
    });
  })().catch((error) => {
    app.log.error({ error }, "Failed to authorize websocket");
    socket.close(1011, "Server error");
  });
});

setInterval(publishWebSocketState, 10000);
setInterval(() => {
  checkOfflineDevices().catch((error) => {
    app.log.error({ error }, "Failed to check offline devices");
  });
}, 10000);

await initDatabase();
await seedInitialAuthData();
await refreshDeviceRegistry();
await refreshDeviceContacts();
await refreshDeviceSettings();
await refreshRecentAlarms();

app.listen({ port: PORT, host: "0.0.0.0" });
