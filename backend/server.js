import Fastify from "fastify";
import websocket from "@fastify/websocket";
import mqtt from "mqtt";
import nodemailer from "nodemailer";
import crypto from "node:crypto";
import fs from "node:fs";
import pg from "pg";

const app = Fastify({ logger: true });
const { Pool } = pg;

await app.register(websocket);

app.addHook("onRequest", async (req, reply) => {
  reply.header("X-Content-Type-Options", "nosniff");
  reply.header("X-Frame-Options", "DENY");
  reply.header("Referrer-Policy", "strict-origin-when-cross-origin");
  reply.header("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
  reply.header("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
});

const PORT = Number(process.env.PORT || 3000);
const API_TOKEN = process.env.API_TOKEN || "";
const DEVICE_API_TOKEN = process.env.DEVICE_API_TOKEN || "";
const DEVICE_OFFLINE_GRACE_MS = Number(process.env.DEVICE_OFFLINE_GRACE_MS || 120000);
const TELEMETRY_LOG_INTERVAL_MS = Math.max(10000, Number(process.env.TELEMETRY_LOG_INTERVAL_MS || 300000));
const DATABASE_URL = process.env.DATABASE_URL || "";
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || "";
const ADMIN_INITIAL_PASSWORD = process.env.ADMIN_INITIAL_PASSWORD || "";
const DEFAULT_CUSTOMER_ID = process.env.DEFAULT_CUSTOMER_ID || "triotech";
const DEFAULT_CUSTOMER_NAME = process.env.DEFAULT_CUSTOMER_NAME || "Triotech";
const PUBLIC_APP_URL = (process.env.PUBLIC_APP_URL || "").replace(/\/+$/, "");
const ALERT_EMAIL_ENABLED = String(process.env.ALERT_EMAIL_ENABLED || "false").toLowerCase() === "true";
const ALERT_EMAIL_BATCH_MS = Math.max(0, Number(process.env.ALERT_EMAIL_BATCH_MS || 120000));
const DEVICE_SETTING_WEB_PENDING_MS = Math.max(0, Number(process.env.DEVICE_SETTING_WEB_PENDING_MS || 300000));
const SMTP_HOST = process.env.SMTP_HOST || "";
const SMTP_PORT = Number(process.env.SMTP_PORT || 587);
const SMTP_SECURE = String(process.env.SMTP_SECURE || "false").toLowerCase() === "true";
const SMTP_USER = process.env.SMTP_USER || "";
const SMTP_PASS = process.env.SMTP_PASS || "";
const SMTP_FROM = process.env.SMTP_FROM || SMTP_USER;
const SESSION_MAX_AGE_MS = 12 * 60 * 60 * 1000;
const LOGO_FILE = new URL("./public/LOGO_Triotech.png", import.meta.url);
const PASSWORD_RESET_MAX_AGE_MS = 30 * 60 * 1000;
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
const notificationEmailSettings = {};
const pendingDesiredSettings = {};
let recentAlarms = [];
const wsClients = new Map();
const dashboardSessions = new Map();
let mailTransporter = null;
const alarmEmailBatches = new Map();
const lastTelemetryLogAt = {};
const pendingTelemetryLogTimers = {};

const dashboardHtml = String.raw`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Snjalli H&uacute;sv&ouml;r&eth;urinn</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #1e2939;
      --panel: #243247;
      --panel-soft: #2b3a51;
      --input: #182335;
      --line: #41506a;
      --text: #eef7ff;
      --muted: #a9b8cc;
      --accent: #7dd3fc;
      --accent-strong: #38bdf8;
      --accent-dark: #0e7490;
      --alarm: #3f1f25;
      --alarm-line: #ef4444;
      --offline: #283044;
      --warn-bg: #3a3323;
      --warn-line: #f59e0b;
      --ok: #42df95;
      --warn: #f6c453;
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

    a,
    a:visited {
      color: var(--accent);
    }

    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 22px;
      background: #172235;
      border-bottom: 1px solid var(--line);
    }

    .brand-lockup {
      display: flex;
      align-items: center;
      gap: 12px;
      min-width: 230px;
    }

    .brand-logo {
      width: 54px;
      height: 54px;
      border-radius: 8px;
      object-fit: cover;
      background: var(--bg);
      border: 1px solid rgba(255, 255, 255, 0.12);
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
      background: var(--panel-soft);
    }

    .nav-links a.active {
      border-color: var(--accent-strong);
      color: var(--accent);
      font-weight: 700;
    }

    input {
      width: min(420px, 70vw);
      height: 34px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 0 10px;
      font: inherit;
      background: var(--input);
      color: var(--text);
    }

    input::placeholder,
    textarea::placeholder {
      color: #8293aa;
    }

    button {
      height: 34px;
      border: 1px solid var(--accent-strong);
      border-radius: 6px;
      background: var(--accent-dark);
      color: #ecfeff;
      padding: 0 12px;
      font: inherit;
      cursor: pointer;
    }

    .secondary-button {
      background: transparent;
      color: var(--accent);
    }

    .row-input {
      width: 12ch;
      height: 30px;
      padding: 0 6px;
      background: var(--input);
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

    select,
    textarea {
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px 10px;
      font: inherit;
      background: var(--input);
      color: var(--text);
    }

    select {
      height: 34px;
    }

    textarea {
      width: min(560px, 100%);
      min-height: 150px;
      resize: vertical;
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
      background: var(--warn-bg);
      border: 1px solid var(--warn-line);
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
      background: #1a2638;
      font-size: 12px;
      color: #cfe1f5;
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
      line-height: 1.15;
      white-space: normal;
    }

    .sort-label {
      display: inline-flex;
      flex-direction: column;
      gap: 1px;
    }

    .sort-label-line {
      display: block;
      white-space: nowrap;
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
      background: var(--offline);
    }

    tr.offline td:first-child {
      border-left: 4px solid var(--warn-line);
    }

    .badge {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 2px 8px;
      border-radius: 999px;
      background: #33445d;
      color: var(--text);
      font-size: 12px;
      font-weight: 700;
    }

    .badge.ok {
      background: #173d31;
      color: var(--ok);
    }

    .badge.alarm {
      background: var(--alarm);
      color: #ffd5d5;
    }

    .badge.warn {
      background: var(--warn-bg);
      color: var(--warn);
    }

    section {
      margin-top: 18px;
    }

    h2 {
      margin: 0 0 10px;
      font-size: 16px;
    }

    .settings-panel {
      display: grid;
      gap: 12px;
      max-width: 680px;
      padding: 16px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }

    .settings-row {
      display: grid;
      gap: 6px;
    }

    .settings-row label {
      font-weight: 700;
    }

    .settings-actions {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }

    .super-admin-section {
      display: grid;
      gap: 16px;
      margin-top: 20px;
    }

    .admin-panel {
      display: grid;
      gap: 10px;
      padding: 14px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }

    .admin-panel h3 {
      margin: 0;
      font-size: 14px;
    }

    .admin-form {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }

    .admin-form input,
    .admin-form select,
    .admin-table input,
    .admin-table select {
      width: 16ch;
      height: 30px;
      padding: 0 8px;
    }

    .admin-form .wide-input,
    .admin-table .wide-input {
      width: 28ch;
    }

    .admin-table {
      min-width: 860px;
      font-size: 13px;
    }

    .admin-table th,
    .admin-table td {
      padding: 7px 6px;
    }

    .danger-button {
      border-color: var(--alarm-line);
      background: #9f1d2f;
    }

    .status-message {
      min-height: 18px;
      color: var(--muted);
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

    .clickable-row {
      cursor: pointer;
    }

    .device-link {
      color: var(--accent);
      font-weight: 700;
      text-decoration: none;
    }

    .device-link:visited {
      color: var(--accent);
    }

    .device-link:hover {
      text-decoration: underline;
    }

    .detail-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }

    .detail-header h2 {
      margin: 0;
      font-size: 20px;
    }

    .detail-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(150px, 1fr));
      gap: 10px;
      margin-bottom: 18px;
    }

    .detail-item {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      min-height: 70px;
    }

    .detail-label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0;
      margin-bottom: 5px;
    }

    .detail-value {
      font-size: 18px;
      font-weight: 700;
      white-space: normal;
      overflow-wrap: anywhere;
    }

    .range-bar {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 10px;
    }

    .range-button.active {
      background: var(--accent-dark);
      border-color: var(--accent-strong);
    }

    .chart-wrap {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      margin-bottom: 12px;
      overflow: auto;
    }

    .telemetry-chart {
      width: 100%;
      min-width: 640px;
      height: 280px;
      display: block;
    }

    .chart-empty {
      fill: var(--muted);
      font-size: 14px;
    }

    .detail-table table {
      min-width: 720px;
    }

    @media (max-width: 900px) {
      .detail-grid {
        grid-template-columns: repeat(2, minmax(150px, 1fr));
      }
    }

    @media (max-width: 560px) {
      header {
        align-items: flex-start;
        flex-direction: column;
      }

      .nav-links {
        margin-left: 0;
      }

      .detail-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <header>
    <div class="brand-lockup">
      <img class="brand-logo" src="/assets/LOGO_Triotech.png" alt="Triotech">
      <h1 id="pageTitle">Snjalli H&uacute;sv&ouml;r&eth;urinn</h1>
    </div>
    <nav class="nav-links" aria-label="Main pages">
      <a id="devicesLink" href="/dashboard">T&aelig;kjaskr&aacute;</a>
      <a id="alarmsLink" href="/alarms">Vi&eth;v&ouml;runarskr&aacute;</a>
      <a id="settingsLink" href="/settings">Stillingar</a>
    </nav>
    <div class="toolbar">
      <span id="userInfo" class="user-info"></span>
      <button id="languageToggle" type="button">EN</button>
      <input id="emailInput" class="auth-input" type="email" autocomplete="username" placeholder="Netfang">
      <input id="passwordInput" class="auth-input" type="password" autocomplete="current-password" placeholder="Lykilor&eth;">
      <button id="loginButton">Innskr&aacute;</button>
      <button id="forgotPasswordButton" class="secondary-button" type="button">Gleymt lykilor&eth;?</button>
      <button id="logoutButton" hidden>&Uacute;tskr&aacute;</button>
    </div>
  </header>

  <main>
    <div class="status">
      <span><span id="apiLabel">API:</span> <strong id="apiStatus">b&iacute;&eth;</strong></span>
      <span><span id="wsLabel">WebSocket:</span> <strong id="wsStatus">b&iacute;&eth;</strong></span>
      <span><span id="deviceCountLabel">T&aelig;ki:</span> <strong id="deviceCount">0</strong></span>
      <span><span id="lastUpdateLabel">S&iacute;&eth;asta uppf&aelig;rsla:</span> <strong id="lastUpdate">aldrei</strong></span>
    </div>

    <section id="passwordChangeSection" class="password-panel" hidden>
      <strong id="passwordChangeTitle">Breyta &thorn;arf lykilor&eth;i</strong>
      <input id="currentPasswordInput" class="auth-input" type="password" autocomplete="current-password" placeholder="N&uacute;verandi lykilor&eth;">
      <input id="newPasswordInput" class="auth-input" type="password" autocomplete="new-password" placeholder="N&yacute;tt lykilor&eth;">
      <input id="confirmPasswordInput" class="auth-input" type="password" autocomplete="new-password" placeholder="Sta&eth;festa n&yacute;tt lykilor&eth;">
      <button id="changePasswordButton">Breyta lykilor&eth;i</button>
      <span id="passwordChangeStatus"></span>
    </section>

    <section id="passwordResetSection" class="password-panel" hidden>
      <strong id="passwordResetTitle">Endursetja lykilor&eth;</strong>
      <input id="resetPasswordInput" class="auth-input" type="password" autocomplete="new-password" placeholder="N&yacute;tt lykilor&eth;">
      <input id="resetPasswordConfirmInput" class="auth-input" type="password" autocomplete="new-password" placeholder="Sta&eth;festa n&yacute;tt lykilor&eth;">
      <button id="resetPasswordButton">Endursetja lykilor&eth;</button>
      <span id="passwordResetStatus"></span>
    </section>

    <section id="deviceSection">
      <div class="filter-bar">
        <label id="deviceFilterLabel" for="deviceFilter">S&iacute;a</label>
        <input id="deviceFilter" class="filter-input" type="search" autocomplete="off" placeholder="Leita a&eth; t&aelig;ki, heimilisfangi, s&iacute;ma, st&ouml;&eth;u, afli, vi&eth;v&ouml;run...">
        <button id="clearDeviceFilter" type="button">Hreinsa</button>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th><button class="sort-button" data-sort-key="device_id" data-sort-type="text">T&aelig;ki <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="phone_number" data-sort-type="text">S&iacute;mi <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="address" data-sort-type="text">Heimilisfang <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="connection_state" data-sort-type="text">Sta&eth;a <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="last_seen" data-sort-type="time">S&iacute;&eth;ast s&eacute;&eth; <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="temperature" data-sort-type="number">Hiti (C) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="humidity" data-sort-type="number">Raki (%) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="power_source" data-sort-type="text">Aflgjafi <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="ble_power_monitor_installed" data-sort-type="text">Auka aflvaki <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="ble_power_monitor_connection" data-sort-type="text">Auka aflvaki sta&eth;a <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="low_temperature_display" data-sort-type="number">Settur l&aacute;gur hiti (C) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="high_temperature_display" data-sort-type="number">Settur h&aacute;r hiti (C) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="high_humidity_display" data-sort-type="number">Settur h&aacute;r raki (%) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="telemetry_interval_display" data-sort-type="number">Uppf&aelig;rslu-t&iacute;&eth;ni (s) <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="alarm_state" data-sort-type="text">Vi&eth;v&ouml;run <span class="sort-indicator"></span></button></th>
              <th id="saveHeader">Vista</th>
            </tr>
          </thead>
          <tbody id="deviceRows">
            <tr><td colspan="16" class="empty">Engin t&aelig;kjag&ouml;gn s&oacute;tt.</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="deviceDetailSection" hidden>
      <div class="detail-header">
        <div>
          <a id="deviceDetailBack" class="device-link" href="/dashboard">Til baka &iacute; t&aelig;kjaskr&aacute;</a>
          <h2 id="deviceDetailTitle">T&aelig;ki</h2>
        </div>
        <span id="deviceDetailStatus"></span>
      </div>
      <div id="deviceDetailSummary" class="detail-grid"></div>

      <h2 id="deviceTagsTitle">N&uacute;verandi gildi</h2>
      <div class="table-wrap detail-table">
        <table>
          <thead>
            <tr>
              <th id="deviceTagHeader">Tag</th>
              <th id="deviceTagValueHeader">Gildi</th>
            </tr>
          </thead>
          <tbody id="deviceTagRows">
            <tr><td colspan="2" class="empty">Engin t&aelig;kjag&ouml;gn s&oacute;tt.</td></tr>
          </tbody>
        </table>
      </div>

      <h2 id="deviceTelemetryTitle">Hiti og raki</h2>
      <div class="range-bar">
        <button class="range-button active" type="button" data-range="24h">24 t&iacute;mar</button>
        <button class="range-button" type="button" data-range="7d">7 dagar</button>
        <button class="range-button" type="button" data-range="30d">30 dagar</button>
        <span id="telemetryStatus"></span>
      </div>
      <div class="chart-wrap">
        <svg id="telemetryChart" class="telemetry-chart" viewBox="0 0 760 280" role="img" aria-label="Telemetry graph"></svg>
      </div>
      <div class="table-wrap detail-table">
        <table>
          <thead>
            <tr>
              <th id="telemetryTimeHeader">T&iacute;mi</th>
              <th id="telemetryTempHeader">Hiti (C)</th>
              <th id="telemetryHumidityHeader">Raki (%)</th>
              <th id="telemetryPowerHeader">Aflgjafi</th>
              <th id="telemetryAlarmHeader">Vi&eth;v&ouml;run</th>
            </tr>
          </thead>
          <tbody id="telemetryRows">
            <tr><td colspan="5" class="empty">Engin m&aelig;lig&ouml;gn hafa veri&eth; skr&aacute;&eth;.</td></tr>
          </tbody>
        </table>
      </div>

      <h2 id="deviceAlarmTitle">Vi&eth;varanir fyrir t&aelig;ki</h2>
      <div class="table-wrap detail-table">
        <table>
          <thead>
            <tr>
              <th id="deviceAlarmTimeHeader">T&iacute;mi</th>
              <th id="deviceAlarmClearedHeader">Hreinsa&eth;</th>
              <th id="deviceAlarmTypeHeader">Vi&eth;v&ouml;run</th>
              <th id="deviceAlarmTopicHeader">Topic</th>
            </tr>
          </thead>
          <tbody id="deviceAlarmRows">
            <tr><td colspan="4" class="empty">Engin vi&eth;v&ouml;runarsaga hefur borist.</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="alarmSection">
      <h2 id="alarmTitle">Vi&eth;v&ouml;runarskr&aacute;</h2>
      <div class="filter-bar">
        <label id="alarmFilterLabel" for="alarmFilter">S&iacute;a</label>
        <input id="alarmFilter" class="filter-input" type="search" autocomplete="off" placeholder="Leita a&eth; t&iacute;ma, t&aelig;ki, vi&eth;v&ouml;run, topic, st&ouml;&eth;u...">
        <button id="clearAlarmFilter" type="button">Hreinsa</button>
        <span><span id="shownLabel">S&yacute;nt:</span> <strong id="alarmCount">0</strong></span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th><button class="sort-button" data-sort-key="created_at" data-sort-type="time">T&iacute;mi <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="cleared_at" data-sort-type="time">Hreinsa&eth; <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="device_id" data-sort-type="text">T&aelig;ki <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="alarm_type" data-sort-type="text">Vi&eth;v&ouml;run <span class="sort-indicator"></span></button></th>
              <th><button class="sort-button" data-sort-key="source_topic" data-sort-type="text">Topic <span class="sort-indicator"></span></button></th>
            </tr>
          </thead>
          <tbody id="alarmRows">
            <tr><td colspan="5" class="empty">Engin vi&eth;v&ouml;runarsaga hefur borist.</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="settingsSection">
      <h2 id="settingsTitle">Stillingar</h2>
      <div class="settings-panel">
        <div id="settingsCustomerWrap" class="settings-row" hidden>
          <label id="settingsCustomerLabel" for="settingsCustomerSelect">Vi&eth;skiptavinur</label>
          <select id="settingsCustomerSelect"></select>
        </div>
        <div class="settings-row">
          <label id="notificationEmailsLabel" for="notificationEmailsInput">Vi&eth;v&ouml;runar netf&ouml;ng</label>
          <textarea id="notificationEmailsInput" spellcheck="false" autocomplete="off" placeholder="eitt netfang &iacute; hverja l&iacute;nu"></textarea>
        </div>
        <div class="settings-actions">
          <button id="saveNotificationEmails" type="button">Vista netf&ouml;ng</button>
          <span id="notificationEmailsStatus"></span>
        </div>
      </div>

      <div id="superAdminSection" class="super-admin-section" hidden>
        <h2 id="superAdminTitle">Kerfisstj&oacute;rn</h2>

        <div id="adminCustomersPanel" class="admin-panel">
          <h3 id="adminCustomersTitle">Vi&eth;skiptavinir</h3>
          <div class="admin-form">
            <input id="adminCustomerIdInput" type="text" autocomplete="off" placeholder="customer-id">
            <input id="adminCustomerNameInput" class="wide-input" type="text" autocomplete="off" placeholder="Nafn">
            <select id="adminCustomerStatusSelect"></select>
            <select id="adminCustomerPlanSelect"></select>
            <input id="adminCustomerPaidUntilInput" type="date">
            <select id="adminCustomerLoginSelect"></select>
            <button id="createCustomerButton" type="button">B&uacute;a til</button>
            <span id="adminCustomersStatus" class="status-message"></span>
          </div>
          <div class="table-wrap">
            <table class="admin-table">
              <thead>
                <tr>
                  <th id="adminCustomerIdHeader"><button class="sort-button" data-admin-sort="customers" data-sort-key="id" data-sort-type="text">ID <span class="sort-indicator"></span></button></th>
                  <th id="adminCustomerNameHeader"><button class="sort-button" data-admin-sort="customers" data-sort-key="name" data-sort-type="text">Nafn <span class="sort-indicator"></span></button></th>
                  <th id="adminCustomerStatusHeader"><button class="sort-button" data-admin-sort="customers" data-sort-key="subscription_status" data-sort-type="text">Sta&eth;a <span class="sort-indicator"></span></button></th>
                  <th id="adminCustomerPlanHeader"><button class="sort-button" data-admin-sort="customers" data-sort-key="subscription_plan" data-sort-type="text">&Aacute;skrift <span class="sort-indicator"></span></button></th>
                  <th id="adminCustomerPaidUntilHeader"><button class="sort-button" data-admin-sort="customers" data-sort-key="paid_until" data-sort-type="time">Greitt til <span class="sort-indicator"></span></button></th>
                  <th id="adminCustomerLoginHeader"><button class="sort-button" data-admin-sort="customers" data-sort-key="login_enabled" data-sort-type="text">A&eth;gangur <span class="sort-indicator"></span></button></th>
                  <th id="adminCustomerSaveHeader">Vista</th>
                  <th id="adminCustomerDeleteHeader">Ey&eth;a</th>
                </tr>
              </thead>
              <tbody id="adminCustomerRows">
                <tr><td colspan="8" class="empty">Engir vi&eth;skiptavinir.</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <div id="adminUsersPanel" class="admin-panel">
          <h3 id="adminUsersTitle">Notendur</h3>
          <div class="admin-form">
            <input id="adminUserEmailInput" class="wide-input" type="email" autocomplete="off" placeholder="netfang">
            <select id="adminUserCustomerSelect"></select>
            <select id="adminUserRoleSelect"></select>
            <button id="createUserButton" type="button">B&aelig;ta vi&eth;</button>
            <span id="adminUsersStatus" class="status-message"></span>
          </div>
          <div class="table-wrap">
            <table class="admin-table">
              <thead>
                <tr>
                  <th id="adminUserEmailHeader"><button class="sort-button" data-admin-sort="users" data-sort-key="email" data-sort-type="text">Netfang <span class="sort-indicator"></span></button></th>
                  <th id="adminUserCustomerHeader"><button class="sort-button" data-admin-sort="users" data-sort-key="customer_name" data-sort-type="text">Vi&eth;skiptavinur <span class="sort-indicator"></span></button></th>
                  <th id="adminUserRoleHeader"><button class="sort-button" data-admin-sort="users" data-sort-key="role" data-sort-type="text">Hlutverk <span class="sort-indicator"></span></button></th>
                  <th id="adminUserActiveHeader">Virkur</th>
                  <th id="adminUserMustChangeHeader">N&yacute;tt lykilor&eth;</th>
                  <th id="adminUserSaveHeader">Vista</th>
                  <th id="adminUserResetHeader">Endursetja</th>
                  <th id="adminUserWelcomeHeader">Senda</th>
                </tr>
              </thead>
              <tbody id="adminUserRows">
                <tr><td colspan="8" class="empty">Engir notendur.</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <div id="adminDevicesPanel" class="admin-panel">
          <h3 id="adminDevicesTitle">T&aelig;kja&uacute;thlutun</h3>
          <div class="admin-form">
            <input id="adminDeviceIdInput" type="text" autocomplete="off" placeholder="SH1000">
            <select id="adminDeviceCustomerSelect"></select>
            <button id="assignDeviceButton" type="button">Vista</button>
            <span id="adminDevicesStatus" class="status-message"></span>
          </div>
          <div class="table-wrap">
            <table class="admin-table">
              <thead>
                <tr>
                  <th id="adminDeviceIdHeader"><button class="sort-button" data-admin-sort="devices" data-sort-key="device_id" data-sort-type="text">T&aelig;ki <span class="sort-indicator"></span></button></th>
                  <th id="adminDeviceCustomerHeader"><button class="sort-button" data-admin-sort="devices" data-sort-key="customer_name" data-sort-type="text">Vi&eth;skiptavinur <span class="sort-indicator"></span></button></th>
                  <th id="adminDeviceUpdatedHeader"><button class="sort-button" data-admin-sort="devices" data-sort-key="updated_at" data-sort-type="time">Uppf&aelig;rt <span class="sort-indicator"></span></button></th>
                  <th id="adminDeviceSaveHeader">Vista</th>
                  <th id="adminDeviceDeleteHeader">Fjarl&aelig;gja</th>
                </tr>
              </thead>
              <tbody id="adminDeviceRows">
                <tr><td colspan="5" class="empty">Engin t&aelig;ki.</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </section>
  </main>

  <script>
    const emailInput = document.getElementById("emailInput");
    const passwordInput = document.getElementById("passwordInput");
    const loginButton = document.getElementById("loginButton");
    const forgotPasswordButton = document.getElementById("forgotPasswordButton");
    const logoutButton = document.getElementById("logoutButton");
    const languageToggle = document.getElementById("languageToggle");
    const apiStatus = document.getElementById("apiStatus");
    const wsStatus = document.getElementById("wsStatus");
    const pageTitle = document.getElementById("pageTitle");
    const userInfo = document.getElementById("userInfo");
    const apiLabel = document.getElementById("apiLabel");
    const wsLabel = document.getElementById("wsLabel");
    const deviceCountLabel = document.getElementById("deviceCountLabel");
    const lastUpdateLabel = document.getElementById("lastUpdateLabel");
    const passwordChangeSection = document.getElementById("passwordChangeSection");
    const passwordChangeTitle = document.getElementById("passwordChangeTitle");
    const currentPasswordInput = document.getElementById("currentPasswordInput");
    const newPasswordInput = document.getElementById("newPasswordInput");
    const confirmPasswordInput = document.getElementById("confirmPasswordInput");
    const changePasswordButton = document.getElementById("changePasswordButton");
    const passwordChangeStatus = document.getElementById("passwordChangeStatus");
    const passwordResetSection = document.getElementById("passwordResetSection");
    const passwordResetTitle = document.getElementById("passwordResetTitle");
    const resetPasswordInput = document.getElementById("resetPasswordInput");
    const resetPasswordConfirmInput = document.getElementById("resetPasswordConfirmInput");
    const resetPasswordButton = document.getElementById("resetPasswordButton");
    const passwordResetStatus = document.getElementById("passwordResetStatus");
    const deviceCount = document.getElementById("deviceCount");
    const lastUpdate = document.getElementById("lastUpdate");
    const deviceRows = document.getElementById("deviceRows");
    const alarmRows = document.getElementById("alarmRows");
    const deviceSection = document.getElementById("deviceSection");
    const alarmSection = document.getElementById("alarmSection");
    const settingsSection = document.getElementById("settingsSection");
    const devicesLink = document.getElementById("devicesLink");
    const alarmsLink = document.getElementById("alarmsLink");
    const settingsLink = document.getElementById("settingsLink");
    const deviceFilterLabel = document.getElementById("deviceFilterLabel");
    const deviceFilter = document.getElementById("deviceFilter");
    const clearDeviceFilter = document.getElementById("clearDeviceFilter");
    const saveHeader = document.getElementById("saveHeader");
    const alarmTitle = document.getElementById("alarmTitle");
    const alarmFilterLabel = document.getElementById("alarmFilterLabel");
    const alarmFilter = document.getElementById("alarmFilter");
    const clearAlarmFilter = document.getElementById("clearAlarmFilter");
    const shownLabel = document.getElementById("shownLabel");
    const alarmCount = document.getElementById("alarmCount");
    const settingsTitle = document.getElementById("settingsTitle");
    const settingsCustomerWrap = document.getElementById("settingsCustomerWrap");
    const settingsCustomerLabel = document.getElementById("settingsCustomerLabel");
    const settingsCustomerSelect = document.getElementById("settingsCustomerSelect");
    const notificationEmailsLabel = document.getElementById("notificationEmailsLabel");
    const notificationEmailsInput = document.getElementById("notificationEmailsInput");
    const saveNotificationEmails = document.getElementById("saveNotificationEmails");
    const notificationEmailsStatus = document.getElementById("notificationEmailsStatus");
    const superAdminSection = document.getElementById("superAdminSection");
    const superAdminTitle = document.getElementById("superAdminTitle");
    const adminCustomersTitle = document.getElementById("adminCustomersTitle");
    const adminCustomerIdInput = document.getElementById("adminCustomerIdInput");
    const adminCustomerNameInput = document.getElementById("adminCustomerNameInput");
    const adminCustomerStatusSelect = document.getElementById("adminCustomerStatusSelect");
    const adminCustomerPlanSelect = document.getElementById("adminCustomerPlanSelect");
    const adminCustomerPaidUntilInput = document.getElementById("adminCustomerPaidUntilInput");
    const adminCustomerLoginSelect = document.getElementById("adminCustomerLoginSelect");
    const createCustomerButton = document.getElementById("createCustomerButton");
    const adminCustomersStatus = document.getElementById("adminCustomersStatus");
    const adminCustomerRows = document.getElementById("adminCustomerRows");
    const adminCustomerIdHeader = document.getElementById("adminCustomerIdHeader");
    const adminCustomerNameHeader = document.getElementById("adminCustomerNameHeader");
    const adminCustomerStatusHeader = document.getElementById("adminCustomerStatusHeader");
    const adminCustomerPlanHeader = document.getElementById("adminCustomerPlanHeader");
    const adminCustomerPaidUntilHeader = document.getElementById("adminCustomerPaidUntilHeader");
    const adminCustomerLoginHeader = document.getElementById("adminCustomerLoginHeader");
    const adminCustomerSaveHeader = document.getElementById("adminCustomerSaveHeader");
    const adminCustomerDeleteHeader = document.getElementById("adminCustomerDeleteHeader");
    const adminUsersTitle = document.getElementById("adminUsersTitle");
    const adminUserEmailInput = document.getElementById("adminUserEmailInput");
    const adminUserCustomerSelect = document.getElementById("adminUserCustomerSelect");
    const adminUserRoleSelect = document.getElementById("adminUserRoleSelect");
    const createUserButton = document.getElementById("createUserButton");
    const adminUsersStatus = document.getElementById("adminUsersStatus");
    const adminUserRows = document.getElementById("adminUserRows");
    const adminUserEmailHeader = document.getElementById("adminUserEmailHeader");
    const adminUserCustomerHeader = document.getElementById("adminUserCustomerHeader");
    const adminUserRoleHeader = document.getElementById("adminUserRoleHeader");
    const adminUserActiveHeader = document.getElementById("adminUserActiveHeader");
    const adminUserMustChangeHeader = document.getElementById("adminUserMustChangeHeader");
    const adminUserSaveHeader = document.getElementById("adminUserSaveHeader");
    const adminUserResetHeader = document.getElementById("adminUserResetHeader");
    const adminUserWelcomeHeader = document.getElementById("adminUserWelcomeHeader");
    const adminDevicesTitle = document.getElementById("adminDevicesTitle");
    const adminDeviceIdInput = document.getElementById("adminDeviceIdInput");
    const adminDeviceCustomerSelect = document.getElementById("adminDeviceCustomerSelect");
    const assignDeviceButton = document.getElementById("assignDeviceButton");
    const adminDevicesStatus = document.getElementById("adminDevicesStatus");
    const adminDeviceRows = document.getElementById("adminDeviceRows");
    const adminDeviceIdHeader = document.getElementById("adminDeviceIdHeader");
    const adminDeviceCustomerHeader = document.getElementById("adminDeviceCustomerHeader");
    const adminDeviceUpdatedHeader = document.getElementById("adminDeviceUpdatedHeader");
    const adminDeviceSaveHeader = document.getElementById("adminDeviceSaveHeader");
    const adminDeviceDeleteHeader = document.getElementById("adminDeviceDeleteHeader");
    const deviceDetailSection = document.getElementById("deviceDetailSection");
    const deviceDetailBack = document.getElementById("deviceDetailBack");
    const deviceDetailTitle = document.getElementById("deviceDetailTitle");
    const deviceDetailStatus = document.getElementById("deviceDetailStatus");
    const deviceDetailSummary = document.getElementById("deviceDetailSummary");
    const deviceTagsTitle = document.getElementById("deviceTagsTitle");
    const deviceTagHeader = document.getElementById("deviceTagHeader");
    const deviceTagValueHeader = document.getElementById("deviceTagValueHeader");
    const deviceTagRows = document.getElementById("deviceTagRows");
    const deviceTelemetryTitle = document.getElementById("deviceTelemetryTitle");
    const telemetryStatus = document.getElementById("telemetryStatus");
    const telemetryChart = document.getElementById("telemetryChart");
    const telemetryRows = document.getElementById("telemetryRows");
    const telemetryTimeHeader = document.getElementById("telemetryTimeHeader");
    const telemetryTempHeader = document.getElementById("telemetryTempHeader");
    const telemetryHumidityHeader = document.getElementById("telemetryHumidityHeader");
    const telemetryPowerHeader = document.getElementById("telemetryPowerHeader");
    const telemetryAlarmHeader = document.getElementById("telemetryAlarmHeader");
    const deviceAlarmTitle = document.getElementById("deviceAlarmTitle");
    const deviceAlarmRows = document.getElementById("deviceAlarmRows");
    const deviceAlarmTimeHeader = document.getElementById("deviceAlarmTimeHeader");
    const deviceAlarmClearedHeader = document.getElementById("deviceAlarmClearedHeader");
    const deviceAlarmTypeHeader = document.getElementById("deviceAlarmTypeHeader");
    const deviceAlarmTopicHeader = document.getElementById("deviceAlarmTopicHeader");

    let ws = null;
    const pendingContactEdits = new Map();
    const pendingSettingEdits = new Map();
    const isPasswordResetPage = location.pathname.startsWith("/reset-password");
    const deviceDetailMatch = location.pathname.match(/^\/devices\/([^/]+)$/);
    const detailDeviceId = deviceDetailMatch ? decodeURIComponent(deviceDetailMatch[1]) : "";
    const isDeviceDetailPage = Boolean(deviceDetailMatch);
    const isAlarmPage = !isPasswordResetPage && !isDeviceDetailPage && location.pathname.startsWith("/alarms");
    const isSettingsPage = !isPasswordResetPage && !isDeviceDetailPage && location.pathname.startsWith("/settings");
    const isDevicePage = !isAlarmPage && !isSettingsPage && !isPasswordResetPage && !isDeviceDetailPage;
    const resetToken = new URLSearchParams(location.search).get("token") || "";
    const sortState = { key: "device_id", type: "text", direction: "asc" };
    const alarmSortState = { key: "created_at", type: "time", direction: "desc" };
    const adminSortStates = {
      customers: { key: "id", type: "text", direction: "asc" },
      users: { key: "email", type: "text", direction: "asc" },
      devices: { key: "device_id", type: "text", direction: "asc" }
    };
    let latestRaw = {};
    let latestDevices = [];
    let latestAlarms = [];
    let latestTelemetry = [];
    let telemetryRange = localStorage.getItem("snjallhus_telemetry_range") || "24h";
    if (!["24h", "7d", "30d"].includes(telemetryRange)) {
      telemetryRange = "24h";
    }
    let telemetryRefreshTimer = null;
    let currentUser = null;
    let currentLanguage = localStorage.getItem("snjallhus_language") || "is";
    let adminCustomers = [];
    let adminUsers = [];
    let adminDevices = [];
    emailInput.value = localStorage.getItem("snjallhus_email") || "";
    deviceFilter.value = localStorage.getItem("snjallhus_device_filter") || "";
    alarmFilter.value = localStorage.getItem("snjallhus_alarm_filter") || "";

    deviceSection.hidden = !isDevicePage;
    deviceDetailSection.hidden = !isDeviceDetailPage;
    alarmSection.hidden = !isAlarmPage;
    settingsSection.hidden = !isSettingsPage;
    passwordResetSection.hidden = !isPasswordResetPage;
    devicesLink.classList.toggle("active", isDevicePage || isDeviceDetailPage);
    alarmsLink.classList.toggle("active", isAlarmPage);
    settingsLink.classList.toggle("active", isSettingsPage);

    function authHeaders() {
      return {};
    }

    const translations = {
      is: {
        pageTitle: "Snjalli Húsvörðurinn",
        devices: "Tækjaskrá",
        alarms: "Viðvörunarskrá",
        settings: "Stillingar",
        emailPlaceholder: "Netfang",
        passwordPlaceholder: "Lykilorð",
        login: "Innskrá",
        forgotPassword: "Gleymt lykilorð?",
        logout: "Útskrá",
        apiLabel: "API:",
        wsLabel: "WebSocket:",
        devicesLabel: "Tæki:",
        lastUpdateLabel: "Síðasta uppfærsla:",
        waiting: "bíð",
        never: "aldrei",
        passwordChangeRequired: "Breyta þarf lykilorði",
        currentPassword: "Núverandi lykilorð",
        newPassword: "Nýtt lykilorð",
        confirmPassword: "Staðfesta nýtt lykilorð",
        changePassword: "Breyta lykilorði",
        resetPassword: "Endursetja lykilorð",
        resetEmailSent: "Ef netfangið er til færðu endursetningarhlekk í tölvupósti.",
        resetPasswordDone: "Lykilorði hefur verið breytt. Þú getur nú skráð þig inn.",
        missingEmail: "Settu inn netfang fyrst",
        missingResetToken: "Endursetningarhlekk vantar eða er rangur",
        filter: "Sía",
        clear: "Hreinsa",
        deviceFilterPlaceholder: "Leita að tæki, heimilisfangi, síma, stöðu, afli, viðvörun...",
        alarmFilterPlaceholder: "Leita að tíma, tæki, viðvörun, topic, stöðu...",
        shown: "Sýnt:",
        save: "Vista",
        saving: "Vista",
        saved: "Vistað",
        error: "Villa",
        connected: "tengt",
        disconnected: "ótengt",
        notConnected: "ekki tengt",
        notLoggedIn: "ekki innskráð",
        loggingIn: "skrái inn",
        noDeviceData: "Engin tækjagögn sótt.",
        noDevicesReceived: "Engin tæki hafa borist enn.",
        noDeviceMatches: "Engin tæki passa við síuna.",
        noAlarmHistory: "Engin viðvörunarsaga hefur borist.",
        noAlarmMatches: "Engar viðvaranir passa við síuna.",
        passwordMismatch: "Nýju lykilorðin passa ekki saman",
        passwordChanged: "breytt",
        activeValue: "Virkt gildi",
        savePrefix: "vista ",
        columnDeviceId: "Tæki",
        columnPhone: "Sími",
        columnAddress: "Heimilisfang",
        columnStatus: "Staða",
        columnLastSeen: "Síðast séð",
        columnTemperature: "Hiti (C)",
        columnHumidity: "Raki (%)",
        columnPower: "Aflgjafi",
        columnExtraPowerMonitor: "Auka\naflvaki",
        columnExtraPowerMonitorConnection: "Auka aflvaki\nstaða",
        columnSetLowTemp: "Settur lágur\nhiti (C)",
        columnSetHighTemp: "Settur hár\nhiti (C)",
        columnSetHighHumidity: "Settur hár\nraki (%)",
        columnRefreshInterval: "Uppfærslu-\ntíðni (s)",
        columnAlarm: "Viðvörun",
        columnTime: "Tími",
        columnCleared: "Hreinsað",
        columnTopic: "Topic",
        customer: "Viðskiptavinur",
        notificationEmails: "Viðvörunar netföng",
        notificationEmailsPlaceholder: "eitt netfang í hverja línu",
        saveNotificationEmails: "Vista netföng",
        loading: "sæki",
        settingsSaved: "vistað",
        backToDevices: "Til baka i t\u00e6kjaskr\u00e1",
        deviceDetail: "T\u00e6ki",
        currentStatus: "N\u00faverandi sta\u00f0a",
        currentTags: "N\u00faverandi gildi",
        tagValue: "Gildi",
        telemetryHistory: "Hiti og raki",
        deviceAlarmHistory: "Vi\u00f0varanir fyrir t\u00e6ki",
        noTelemetryHistory: "Engin m\u00e6lig\u00f6gn hafa veri\u00f0 skr\u00e1\u00f0.",
        range24h: "24 t\u00edmar",
        range7d: "7 dagar",
        range30d: "30 dagar",
        detailAddress: "Heimilisfang",
        detailPhone: "S\u00edmi",
        detailPower: "Aflgjafi",
        detailAuxPower: "Auka aflvaki",
        detailAuxPowerStatus: "Auka aflvaki sta\u00f0a",
        detailLowTemp: "Settur l\u00e1gur hiti",
        detailHighTemp: "Settur h\u00e1r hiti",
        detailHighHumidity: "Settur h\u00e1r raki",
        detailInterval: "Uppf\u00e6rslu-ti\u00f0ni",
        chartLegendTemp: "Hiti",
        chartLegendHumidity: "Raki",
        superAdmin: "Kerfisstj\u00f3rn",
        adminCustomers: "Vi\u00f0skiptavinir",
        adminUsers: "Notendur",
        adminDevices: "T\u00e6kja\u00fathlutun",
        userEmail: "Netfang",
        customerId: "Au\u00f0kenni",
        customerName: "Nafn",
        subscriptionStatus: "Sta\u00f0a",
        subscriptionPlan: "\u00c1skrift",
        paidUntil: "Greitt til",
        loginAccess: "A\u00f0gangur",
        deleteText: "Ey\u00f0a",
        create: "B\u00faa til",
        addUser: "B\u00e6ta vi\u00f0",
        assignDevice: "B\u00e6ta vi\u00f0",
        userRole: "Hlutverk",
        activeUser: "Virkur",
        mustChangePassword: "Skiptir um lykilor\u00f0",
        resetUserPassword: "N\u00fdtt lykilor\u00f0",
        sendWelcome: "Senda",
        welcomeSent: "Velkomup\u00f3stur sendur",
        removeDevice: "Fjarl\u00e6gja",
        tempPassword: "T\u00edmabundi\u00f0 lykilor\u00f0",
        updated: "Uppf\u00e6rt",
        yes: "j\u00e1",
        no: "nei",
        accessOn: "virkur",
        accessOff: "loka\u00f0",
        noCustomers: "Engir vi\u00f0skiptavinir.",
        noUsers: "Engir notendur.",
        noAdminDevices: "Engin t\u00e6ki.",
        confirmDeleteCustomer: "Ey\u00f0a vi\u00f0skiptavini? A\u00f0eins t\u00f3mir vi\u00f0skiptavinir eru eyddir.",
        confirmDeleteDevice: "Fjarl\u00e6gja t\u00e6ki \u00far virkri t\u00e6kjaskr\u00e1?",
        customerIdPlaceholder: "customer-id",
        customerNamePlaceholder: "Nafn",
        userEmailPlaceholder: "netfang",
        deviceIdPlaceholder: "SH1000"
      },
      en: {
        pageTitle: "Snjalli Husvordurinn",
        devices: "Devices",
        alarms: "Alarm log",
        settings: "Settings",
        emailPlaceholder: "Email",
        passwordPlaceholder: "Password",
        login: "Login",
        forgotPassword: "Forgot password?",
        logout: "Logout",
        apiLabel: "API:",
        wsLabel: "WebSocket:",
        devicesLabel: "Devices:",
        lastUpdateLabel: "Last update:",
        waiting: "waiting",
        never: "never",
        passwordChangeRequired: "Password change required",
        currentPassword: "Current password",
        newPassword: "New password",
        confirmPassword: "Confirm new password",
        changePassword: "Change password",
        resetPassword: "Reset password",
        resetEmailSent: "If the email exists, a reset link has been sent.",
        resetPasswordDone: "Password has been changed. You can now log in.",
        missingEmail: "Enter the email address first",
        missingResetToken: "Reset link is missing or invalid",
        filter: "Filter",
        clear: "Clear",
        deviceFilterPlaceholder: "Search device ID, address, phone, status, power, alarm...",
        alarmFilterPlaceholder: "Search time, device ID, alarm, topic, status...",
        shown: "Shown:",
        save: "Save",
        saving: "Saving",
        saved: "Saved",
        error: "Error",
        connected: "connected",
        disconnected: "disconnected",
        notConnected: "not connected",
        notLoggedIn: "not logged in",
        loggingIn: "logging in",
        noDeviceData: "No device data loaded.",
        noDevicesReceived: "No devices received yet.",
        noDeviceMatches: "No devices match the current filter.",
        noAlarmHistory: "No alarm history received.",
        noAlarmMatches: "No alarms match the current filter.",
        passwordMismatch: "New passwords do not match",
        passwordChanged: "changed",
        activeValue: "Active value",
        savePrefix: "save ",
        columnDeviceId: "Device ID",
        columnPhone: "Phone",
        columnAddress: "Address",
        columnStatus: "Status",
        columnLastSeen: "Last seen",
        columnTemperature: "Temp (C)",
        columnHumidity: "Humidity (%)",
        columnPower: "Power",
        columnExtraPowerMonitor: "Aux. power\nmonitor",
        columnExtraPowerMonitorConnection: "Aux. power\nmonitor status",
        columnSetLowTemp: "SET Low\ntemp (C)",
        columnSetHighTemp: "SET High\ntemp (C)",
        columnSetHighHumidity: "SET High\nhumidity (%)",
        columnRefreshInterval: "Refresh\ninterval (s)",
        columnAlarm: "Alarm",
        columnTime: "Time",
        columnCleared: "Cleared",
        columnTopic: "Topic",
        customer: "Customer",
        notificationEmails: "Alarm email recipients",
        notificationEmailsPlaceholder: "one email address per line",
        saveNotificationEmails: "Save emails",
        loading: "loading",
        settingsSaved: "saved",
        backToDevices: "Back to devices",
        deviceDetail: "Device",
        currentStatus: "Current status",
        currentTags: "Current tags",
        tagValue: "Value",
        telemetryHistory: "Temperature and humidity",
        deviceAlarmHistory: "Device alarm history",
        noTelemetryHistory: "No telemetry history has been stored yet.",
        range24h: "24 hours",
        range7d: "7 days",
        range30d: "30 days",
        detailAddress: "Address",
        detailPhone: "Phone",
        detailPower: "Power source",
        detailAuxPower: "Aux. power monitor",
        detailAuxPowerStatus: "Aux. power status",
        detailLowTemp: "Set low temp",
        detailHighTemp: "Set high temp",
        detailHighHumidity: "Set high humidity",
        detailInterval: "Update frequency",
        chartLegendTemp: "Temperature",
        chartLegendHumidity: "Humidity",
        superAdmin: "System admin",
        adminCustomers: "Customers",
        adminUsers: "Users",
        adminDevices: "Device assignment",
        userEmail: "Email",
        customerId: "ID",
        customerName: "Name",
        subscriptionStatus: "Status",
        subscriptionPlan: "Plan",
        paidUntil: "Paid until",
        loginAccess: "Access",
        deleteText: "Delete",
        create: "Create",
        addUser: "Add user",
        assignDevice: "Add device",
        userRole: "Role",
        activeUser: "Active",
        mustChangePassword: "Must change password",
        resetUserPassword: "New password",
        sendWelcome: "Send",
        welcomeSent: "Welcome email sent",
        removeDevice: "Remove",
        tempPassword: "Temporary password",
        updated: "Updated",
        yes: "yes",
        no: "no",
        accessOn: "enabled",
        accessOff: "blocked",
        noCustomers: "No customers.",
        noUsers: "No users.",
        noAdminDevices: "No devices.",
        confirmDeleteCustomer: "Delete customer? Only empty customers can be deleted.",
        confirmDeleteDevice: "Remove device from the active device registry?",
        customerIdPlaceholder: "customer-id",
        customerNamePlaceholder: "Name",
        userEmailPlaceholder: "email",
        deviceIdPlaceholder: "SH1000"
      }
    };

    function t(key) {
      const dictionary = translations[currentLanguage] || translations.is;
      return dictionary[key] || translations.en[key] || key;
    }

    function setSortLabel(sectionSelector, key, label) {
      const button = document.querySelector(sectionSelector + " .sort-button[data-sort-key='" + key + "']");
      if (!button) return;

      const indicator = button.querySelector(".sort-indicator");
      let labelElement = button.querySelector(".sort-label");

      if (!labelElement) {
        labelElement = document.createElement("span");
        labelElement.className = "sort-label";

        for (const node of Array.from(button.childNodes)) {
          if (node !== indicator) {
            node.remove();
          }
        }

        button.insertBefore(labelElement, indicator || null);
      }

      labelElement.textContent = "";

      for (const part of String(label).split("\n")) {
        const line = document.createElement("span");
        line.className = "sort-label-line";
        line.textContent = part;
        labelElement.appendChild(line);
      }
    }

    function applyLanguage() {
      if (!translations[currentLanguage]) {
        currentLanguage = "is";
      }

      document.documentElement.lang = currentLanguage;
      document.title = t("pageTitle");
      pageTitle.textContent = t("pageTitle");
      devicesLink.textContent = t("devices");
      alarmsLink.textContent = t("alarms");
      settingsLink.textContent = t("settings");
      languageToggle.textContent = currentLanguage === "is" ? "EN" : "IS";
      languageToggle.title = currentLanguage === "is" ? "English" : "Íslenska";
      emailInput.placeholder = t("emailPlaceholder");
      passwordInput.placeholder = t("passwordPlaceholder");
      loginButton.textContent = t("login");
      forgotPasswordButton.textContent = t("forgotPassword");
      logoutButton.textContent = t("logout");
      apiLabel.textContent = t("apiLabel");
      wsLabel.textContent = t("wsLabel");
      deviceCountLabel.textContent = t("devicesLabel");
      lastUpdateLabel.textContent = t("lastUpdateLabel");
      passwordChangeTitle.textContent = t("passwordChangeRequired");
      currentPasswordInput.placeholder = t("currentPassword");
      newPasswordInput.placeholder = t("newPassword");
      confirmPasswordInput.placeholder = t("confirmPassword");
      changePasswordButton.textContent = t("changePassword");
      passwordResetTitle.textContent = t("resetPassword");
      resetPasswordInput.placeholder = t("newPassword");
      resetPasswordConfirmInput.placeholder = t("confirmPassword");
      resetPasswordButton.textContent = t("resetPassword");
      deviceFilterLabel.textContent = t("filter");
      deviceFilter.placeholder = t("deviceFilterPlaceholder");
      clearDeviceFilter.textContent = t("clear");
      saveHeader.textContent = t("save");
      alarmTitle.textContent = t("alarms");
      alarmFilterLabel.textContent = t("filter");
      alarmFilter.placeholder = t("alarmFilterPlaceholder");
      clearAlarmFilter.textContent = t("clear");
      shownLabel.textContent = t("shown");
      settingsTitle.textContent = t("settings");
      settingsCustomerLabel.textContent = t("customer");
      notificationEmailsLabel.textContent = t("notificationEmails");
      notificationEmailsInput.placeholder = t("notificationEmailsPlaceholder");
      saveNotificationEmails.textContent = t("saveNotificationEmails");
      superAdminTitle.textContent = t("superAdmin");
      adminCustomersTitle.textContent = t("adminCustomers");
      adminUsersTitle.textContent = t("adminUsers");
      adminDevicesTitle.textContent = t("adminDevices");
      adminCustomerIdInput.placeholder = t("customerIdPlaceholder");
      adminCustomerNameInput.placeholder = t("customerNamePlaceholder");
      createCustomerButton.textContent = t("create");
      setSortLabel("#adminCustomersPanel", "id", t("customerId"));
      setSortLabel("#adminCustomersPanel", "name", t("customerName"));
      setSortLabel("#adminCustomersPanel", "subscription_status", t("subscriptionStatus"));
      setSortLabel("#adminCustomersPanel", "subscription_plan", t("subscriptionPlan"));
      setSortLabel("#adminCustomersPanel", "paid_until", t("paidUntil"));
      setSortLabel("#adminCustomersPanel", "login_enabled", t("loginAccess"));
      adminCustomerSaveHeader.textContent = t("save");
      adminCustomerDeleteHeader.textContent = t("deleteText");
      adminUserEmailInput.placeholder = t("userEmailPlaceholder");
      createUserButton.textContent = t("addUser");
      setSortLabel("#adminUsersPanel", "email", t("userEmail"));
      setSortLabel("#adminUsersPanel", "customer_name", t("customer"));
      setSortLabel("#adminUsersPanel", "role", t("userRole"));
      adminUserActiveHeader.textContent = t("activeUser");
      adminUserMustChangeHeader.textContent = t("mustChangePassword");
      adminUserSaveHeader.textContent = t("save");
      adminUserResetHeader.textContent = t("resetUserPassword");
      adminUserWelcomeHeader.textContent = t("sendWelcome");
      adminDeviceIdInput.placeholder = t("deviceIdPlaceholder");
      assignDeviceButton.textContent = t("assignDevice");
      setSortLabel("#adminDevicesPanel", "device_id", t("columnDeviceId"));
      setSortLabel("#adminDevicesPanel", "customer_name", t("customer"));
      setSortLabel("#adminDevicesPanel", "updated_at", t("updated"));
      adminDeviceSaveHeader.textContent = t("save");
      adminDeviceDeleteHeader.textContent = t("removeDevice");
      deviceDetailBack.textContent = t("backToDevices");
      deviceTagsTitle.textContent = t("currentTags");
      deviceTagHeader.textContent = "Tag";
      deviceTagValueHeader.textContent = t("tagValue");
      deviceTelemetryTitle.textContent = t("telemetryHistory");
      deviceAlarmTitle.textContent = t("deviceAlarmHistory");
      telemetryTimeHeader.textContent = t("columnTime");
      telemetryTempHeader.textContent = t("columnTemperature");
      telemetryHumidityHeader.textContent = t("columnHumidity");
      telemetryPowerHeader.textContent = t("columnPower");
      telemetryAlarmHeader.textContent = t("columnAlarm");
      deviceAlarmTimeHeader.textContent = t("columnTime");
      deviceAlarmClearedHeader.textContent = t("columnCleared");
      deviceAlarmTypeHeader.textContent = t("columnAlarm");
      deviceAlarmTopicHeader.textContent = t("columnTopic");
      document.querySelectorAll(".range-button").forEach((button) => {
        button.textContent = t(button.dataset.range === "24h" ? "range24h" : (button.dataset.range === "7d" ? "range7d" : "range30d"));
      });
      if (apiStatus.textContent === "waiting" || apiStatus.textContent === "bíð") {
        apiStatus.textContent = t("waiting");
      }
      if (wsStatus.textContent === "waiting" || wsStatus.textContent === "bíð") {
        wsStatus.textContent = t("waiting");
      }
      if (lastUpdate.textContent === "never" || lastUpdate.textContent === "aldrei") {
        lastUpdate.textContent = t("never");
      }

      setSortLabel("#deviceSection", "device_id", t("columnDeviceId"));
      setSortLabel("#deviceSection", "phone_number", t("columnPhone"));
      setSortLabel("#deviceSection", "address", t("columnAddress"));
      setSortLabel("#deviceSection", "connection_state", t("columnStatus"));
      setSortLabel("#deviceSection", "last_seen", t("columnLastSeen"));
      setSortLabel("#deviceSection", "temperature", t("columnTemperature"));
      setSortLabel("#deviceSection", "humidity", t("columnHumidity"));
      setSortLabel("#deviceSection", "power_source", t("columnPower"));
      setSortLabel("#deviceSection", "ble_power_monitor_installed", t("columnExtraPowerMonitor"));
      setSortLabel("#deviceSection", "ble_power_monitor_connection", t("columnExtraPowerMonitorConnection"));
      setSortLabel("#deviceSection", "low_temperature_display", t("columnSetLowTemp"));
      setSortLabel("#deviceSection", "high_temperature_display", t("columnSetHighTemp"));
      setSortLabel("#deviceSection", "high_humidity_display", t("columnSetHighHumidity"));
      setSortLabel("#deviceSection", "telemetry_interval_display", t("columnRefreshInterval"));
      setSortLabel("#deviceSection", "alarm_state", t("columnAlarm"));

      setSortLabel("#alarmSection", "created_at", t("columnTime"));
      setSortLabel("#alarmSection", "cleared_at", t("columnCleared"));
      setSortLabel("#alarmSection", "device_id", t("columnDeviceId"));
      setSortLabel("#alarmSection", "alarm_type", t("columnAlarm"));
      setSortLabel("#alarmSection", "source_topic", t("columnTopic"));
      updateSortIndicators();
      updateAlarmSortIndicators();
      updateAdminSortIndicators();
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
        indicator.textContent = active ? (sortState.direction === "asc" ? "\u25B2" : "\u25BC") : "";
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
        indicator.textContent = active ? (alarmSortState.direction === "asc" ? "\u25B2" : "\u25BC") : "";
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
      input.title = t("activeValue") + ": " + fmt(activeValue, unit ? " " + unit : "");
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

    async function sendJson(method, url, body) {
      const response = await fetch(url, {
        method,
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

    async function patchJson(url, body) {
      return sendJson("PATCH", url, body);
    }

    async function putJson(url, body) {
      return sendJson("PUT", url, body);
    }

    async function postJson(url, body) {
      return sendJson("POST", url, body);
    }

    async function deleteJson(url) {
      const response = await fetch(url, {
        method: "DELETE",
        headers: authHeaders()
      });

      if (!response.ok) {
        const responseBody = await response.json().catch(() => ({}));
        throw new Error(responseBody.error || ("HTTP " + response.status));
      }

      return response.json();
    }

    async function getJson(url) {
      const response = await fetch(url, { headers: authHeaders() });

      if (!response.ok) {
        const responseBody = await response.json().catch(() => ({}));
        throw new Error(responseBody.error || ("HTTP " + response.status));
      }

      return response.json();
    }

    function dateInputValue(value) {
      if (!value) return "";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return "";
      return date.toISOString().slice(0, 10);
    }

    function setSelectOptions(select, options, selectedValue) {
      select.textContent = "";

      for (const optionValue of options) {
        const option = document.createElement("option");
        option.value = String(optionValue.value);
        option.textContent = optionValue.label;
        select.appendChild(option);
      }

      if ([...select.options].some((option) => option.value === String(selectedValue))) {
        select.value = String(selectedValue);
      }
    }

    function subscriptionStatusOptions() {
      return ["active", "trialing", "grace", "past_due", "suspended", "canceled"]
        .map((value) => ({ value, label: value }));
    }

    function subscriptionPlanOptions() {
      return ["monthly", "yearly", "trial", "manual"]
        .map((value) => ({ value, label: value }));
    }

    function loginOptions() {
      return [
        { value: "true", label: t("accessOn") },
        { value: "false", label: t("accessOff") }
      ];
    }

    function boolOptions() {
      return [
        { value: "true", label: t("yes") },
        { value: "false", label: t("no") }
      ];
    }

    function roleOptions() {
      return [
        { value: "customer", label: "customer" },
        { value: "admin", label: "admin" },
        { value: "super_admin", label: "super_admin" }
      ];
    }

    function customerOptions(includeBlank = false) {
      const options = adminCustomers.map((customer) => ({
        value: customer.id,
        label: customer.name + " (" + customer.id + ")"
      }));

      return includeBlank ? [{ value: "", label: "-" }, ...options] : options;
    }

    function populateAdminSelects() {
      setSelectOptions(adminCustomerStatusSelect, subscriptionStatusOptions(), "active");
      setSelectOptions(adminCustomerPlanSelect, subscriptionPlanOptions(), "monthly");
      setSelectOptions(adminCustomerLoginSelect, loginOptions(), "true");
      setSelectOptions(adminUserRoleSelect, roleOptions(), "customer");
      setSelectOptions(adminUserCustomerSelect, customerOptions(true), adminUserCustomerSelect.value);
      setSelectOptions(adminDeviceCustomerSelect, customerOptions(false), adminDeviceCustomerSelect.value);
    }

    function adminInput(value, className) {
      const input = document.createElement("input");
      input.className = className || "";
      input.value = value || "";
      return input;
    }

    function adminDateInput(value) {
      const input = document.createElement("input");
      input.type = "date";
      input.value = dateInputValue(value);
      return input;
    }

    function adminSelect(options, value) {
      const select = document.createElement("select");
      setSelectOptions(select, options, value);
      return select;
    }

    function adminSortValue(tableName, row, key) {
      if (key === "customer_name") {
        return row.customer_name || row.customer_id || "";
      }

      if (key === "login_enabled") {
        return row.login_enabled !== false ? t("accessOn") : t("accessOff");
      }

      return row[key];
    }

    function compareAdminRows(tableName, left, right) {
      const state = adminSortStates[tableName];
      const direction = state.direction === "desc" ? -1 : 1;
      const leftValue = adminSortValue(tableName, left, state.key);
      const rightValue = adminSortValue(tableName, right, state.key);
      const leftEmpty = leftValue === null || leftValue === undefined || leftValue === "";
      const rightEmpty = rightValue === null || rightValue === undefined || rightValue === "";

      if (leftEmpty && rightEmpty) return 0;
      if (leftEmpty) return 1;
      if (rightEmpty) return -1;

      let result = 0;

      if (state.type === "time") {
        result = Date.parse(leftValue) - Date.parse(rightValue);
      } else if (state.type === "number") {
        result = Number(leftValue) - Number(rightValue);
      } else {
        result = String(leftValue).localeCompare(String(rightValue), undefined, {
          numeric: true,
          sensitivity: "base"
        });
      }

      if (!Number.isFinite(result)) {
        result = 0;
      }

      return result * direction;
    }

    function sortedAdminRows(tableName, rows) {
      return [...rows].sort((left, right) => compareAdminRows(tableName, left, right));
    }

    function updateAdminSortIndicators() {
      document.querySelectorAll("[data-admin-sort]").forEach((button) => {
        const tableName = button.dataset.adminSort;
        const state = adminSortStates[tableName];
        const indicator = button.querySelector(".sort-indicator");

        if (!state || !indicator) {
          return;
        }

        const active = button.dataset.sortKey === state.key;
        button.setAttribute("aria-sort", active ? (state.direction === "asc" ? "ascending" : "descending") : "none");
        indicator.textContent = active ? (state.direction === "asc" ? "\u25B2" : "\u25BC") : "";
      });
    }

    function clearAdminTables() {
      adminCustomerRows.innerHTML = '<tr><td colspan="8" class="empty">' + t("noCustomers") + '</td></tr>';
      adminUserRows.innerHTML = '<tr><td colspan="8" class="empty">' + t("noUsers") + '</td></tr>';
      adminDeviceRows.innerHTML = '<tr><td colspan="5" class="empty">' + t("noAdminDevices") + '</td></tr>';
    }

    function renderAdminCustomers() {
      adminCustomerRows.textContent = "";
      populateAdminSelects();

      if (!adminCustomers.length) {
        adminCustomerRows.innerHTML = '<tr><td colspan="8" class="empty">' + t("noCustomers") + '</td></tr>';
        return;
      }

      for (const customer of sortedAdminRows("customers", adminCustomers)) {
        const row = document.createElement("tr");
        const nameInput = adminInput(customer.name, "wide-input");
        const statusSelect = adminSelect(subscriptionStatusOptions(), customer.subscription_status);
        const planSelect = adminSelect(subscriptionPlanOptions(), customer.subscription_plan);
        const paidUntilInput = adminDateInput(customer.paid_until);
        const loginSelect = adminSelect(loginOptions(), String(customer.login_enabled !== false));
        const saveButton = document.createElement("button");
        const deleteButton = document.createElement("button");

        saveButton.type = "button";
        saveButton.textContent = t("save");
        deleteButton.type = "button";
        deleteButton.textContent = t("deleteText");
        deleteButton.className = "danger-button";

        saveButton.addEventListener("click", async () => {
          adminCustomersStatus.textContent = t("saving");
          try {
            await patchJson("/api/v1/admin/customers/" + encodeURIComponent(customer.id), {
              name: nameInput.value,
              subscription_status: statusSelect.value,
              subscription_plan: planSelect.value,
              paid_until: paidUntilInput.value || null,
              login_enabled: loginSelect.value === "true"
            });
            adminCustomersStatus.textContent = t("settingsSaved");
            await loadSuperAdminWorkspace();
          } catch (error) {
            adminCustomersStatus.textContent = error.message;
          }
        });

        deleteButton.addEventListener("click", async () => {
          if (!confirm(t("confirmDeleteCustomer"))) {
            return;
          }

          adminCustomersStatus.textContent = t("saving");
          try {
            await deleteJson("/api/v1/admin/customers/" + encodeURIComponent(customer.id));
            adminCustomersStatus.textContent = t("settingsSaved");
            await loadSettingsCustomers();
            await loadSuperAdminWorkspace();
          } catch (error) {
            adminCustomersStatus.textContent = error.message;
          }
        });

        cell(row, customer.id);
        cell(row, nameInput);
        cell(row, statusSelect);
        cell(row, planSelect);
        cell(row, paidUntilInput);
        cell(row, loginSelect);
        cell(row, saveButton);
        cell(row, deleteButton);
        adminCustomerRows.appendChild(row);
      }
    }

    function renderAdminUsers() {
      adminUserRows.textContent = "";

      if (!adminUsers.length) {
        adminUserRows.innerHTML = '<tr><td colspan="8" class="empty">' + t("noUsers") + '</td></tr>';
        return;
      }

      for (const user of sortedAdminRows("users", adminUsers)) {
        const row = document.createElement("tr");
        const customerSelect = adminSelect(customerOptions(true), user.customer_id || "");
        const roleSelect = adminSelect(roleOptions(), user.role);
        const activeSelect = adminSelect(boolOptions(), String(user.active));
        const mustChangeSelect = adminSelect(boolOptions(), String(user.must_change_password));
        const saveButton = document.createElement("button");
        const resetButton = document.createElement("button");
        const welcomeButton = document.createElement("button");

        saveButton.type = "button";
        saveButton.textContent = t("save");
        resetButton.type = "button";
        resetButton.textContent = t("resetUserPassword");
        welcomeButton.type = "button";
        welcomeButton.textContent = t("sendWelcome");

        saveButton.addEventListener("click", async () => {
          adminUsersStatus.textContent = t("saving");
          try {
            await patchJson("/api/v1/admin/users/" + encodeURIComponent(user.id), {
              customer_id: customerSelect.value || null,
              role: roleSelect.value,
              active: activeSelect.value === "true",
              must_change_password: mustChangeSelect.value === "true"
            });
            adminUsersStatus.textContent = t("settingsSaved");
            await loadSuperAdminWorkspace();
          } catch (error) {
            adminUsersStatus.textContent = error.message;
          }
        });

        resetButton.addEventListener("click", async () => {
          adminUsersStatus.textContent = t("saving");
          try {
            const result = await patchJson("/api/v1/admin/users/" + encodeURIComponent(user.id), {
              customer_id: customerSelect.value || null,
              role: roleSelect.value,
              active: activeSelect.value === "true",
              must_change_password: true,
              reset_password: true
            });
            adminUsersStatus.textContent = t("tempPassword") + ": " + result.temp_password;
            await loadSuperAdminWorkspace();
          } catch (error) {
            adminUsersStatus.textContent = error.message;
          }
        });

        welcomeButton.addEventListener("click", async () => {
          adminUsersStatus.textContent = t("saving");
          welcomeButton.disabled = true;

          try {
            await postJson("/api/v1/admin/users/" + encodeURIComponent(user.id) + "/welcome", {});
            adminUsersStatus.textContent = t("welcomeSent") + ": " + user.email;
            await loadSuperAdminWorkspace();
          } catch (error) {
            adminUsersStatus.textContent = error.message;
          } finally {
            welcomeButton.disabled = false;
          }
        });

        cell(row, user.email);
        cell(row, customerSelect);
        cell(row, roleSelect);
        cell(row, activeSelect);
        cell(row, mustChangeSelect);
        cell(row, saveButton);
        cell(row, resetButton);
        cell(row, welcomeButton);
        adminUserRows.appendChild(row);
      }
    }

    function renderAdminDevices() {
      adminDeviceRows.textContent = "";

      if (!adminDevices.length) {
        adminDeviceRows.innerHTML = '<tr><td colspan="5" class="empty">' + t("noAdminDevices") + '</td></tr>';
        return;
      }

      for (const device of sortedAdminRows("devices", adminDevices)) {
        const row = document.createElement("tr");
        const customerSelect = adminSelect(customerOptions(false), device.customer_id || "");
        const saveButton = document.createElement("button");
        const deleteButton = document.createElement("button");

        saveButton.type = "button";
        saveButton.textContent = t("save");
        deleteButton.type = "button";
        deleteButton.textContent = t("removeDevice");
        deleteButton.className = "danger-button";

        saveButton.addEventListener("click", async () => {
          adminDevicesStatus.textContent = t("saving");
          try {
            await patchJson("/api/v1/admin/devices/" + encodeURIComponent(device.device_id), {
              customer_id: customerSelect.value
            });
            adminDevicesStatus.textContent = t("settingsSaved");
            await loadSuperAdminWorkspace();
            await loadState();
          } catch (error) {
            adminDevicesStatus.textContent = error.message;
          }
        });

        deleteButton.addEventListener("click", async () => {
          if (!confirm(t("confirmDeleteDevice"))) {
            return;
          }

          adminDevicesStatus.textContent = t("saving");
          try {
            await deleteJson("/api/v1/admin/devices/" + encodeURIComponent(device.device_id));
            adminDevicesStatus.textContent = t("settingsSaved");
            await loadSuperAdminWorkspace();
            await loadState();
          } catch (error) {
            adminDevicesStatus.textContent = error.message;
          }
        });

        cell(row, device.device_id);
        cell(row, customerSelect);
        cell(row, fmtTime(device.updated_at));
        cell(row, saveButton);
        cell(row, deleteButton);
        adminDeviceRows.appendChild(row);
      }
    }

    function renderSuperAdminWorkspace() {
      superAdminSection.hidden = !isSettingsPage || !isSuperUser();

      if (superAdminSection.hidden) {
        return;
      }

      populateAdminSelects();
      renderAdminCustomers();
      renderAdminUsers();
      renderAdminDevices();
      updateAdminSortIndicators();
    }

    async function saveRow(deviceId, phoneInput, addressInput, lowInput, highInput, humidityInput, intervalInput, button) {
      button.disabled = true;
      button.textContent = t("saving");
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

        button.textContent = t("saved");
        button.classList.add("saved");
        setTimeout(() => {
          button.textContent = t("save");
          button.classList.remove("saved");
        }, 1200);
      } catch (error) {
        button.textContent = t("error");
        apiStatus.textContent = t("savePrefix") + error.message;
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
        cell(row, t("noDevicesReceived"));
        row.firstChild.colSpan = 16;
        row.firstChild.className = "empty";
        deviceRows.appendChild(row);
        return;
      }

      if (!visibleDevices.length) {
        const row = document.createElement("tr");
        cell(row, t("noDeviceMatches"));
        row.firstChild.colSpan = 16;
        row.firstChild.className = "empty";
        deviceRows.appendChild(row);
        return;
      }

      for (const device of sortedDevices(visibleDevices)) {
        const row = document.createElement("tr");
        row.classList.add("clickable-row");
        if (device.alarm_state === "ALARM") {
          row.classList.add("alarm");
        } else if (device.is_offline) {
          row.classList.add("offline");
        }

        row.addEventListener("click", (event) => {
          if (event.target.closest("input, button, a, select, textarea")) {
            return;
          }

          location.href = "/devices/" + encodeURIComponent(device.device_id);
        });

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
        rowSaveButton.textContent = t("save");
        lowInput.addEventListener("input", () => setPendingSetting(device.device_id, "low_temperature", lowInput.value));
        highInput.addEventListener("input", () => setPendingSetting(device.device_id, "high_temperature", highInput.value));
        humidityInput.addEventListener("input", () => setPendingSetting(device.device_id, "high_humidity", humidityInput.value));
        intervalInput.addEventListener("input", () => setPendingSetting(device.device_id, "telemetry_interval_sec", intervalInput.value));
        rowSaveButton.addEventListener("click", () => {
          saveRow(device.device_id, phoneInput, addressInput, lowInput, highInput, humidityInput, intervalInput, rowSaveButton);
        });

        const deviceLink = document.createElement("a");
        deviceLink.className = "device-link";
        deviceLink.href = "/devices/" + encodeURIComponent(device.device_id);
        deviceLink.textContent = device.device_id;

        cell(row, deviceLink);
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
        cell(row, t("noAlarmHistory"));
        row.firstChild.colSpan = 5;
        row.firstChild.className = "empty";
        alarmRows.appendChild(row);
        return;
      }

      if (!visibleAlarms.length) {
        const row = document.createElement("tr");
        cell(row, t("noAlarmMatches"));
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

    function detailItem(label, value) {
      const item = document.createElement("div");
      const labelElement = document.createElement("div");
      const valueElement = document.createElement("div");
      item.className = "detail-item";
      labelElement.className = "detail-label";
      valueElement.className = "detail-value";
      labelElement.textContent = label;

      if (value instanceof Node) {
        valueElement.appendChild(value);
      } else {
        valueElement.textContent = value || "";
      }

      item.append(labelElement, valueElement);
      return item;
    }

    function renderDeviceDetail(device, alarms) {
      if (!isDeviceDetailPage) {
        return;
      }

      deviceDetailTitle.textContent = t("deviceDetail") + ": " + detailDeviceId;
      deviceDetailSummary.textContent = "";
      deviceAlarmRows.textContent = "";

      if (!device) {
        deviceDetailStatus.textContent = t("noDeviceData");
        deviceDetailSummary.appendChild(detailItem(t("currentStatus"), t("noDeviceData")));
        renderDeviceAlarms([]);
        return;
      }

      const status = device.connection_state || device.status;
      const statusKind = device.is_offline ? "warn" : (status === "online" ? "ok" : "warn");
      deviceDetailStatus.textContent = "";
      deviceDetailStatus.appendChild(badge(status, statusKind));

      const alarmBadge = device.alarm_state
        ? badge(device.alarm_state, device.alarm_state === "ALARM" ? "alarm" : "ok")
        : "";

      const items = [
        [t("columnLastSeen"), fmtTime(device.last_seen) + (device.seconds_since_seen !== null ? " (" + fmtAge(device.seconds_since_seen) + ")" : "")],
        [t("columnTemperature"), fmt(device.temperature, " C")],
        [t("columnHumidity"), fmt(device.humidity, " %")],
        [t("columnAlarm"), alarmBadge],
        [t("detailPower"), device.power_source],
        [t("detailAuxPower"), device.ble_power_monitor_installed === true ? "yes" : (device.ble_power_monitor_installed === false ? "no" : "")],
        [t("detailAuxPowerStatus"), device.ble_power_monitor_connection || ""],
        [t("detailAddress"), device.address],
        [t("detailPhone"), device.phone_number],
        [t("detailLowTemp"), fmt(displaySetting(device, "desired_low_temperature", "low_temperature"), " C")],
        [t("detailHighTemp"), fmt(displaySetting(device, "desired_high_temperature", "high_temperature"), " C")],
        [t("detailHighHumidity"), fmt(displaySetting(device, "desired_high_humidity", "high_humidity"), " %")],
        [t("detailInterval"), fmt(displaySetting(device, "desired_telemetry_interval_sec", "telemetry_interval_sec"), " s")]
      ];

      for (const [label, value] of items) {
        deviceDetailSummary.appendChild(detailItem(label, value));
      }

      renderDeviceAlarms(alarms);
    }

    function renderDeviceTags(rawState) {
      if (!isDeviceDetailPage) {
        return;
      }

      deviceTagRows.textContent = "";
      const prefix = "snjalli/" + detailDeviceId + "/";
      const rows = Object.entries(rawState || {})
        .filter(([topic]) => topic.startsWith(prefix))
        .sort(([a], [b]) => a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }));

      if (!rows.length) {
        const row = document.createElement("tr");
        cell(row, t("noDeviceData"));
        row.firstChild.colSpan = 2;
        row.firstChild.className = "empty";
        deviceTagRows.appendChild(row);
        return;
      }

      for (const [topic, value] of rows) {
        const row = document.createElement("tr");
        cell(row, topic.slice(prefix.length));
        cell(row, value);
        deviceTagRows.appendChild(row);
      }
    }

    function svgElement(name, attrs = {}) {
      const element = document.createElementNS("http://www.w3.org/2000/svg", name);

      for (const [key, value] of Object.entries(attrs)) {
        element.setAttribute(key, value);
      }

      return element;
    }

    function telemetryNumber(value) {
      const number = Number(value);
      return Number.isFinite(number) ? number : null;
    }

    function renderTelemetryChart(rows) {
      telemetryChart.textContent = "";
      const width = 760;
      const height = 280;
      const left = 50;
      const right = 18;
      const top = 22;
      const bottom = 38;
      const plotWidth = width - left - right;
      const plotHeight = height - top - bottom;
      const values = [];

      for (const row of rows) {
        const temperature = telemetryNumber(row.temperature);
        const humidity = telemetryNumber(row.humidity);
        if (temperature !== null) values.push(temperature);
        if (humidity !== null) values.push(humidity);
      }

      if (!rows.length || !values.length) {
        telemetryChart.appendChild(svgElement("text", {
          x: "50%",
          y: "50%",
          "text-anchor": "middle",
          class: "chart-empty"
        }));
        telemetryChart.lastChild.textContent = t("noTelemetryHistory");
        return;
      }

      let minValue = Math.min(...values);
      let maxValue = Math.max(...values);

      if (minValue === maxValue) {
        minValue -= 1;
        maxValue += 1;
      } else {
        const padding = (maxValue - minValue) * 0.12;
        minValue -= padding;
        maxValue += padding;
      }

      const times = rows.map((row) => Date.parse(row.created_at)).filter(Number.isFinite);
      const minTime = Math.min(...times);
      const maxTime = Math.max(...times);
      const timeSpan = Math.max(1, maxTime - minTime);

      const x = (time) => left + ((time - minTime) / timeSpan) * plotWidth;
      const y = (value) => top + ((maxValue - value) / (maxValue - minValue)) * plotHeight;

      for (let i = 0; i <= 4; i++) {
        const gridY = top + (plotHeight / 4) * i;
        const labelValue = maxValue - ((maxValue - minValue) / 4) * i;
        telemetryChart.appendChild(svgElement("line", {
          x1: left,
          y1: gridY,
          x2: width - right,
          y2: gridY,
          stroke: "#41506a",
          "stroke-width": 1
        }));
        const label = svgElement("text", {
          x: left - 8,
          y: gridY + 4,
          "text-anchor": "end",
          fill: "#a9b8cc",
          "font-size": 11
        });
        label.textContent = labelValue.toFixed(1);
        telemetryChart.appendChild(label);
      }

      function pointsFor(key) {
        return rows
          .map((row) => {
            const value = telemetryNumber(row[key]);
            const time = Date.parse(row.created_at);
            if (value === null || !Number.isFinite(time)) return null;
            return x(time).toFixed(1) + "," + y(value).toFixed(1);
          })
          .filter(Boolean)
          .join(" ");
      }

      const tempPoints = pointsFor("temperature");
      const humidityPoints = pointsFor("humidity");

      if (tempPoints) {
        telemetryChart.appendChild(svgElement("polyline", {
          points: tempPoints,
          fill: "none",
          stroke: "#ef4444",
          "stroke-width": 2.5
        }));
      }

      if (humidityPoints) {
        telemetryChart.appendChild(svgElement("polyline", {
          points: humidityPoints,
          fill: "none",
          stroke: "#38bdf8",
          "stroke-width": 2.5
        }));
      }

      const axis = svgElement("line", {
        x1: left,
        y1: top + plotHeight,
        x2: width - right,
        y2: top + plotHeight,
        stroke: "#a9b8cc",
        "stroke-width": 1
      });
      telemetryChart.appendChild(axis);

      const legendTemp = svgElement("text", { x: left, y: height - 10, fill: "#ef4444", "font-size": 12 });
      legendTemp.textContent = t("chartLegendTemp") + " (C)";
      telemetryChart.appendChild(legendTemp);

      const legendHumidity = svgElement("text", { x: left + 120, y: height - 10, fill: "#38bdf8", "font-size": 12 });
      legendHumidity.textContent = t("chartLegendHumidity") + " (%)";
      telemetryChart.appendChild(legendHumidity);
    }

    function renderTelemetryRows(rows) {
      telemetryRows.textContent = "";

      if (!rows.length) {
        const row = document.createElement("tr");
        cell(row, t("noTelemetryHistory"));
        row.firstChild.colSpan = 5;
        row.firstChild.className = "empty";
        telemetryRows.appendChild(row);
        return;
      }

      for (const telemetry of [...rows].reverse()) {
        const row = document.createElement("tr");
        cell(row, fmtTime(telemetry.created_at));
        cell(row, fmt(telemetry.temperature, " C"));
        cell(row, fmt(telemetry.humidity, " %"));
        cell(row, telemetry.power_source || "");
        cell(row, telemetry.alarm_state || "");
        telemetryRows.appendChild(row);
      }
    }

    function renderTelemetry(rows) {
      latestTelemetry = rows || [];
      renderTelemetryChart(latestTelemetry);
      renderTelemetryRows(latestTelemetry);
      telemetryStatus.textContent = String(latestTelemetry.length);
      document.querySelectorAll(".range-button").forEach((button) => {
        button.classList.toggle("active", button.dataset.range === telemetryRange);
      });
    }

    function renderDeviceAlarms(alarms) {
      deviceAlarmRows.textContent = "";

      if (!alarms.length) {
        const row = document.createElement("tr");
        cell(row, t("noAlarmHistory"));
        row.firstChild.colSpan = 4;
        row.firstChild.className = "empty";
        deviceAlarmRows.appendChild(row);
        return;
      }

      for (const alarm of sortedAlarms(alarms).slice(0, 100)) {
        const row = document.createElement("tr");
        cell(row, fmtTime(alarm.created_at));
        cell(row, fmtTime(alarm.cleared_at));
        cell(row, alarm.alarm_type);
        cell(row, alarm.source_topic);
        deviceAlarmRows.appendChild(row);
      }
    }

    async function loadDeviceTelemetry() {
      if (!isDeviceDetailPage || !currentUser) {
        return;
      }

      telemetryStatus.textContent = t("loading");

      try {
        const response = await fetch(
          "/api/v1/devices/" + encodeURIComponent(detailDeviceId) + "/telemetry?range=" + encodeURIComponent(telemetryRange),
          { headers: authHeaders() }
        );

        if (response.status === 401) {
          setCurrentUser(null);
          throw new Error(t("notLoggedIn"));
        }

        if (!response.ok) {
          const body = await response.json().catch(() => ({}));
          throw new Error(body.error || ("HTTP " + response.status));
        }

        const body = await response.json();
        renderTelemetry(body.rows || []);
      } catch (error) {
        telemetryStatus.textContent = error.message;
      }
    }

    function startTelemetryRefreshTimer() {
      if (!isDeviceDetailPage || telemetryRefreshTimer) {
        return;
      }

      telemetryRefreshTimer = setInterval(() => {
        loadDeviceTelemetry();
      }, 60000);
    }

    function isEditingContact() {
      return document.activeElement && document.activeElement.classList.contains("row-input");
    }

    function renderState(state) {
      const devices = state.devices || [];
      const alarms = state.alarms || [];
      latestRaw = state.raw || {};
      latestDevices = devices;
      latestAlarms = alarms;
      deviceCount.textContent = String(devices.length);

      if (isDevicePage && !isEditingContact()) {
        renderDevices(devices);
      }

      if (isDeviceDetailPage) {
        renderDeviceDetail(
          devices.find((device) => device.device_id === detailDeviceId),
          alarms.filter((alarm) => alarm.device_id === detailDeviceId)
        );
        renderDeviceTags(state.raw || {});
      }

      if (isAlarmPage) {
        renderAlarms(alarms);
      }

      lastUpdate.textContent = fmtClock(new Date());
    }

    function isSuperUser() {
      return Boolean(currentUser && currentUser.role === "super_admin");
    }

    async function loadSettingsCustomers() {
      if (!isSettingsPage || !currentUser || !isSuperUser()) {
        settingsCustomerWrap.hidden = true;
        settingsCustomerSelect.textContent = "";
        return;
      }

      const previousValue = settingsCustomerSelect.value;
      const response = await fetch("/api/v1/admin/customers", { headers: authHeaders() });

      if (!response.ok) {
        throw new Error("HTTP " + response.status);
      }

      const body = await response.json();
      const customers = body.customers || [];
      settingsCustomerSelect.textContent = "";

      for (const customer of customers) {
        const option = document.createElement("option");
        option.value = customer.id;
        option.textContent = customer.name + " (" + customer.id + ")";
        settingsCustomerSelect.appendChild(option);
      }

      if (previousValue && [...settingsCustomerSelect.options].some((option) => option.value === previousValue)) {
        settingsCustomerSelect.value = previousValue;
      }

      settingsCustomerWrap.hidden = customers.length <= 1;
    }

    function settingsCustomerQuery() {
      if (!isSuperUser() || !settingsCustomerSelect.value) {
        return "";
      }

      return "?customer_id=" + encodeURIComponent(settingsCustomerSelect.value);
    }

    async function loadNotificationSettings() {
      if (!isSettingsPage || !currentUser) {
        return;
      }

      notificationEmailsStatus.textContent = t("loading");

      try {
        await loadSettingsCustomers();
        const response = await fetch("/api/v1/settings/notification-emails" + settingsCustomerQuery(), {
          headers: authHeaders()
        });

        if (!response.ok) {
          const body = await response.json().catch(() => ({}));
          throw new Error(body.error || ("HTTP " + response.status));
        }

        const body = await response.json();
        notificationEmailsInput.value = (body.emails || []).join("\n");
        notificationEmailsStatus.textContent = "";
        await loadSuperAdminWorkspace();
      } catch (error) {
        notificationEmailsStatus.textContent = error.message;
      }
    }

    async function saveNotificationEmailSettings() {
      if (!isSettingsPage || !currentUser) {
        return;
      }

      saveNotificationEmails.disabled = true;
      notificationEmailsStatus.textContent = t("saving");

      try {
        const body = {
          emails: notificationEmailsInput.value
        };

        if (isSuperUser() && settingsCustomerSelect.value) {
          body.customer_id = settingsCustomerSelect.value;
        }

        const result = await putJson("/api/v1/settings/notification-emails", body);
        notificationEmailsInput.value = (result.emails || []).join("\n");
        notificationEmailsStatus.textContent = t("settingsSaved");
      } catch (error) {
        notificationEmailsStatus.textContent = error.message;
      } finally {
        saveNotificationEmails.disabled = false;
      }
    }

    async function loadSuperAdminWorkspace() {
      if (!isSettingsPage || !isSuperUser()) {
        superAdminSection.hidden = true;
        adminCustomers = [];
        adminUsers = [];
        adminDevices = [];
        clearAdminTables();
        return;
      }

      superAdminSection.hidden = false;
      adminCustomersStatus.textContent = t("loading");

      try {
        const [customersBody, usersBody, devicesBody] = await Promise.all([
          getJson("/api/v1/admin/customers"),
          getJson("/api/v1/admin/users"),
          getJson("/api/v1/admin/devices")
        ]);

        adminCustomers = customersBody.customers || [];
        adminUsers = usersBody.users || [];
        adminDevices = devicesBody.devices || [];
        adminCustomersStatus.textContent = "";
        adminUsersStatus.textContent = "";
        adminDevicesStatus.textContent = "";
        renderSuperAdminWorkspace();
      } catch (error) {
        adminCustomersStatus.textContent = error.message;
      }
    }

    async function createCustomerFromForm() {
      createCustomerButton.disabled = true;
      adminCustomersStatus.textContent = t("saving");

      try {
        await postJson("/api/v1/admin/customers", {
          id: adminCustomerIdInput.value,
          name: adminCustomerNameInput.value,
          subscription_status: adminCustomerStatusSelect.value,
          subscription_plan: adminCustomerPlanSelect.value,
          paid_until: adminCustomerPaidUntilInput.value || null,
          login_enabled: adminCustomerLoginSelect.value === "true"
        });

        adminCustomerIdInput.value = "";
        adminCustomerNameInput.value = "";
        adminCustomerPaidUntilInput.value = "";
        adminCustomersStatus.textContent = t("settingsSaved");
        await loadSettingsCustomers();
        await loadSuperAdminWorkspace();
      } catch (error) {
        adminCustomersStatus.textContent = error.message;
      } finally {
        createCustomerButton.disabled = false;
      }
    }

    async function createUserFromForm() {
      createUserButton.disabled = true;
      adminUsersStatus.textContent = t("saving");

      try {
        const result = await postJson("/api/v1/admin/users", {
          email: adminUserEmailInput.value,
          customer_id: adminUserCustomerSelect.value || null,
          role: adminUserRoleSelect.value
        });

        adminUserEmailInput.value = "";
        adminUsersStatus.textContent = t("tempPassword") + ": " + result.temp_password;
        await loadSuperAdminWorkspace();
      } catch (error) {
        adminUsersStatus.textContent = error.message;
      } finally {
        createUserButton.disabled = false;
      }
    }

    async function assignDeviceFromForm() {
      assignDeviceButton.disabled = true;
      adminDevicesStatus.textContent = t("saving");

      try {
        await postJson("/api/v1/admin/devices", {
          device_id: adminDeviceIdInput.value,
          customer_id: adminDeviceCustomerSelect.value
        });

        adminDeviceIdInput.value = "";
        adminDevicesStatus.textContent = t("settingsSaved");
        await loadSuperAdminWorkspace();
        await loadState();
      } catch (error) {
        adminDevicesStatus.textContent = error.message;
      } finally {
        assignDeviceButton.disabled = false;
      }
    }

    async function loadState() {
      if (!currentUser) {
        apiStatus.textContent = t("notLoggedIn");
        return;
      }

      try {
        const response = await fetch("/api/v1/state", { headers: authHeaders() });
        if (response.status === 401) {
          setCurrentUser(null);
          throw new Error(t("notLoggedIn"));
        }
        if (!response.ok) throw new Error("HTTP " + response.status);
        const data = await response.json();
        apiStatus.textContent = t("connected");
        renderState(data);
      } catch (error) {
        apiStatus.textContent = error.message;
      }
    }

    function setCurrentUser(user) {
      currentUser = user;
      const loggedIn = Boolean(user);

      emailInput.hidden = loggedIn || isPasswordResetPage;
      passwordInput.hidden = loggedIn || isPasswordResetPage;
      loginButton.hidden = loggedIn || isPasswordResetPage;
      forgotPasswordButton.hidden = loggedIn || isPasswordResetPage;
      logoutButton.hidden = !loggedIn;
      userInfo.textContent = loggedIn ? user.email + " (" + user.role + ")" : "";
      passwordChangeSection.hidden = !loggedIn || !user.must_change_password;
      passwordResetSection.hidden = !isPasswordResetPage;

      if (!loggedIn) {
        apiStatus.textContent = t("notLoggedIn");
        wsStatus.textContent = t("notConnected");
        latestDevices = [];
        latestAlarms = [];
        latestRaw = {};
        latestTelemetry = [];
        renderDevices([]);
        renderAlarms([]);
        renderDeviceDetail(null, []);
        renderDeviceTags({});
        renderTelemetry([]);
        if (telemetryRefreshTimer) {
          clearInterval(telemetryRefreshTimer);
          telemetryRefreshTimer = null;
        }
        notificationEmailsInput.value = "";
        notificationEmailsStatus.textContent = "";
        superAdminSection.hidden = true;
        adminCustomers = [];
        adminUsers = [];
        adminDevices = [];
        clearAdminTables();
      }
    }

    async function requestPasswordReset() {
      const email = emailInput.value.trim();

      if (!email) {
        apiStatus.textContent = t("missingEmail");
        return;
      }

      forgotPasswordButton.disabled = true;
      apiStatus.textContent = t("loading");

      try {
        await fetch("/api/v1/password-reset/request", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email })
        });

        apiStatus.textContent = t("resetEmailSent");
      } catch (error) {
        apiStatus.textContent = error.message;
      } finally {
        forgotPasswordButton.disabled = false;
      }
    }

    async function login() {
      apiStatus.textContent = t("loggingIn");
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
      await loadDeviceTelemetry();
      await loadNotificationSettings();
      startTelemetryRefreshTimer();
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
      await loadDeviceTelemetry();
      await loadNotificationSettings();
      startTelemetryRefreshTimer();
      connectWs();
    }

    async function changePassword() {
      if (newPasswordInput.value !== confirmPasswordInput.value) {
        throw new Error(t("passwordMismatch"));
      }

      passwordChangeStatus.textContent = t("saving");
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
      passwordChangeStatus.textContent = t("passwordChanged");
      setCurrentUser(body.user);
      setTimeout(() => {
        passwordChangeStatus.textContent = "";
      }, 1500);
    }

    async function confirmPasswordReset() {
      if (!resetToken) {
        throw new Error(t("missingResetToken"));
      }

      if (resetPasswordInput.value !== resetPasswordConfirmInput.value) {
        throw new Error(t("passwordMismatch"));
      }

      passwordResetStatus.textContent = t("saving");
      resetPasswordButton.disabled = true;

      try {
        const response = await fetch("/api/v1/password-reset/confirm", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            token: resetToken,
            new_password: resetPasswordInput.value,
            new_password_confirm: resetPasswordConfirmInput.value
          })
        });

        if (!response.ok) {
          const body = await response.json().catch(() => ({}));
          throw new Error(body.error || ("HTTP " + response.status));
        }

        resetPasswordInput.value = "";
        resetPasswordConfirmInput.value = "";
        passwordResetStatus.textContent = t("resetPasswordDone");
        setTimeout(() => {
          location.href = "/dashboard";
        }, 1800);
      } finally {
        resetPasswordButton.disabled = false;
      }
    }

    function connectWs() {
      if (ws) ws.close();

      if (!currentUser) {
        wsStatus.textContent = t("notLoggedIn");
        return;
      }

      const scheme = location.protocol === "https:" ? "wss" : "ws";
      const url = scheme + "://" + location.host + "/api/v1/ws";
      ws = new WebSocket(url);

      ws.addEventListener("open", () => {
        wsStatus.textContent = t("connected");
      });

      ws.addEventListener("message", (event) => {
        const message = JSON.parse(event.data);
        if (message.type === "state") {
          renderState(message.data || {});
        }
      });

      ws.addEventListener("close", () => {
        wsStatus.textContent = t("disconnected");
        if (currentUser) {
          setTimeout(connectWs, 3000);
        }
      });

      ws.addEventListener("error", () => {
        wsStatus.textContent = t("error");
      });
    }

    loginButton.addEventListener("click", () => {
      login().catch((error) => {
        apiStatus.textContent = error.message;
        wsStatus.textContent = t("notConnected");
      });
    });

    forgotPasswordButton.addEventListener("click", () => {
      requestPasswordReset();
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

    resetPasswordButton.addEventListener("click", () => {
      confirmPasswordReset().catch((error) => {
        passwordResetStatus.textContent = error.message;
        resetPasswordButton.disabled = false;
      });
    });

    resetPasswordConfirmInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        resetPasswordButton.click();
      }
    });

    languageToggle.addEventListener("click", () => {
      currentLanguage = currentLanguage === "is" ? "en" : "is";
      localStorage.setItem("snjallhus_language", currentLanguage);
      applyLanguage();
      renderDevices(latestDevices);
      renderAlarms(latestAlarms);
      renderTelemetry(latestTelemetry);
      renderDeviceDetail(
        latestDevices.find((device) => device.device_id === detailDeviceId),
        latestAlarms.filter((alarm) => alarm.device_id === detailDeviceId)
      );
      renderDeviceTags(latestRaw);
      renderSuperAdminWorkspace();
    });

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

    document.querySelectorAll(".range-button").forEach((button) => {
      button.addEventListener("click", () => {
        telemetryRange = button.dataset.range || "24h";
        localStorage.setItem("snjallhus_telemetry_range", telemetryRange);
        renderTelemetry(latestTelemetry);
        loadDeviceTelemetry();
      });
    });

    settingsCustomerSelect.addEventListener("change", loadNotificationSettings);
    saveNotificationEmails.addEventListener("click", saveNotificationEmailSettings);
    createCustomerButton.addEventListener("click", createCustomerFromForm);
    createUserButton.addEventListener("click", createUserFromForm);
    assignDeviceButton.addEventListener("click", assignDeviceFromForm);

    document.querySelectorAll("[data-admin-sort]").forEach((button) => {
      button.addEventListener("click", () => {
        const tableName = button.dataset.adminSort;
        const state = adminSortStates[tableName];

        if (!state) {
          return;
        }

        const key = button.dataset.sortKey;

        if (state.key === key) {
          state.direction = state.direction === "asc" ? "desc" : "asc";
        } else {
          state.key = key;
          state.type = button.dataset.sortType || "text";
          state.direction = state.type === "time" ? "desc" : "asc";
        }

        if (tableName === "customers") {
          renderAdminCustomers();
        } else if (tableName === "users") {
          renderAdminUsers();
        } else if (tableName === "devices") {
          renderAdminDevices();
        }

        updateAdminSortIndicators();
      });
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

    applyLanguage();

    if (isPasswordResetPage) {
      setCurrentUser(null);
      passwordResetStatus.textContent = resetToken ? "" : t("missingResetToken");
    } else {
      loadMe();
    }
  </script>
</body>
</html>`;

const publicPageText = {
  home: {
    title: "Snjalli Husvordurinn | Triotech",
    eyebrow: "Triotech IoT monitoring",
    heading: "Snjalli H&uacute;sv&ouml;r&eth;urinn",
    lead: "Einf&ouml;ld v&ouml;ktun fyrir h&uacute;s, frystiklefa, geymslur og a&eth;ra sta&eth;i &thorn;ar sem hitastig, raki, rafmagn og vi&eth;varanir skipta m&aacute;li.",
    body: [
      "Kerfi&eth; notar eigin IoT t&aelig;ki sem senda st&ouml;&eth;u og vi&eth;varanir til &thorn;j&oacute;ns Triotech. Vi&eth;skiptavinir skr&aacute; sig inn &aacute; vefinn og sj&aacute; t&aelig;kin s&iacute;n &iacute; sk&yacute;rri t&aelig;kjaskr&aacute;.",
      "Snjalli H&uacute;sv&ouml;r&eth;urinn er hluti af Snjallh&uacute;s lausnunum fr&aacute; Triotech. Kerfi&eth; er byggt fyrir &iacute;slenska vi&eth;skiptavini og fyrsta &uacute;tg&aacute;fan leggur &aacute;herslu &aacute; &aacute;rei&eth;anlega yfirs&yacute;n, vi&eth;varanir og einfaldan rekstur."
    ],
    cards: [
      ["T&aelig;kjav&ouml;ktun", "Hitastig, raki, rafmagn, auka aflvaki og s&iacute;&eth;asta tenging t&aelig;kis."],
      ["Vi&eth;varanir", "Vi&eth;v&ouml;runarskr&aacute;, rau&eth;ar l&iacute;nur &iacute; t&aelig;kjaskr&aacute; og samantekt &iacute; t&ouml;lvup&oacute;sti."],
      ["Fyrirt&aelig;kja&thorn;j&oacute;nusta", "A&eth;gangur er bundinn vi&eth; vi&eth;skiptavini og hver notandi s&eacute;r a&eth;eins s&iacute;n t&aelig;ki."]
    ]
  },
  contact: {
    title: "Contact | Triotech Snjallhus",
    eyebrow: "Contact",
    heading: "Hafa samband",
    lead: "Fyrir a&eth;gang, &thorn;j&oacute;nustu e&eth;a spurningar um Snjallh&uacute;s kerfi Triotech.",
    body: [
      "Sendu okkur t&ouml;lvup&oacute;st &aacute; <a href=\"mailto:info@triotech.is\">info@triotech.is</a>.",
      "Ef &thorn;&uacute; ert vi&eth;skiptavinur og vantar a&eth;sto&eth; vi&eth; innskr&aacute;ningu, taktu fram nafn fyrirt&aelig;kis og netfangi&eth; sem nota&eth; er &iacute; kerfinu."
    ],
    cards: [
      ["A&eth;gangur", "N&yacute;ir notendur f&aacute; a&eth;gang fr&aacute; Triotech e&eth;a kerfisstj&oacute;ra vi&eth;skiptavinar."],
      ["Vi&eth;varanir", "Vi&eth;v&ouml;runarp&oacute;star eru stilltir inni &iacute; kerfinu af heimilu&eth;um notendum."],
      ["&THORN;j&oacute;nusta", "Kerfi&eth; er reki&eth; af Triotech fyrir vi&eth;skiptavini &aacute; &Iacute;slandi."]
    ]
  },
  privacy: {
    title: "Privacy | Triotech Snjallhus",
    eyebrow: "Privacy",
    heading: "Pers&oacute;nuvernd og g&ouml;gn",
    lead: "Snjallh&uacute;s kerfi&eth; geymir a&eth;eins g&ouml;gn sem &thorn;arf til a&eth; reka t&aelig;kjav&ouml;ktun, a&eth;gangsst&yacute;ringu og vi&eth;varanir.",
    body: [
      "Kerfi&eth; getur geymt netf&ouml;ng notenda, hlutverk notenda, t&aelig;kjaau&eth;kenni, stillingar t&aelig;kja, s&iacute;man&uacute;mer og sta&eth;setningarl&yacute;singu t&aelig;kis ef vi&eth;skiptavinur skr&aacute;ir &thorn;&aelig;r uppl&yacute;singar.",
      "T&aelig;ki senda rekstrarg&ouml;gn eins og hitastig, raka, rafmagnsst&ouml;&eth;u, t&aelig;kjast&ouml;&eth;u og vi&eth;varanir. Vi&eth;v&ouml;runarskr&aacute; er geymd svo vi&eth;skiptavinur geti s&eacute;&eth; hva&eth; ger&eth;ist og hven&aelig;r.",
      "A&eth;gangur er takmarka&eth;ur vi&eth; heimila&eth;a notendur. Almennir vi&eth;skiptavinir sj&aacute; a&eth;eins t&aelig;ki sem tengjast s&iacute;num vi&eth;skiptamanni."
    ],
    cards: [
      ["Markmi&eth;", "A&eth; reka v&ouml;ktunarkerfi og senda vi&eth;varanir."],
      ["Geymsla", "G&ouml;gn eru geymd &aacute; &thorn;j&oacute;num sem Triotech st&yacute;rir e&eth;a notar fyrir &thorn;j&oacute;nustuna."],
      ["Hafa samband", "Fyrir bei&eth;nir um g&ouml;gn e&eth;a lei&eth;r&eacute;ttingu: info@triotech.is."]
    ]
  },
  terms: {
    title: "Terms | Triotech Snjallhus",
    eyebrow: "Terms",
    heading: "Notkunarskilm&aacute;lar",
    lead: "Snjallh&uacute;s kerfi&eth; er rekstrar- og v&ouml;ktunarlausn fyrir vi&eth;skiptavini Triotech.",
    body: [
      "A&eth;gangur a&eth; kerfinu er fyrir vi&eth;skiptavini sem hafa virka &thorn;j&oacute;nustu e&eth;a prufu. Notendur bera &aacute;byrg&eth; &aacute; a&eth; halda lykilor&eth;um &ouml;ruggum og skr&aacute; r&eacute;ttar vi&eth;v&ouml;runaruppl&yacute;singar.",
      "Kerfi&eth; er &aelig;tla&eth; til a&eth; hj&aacute;lpa vi&eth; eftirlit og vi&eth;varanir. &THORN;a&eth; kemur ekki &iacute; sta&eth; reglubundins eftirlits &thorn;ar sem l&ouml;g, reglur e&eth;a rekstrar&ouml;ryggi krefjast &thorn;ess.",
      "Triotech getur uppf&aelig;rt &thorn;j&oacute;nustuna, &ouml;ryggisstillingar og notendavi&eth;m&oacute;t til a&eth; b&aelig;ta rekstur og &ouml;ryggi."
    ],
    cards: [
      ["A&eth;gangur", "A&eth;gangur er bundinn vi&eth; virka &thorn;j&oacute;nustu og heimila&eth;a notendur."],
      ["Vi&eth;varanir", "Notendur bera &aacute;byrg&eth; &aacute; r&eacute;ttum netf&ouml;ngum og vi&eth;v&ouml;runarstillingum."],
      ["&THORN;j&oacute;nusta", "Fyrir spurningar um skilm&aacute;la e&eth;a &thorn;j&oacute;nustu: info@triotech.is."]
    ]
  }
};

function publicPageHtml(pageName = "home") {
  const page = publicPageText[pageName] || publicPageText.home;
  const nav = [
    ["home", "/", "Fors&iacute;&eth;a"],
    ["contact", "/contact", "Samband"],
    ["privacy", "/privacy", "Pers&oacute;nuvernd"],
    ["terms", "/terms", "Skilm&aacute;lar"],
    ["dashboard", "/dashboard", "Innskr&aacute;ning"]
  ];

  const navHtml = nav.map(([key, href, label]) => (
    `<a class="${key === pageName ? "active" : ""}" href="${href}">${label}</a>`
  )).join("");

  const bodyHtml = page.body.map((paragraph) => `<p>${paragraph}</p>`).join("");
  const cardsHtml = page.cards.map(([title, text]) => (
    `<article><h2>${title}</h2><p>${text}</p></article>`
  )).join("");

  return String.raw`<!doctype html>
<html lang="is">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="description" content="Triotech Snjallhus IoT monitoring for devices, alarms, temperature, humidity and power state.">
  <title>${page.title}</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #1e2939;
      --panel: #243247;
      --panel-soft: #2b3a51;
      --line: #41506a;
      --text: #eef7ff;
      --muted: #a9b8cc;
      --brand: #7dd3fc;
      --brand-dark: #7ff5fb;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: Arial, Helvetica, sans-serif;
      font-size: 16px;
      line-height: 1.55;
    }

    a,
    a:visited {
      color: var(--brand);
    }

    header {
      background: #172235;
      border-bottom: 1px solid var(--line);
    }

    .topbar,
    main,
    footer {
      width: min(1120px, calc(100% - 36px));
      margin: 0 auto;
    }

    .topbar {
      min-height: 70px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
    }

    .brand {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      font-weight: 700;
      color: var(--text);
      text-decoration: none;
      font-size: 18px;
    }

    .brand-logo {
      width: 48px;
      height: 48px;
      border-radius: 8px;
      object-fit: cover;
      background: var(--bg);
      border: 1px solid rgba(255, 255, 255, 0.12);
    }

    nav {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }

    nav a {
      color: var(--text);
      text-decoration: none;
      border: 1px solid transparent;
      border-radius: 6px;
      padding: 7px 10px;
      background: transparent;
      font-size: 14px;
    }

    nav a:hover,
    nav a.active {
      border-color: var(--line);
      background: var(--panel-soft);
      color: var(--brand-dark);
    }

    main {
      padding: 46px 0 42px;
    }

    .hero {
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(280px, 0.65fr);
      gap: 32px;
      align-items: start;
    }

    .eyebrow {
      margin: 0 0 8px;
      color: var(--brand-dark);
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0;
      font-size: 13px;
    }

    h1 {
      margin: 0 0 16px;
      font-size: 48px;
      line-height: 1.05;
      letter-spacing: 0;
    }

    .lead {
      margin: 0;
      color: var(--muted);
      font-size: 20px;
      max-width: 760px;
    }

    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 20px;
    }

    .panel strong {
      display: block;
      margin-bottom: 10px;
    }

    .panel a,
    .content a {
      color: var(--brand-dark);
      font-weight: 700;
    }

    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 22px;
    }

    .button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 40px;
      border-radius: 6px;
      padding: 0 14px;
      border: 1px solid #38bdf8;
      background: #0e7490;
      color: #ecfeff;
      text-decoration: none;
      font-weight: 700;
    }

    .button.secondary {
      background: transparent;
      color: var(--brand-dark);
      border-color: var(--line);
    }

    .content {
      margin-top: 34px;
      max-width: 850px;
    }

    .cards {
      margin-top: 30px;
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
    }

    article {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 18px;
    }

    article h2 {
      margin: 0 0 8px;
      font-size: 17px;
    }

    article p {
      margin: 0;
      color: var(--muted);
    }

    footer {
      padding: 22px 0 34px;
      border-top: 1px solid var(--line);
      color: var(--muted);
      font-size: 14px;
    }

    @media (max-width: 760px) {
      .topbar {
        align-items: flex-start;
        flex-direction: column;
        padding: 16px 0;
      }

      nav {
        justify-content: flex-start;
      }

      .hero,
      .cards {
        grid-template-columns: 1fr;
      }

      main {
        padding-top: 30px;
      }

      h1 {
        font-size: 34px;
      }
    }
  </style>
</head>
<body>
  <header>
    <div class="topbar">
      <a class="brand" href="/">
        <img class="brand-logo" src="/assets/LOGO_Triotech.png" alt="Triotech">
        <span>Triotech Snjallh&uacute;s</span>
      </a>
      <nav aria-label="Public navigation">${navHtml}</nav>
    </div>
  </header>
  <main>
    <section class="hero">
      <div>
        <p class="eyebrow">${page.eyebrow}</p>
        <h1>${page.heading}</h1>
        <p class="lead">${page.lead}</p>
        <div class="actions">
          <a class="button" href="/dashboard">Innskr&aacute;ning</a>
          <a class="button secondary" href="/contact">Hafa samband</a>
        </div>
      </div>
      <aside class="panel">
        <strong>Smart Home Guardian</strong>
        <p>Business IoT monitoring dashboard for Triotech customers.</p>
        <p><a href="mailto:info@triotech.is">info@triotech.is</a></p>
      </aside>
    </section>
    <section class="content">${bodyHtml}</section>
    <section class="cards">${cardsHtml}</section>
  </main>
  <footer>
    Triotech &middot; Snjallh&uacute;s IoT monitoring &middot; <a href="mailto:info@triotech.is">info@triotech.is</a>
  </footer>
</body>
</html>`;
}

function publicRobotsTxt(req) {
  const baseUrl = publicBaseUrl(req);

  return [
    "User-agent: *",
    "Allow: /",
    "Allow: /contact",
    "Allow: /privacy",
    "Allow: /terms",
    "Disallow: /dashboard",
    "Disallow: /settings",
    "Disallow: /alarms",
    "Disallow: /reset-password",
    "Disallow: /api/",
    `Sitemap: ${baseUrl}/sitemap.xml`,
    ""
  ].join("\n");
}

function publicSitemapXml(req) {
  const baseUrl = publicBaseUrl(req);
  const urls = ["", "/contact", "/privacy", "/terms"].map((path) => (
    `  <url><loc>${baseUrl}${path}</loc></url>`
  )).join("\n");

  return String.raw`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>
`;
}

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

function generateTempPassword() {
  return crypto.randomBytes(18).toString("base64url").slice(0, 18);
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

function hashResetToken(token) {
  return crypto.createHash("sha256").update(String(token)).digest("hex");
}

function publicBaseUrl(req) {
  if (PUBLIC_APP_URL) {
    return PUBLIC_APP_URL;
  }

  const forwardedProto = String(req.headers["x-forwarded-proto"] || "").split(",")[0].trim();
  const proto = forwardedProto || (req.protocol || "https");
  const forwardedHost = String(req.headers["x-forwarded-host"] || "").split(",")[0].trim();
  const host = forwardedHost || req.headers.host || "snjallhus.triotech.is";

  return `${proto}://${host}`.replace(/\/+$/, "");
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
    role: "super_admin",
    customer_id: null,
    must_change_password: false
  };
}

function isAdminRole(role) {
  return role === "super_admin" || role === "admin";
}

function isSuperUser(user) {
  return Boolean(user && user.role === "super_admin");
}

function isAdminUser(user) {
  return Boolean(user && isAdminRole(user.role));
}

function subscriptionFromRow(row) {
  if (!row || row.role === "super_admin") {
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

  if (isSuperUser(user)) {
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

function settingValuesEqual(left, right) {
  if (left === null || left === undefined || right === null || right === undefined) {
    return left === right;
  }

  const leftNumber = Number(left);
  const rightNumber = Number(right);

  if (!Number.isFinite(leftNumber) || !Number.isFinite(rightNumber)) {
    return String(left) === String(right);
  }

  return Math.abs(leftNumber - rightNumber) < 0.05;
}

function rememberPendingDesiredSettings(deviceId, settings) {
  const expiresAt = Date.now() + DEVICE_SETTING_WEB_PENDING_MS;
  const pending = pendingDesiredSettings[deviceId] || {};

  for (const [key, value] of Object.entries(settings)) {
    if (!key.startsWith("desired_") || key === "settings_updated_at") {
      continue;
    }

    pending[key] = { value, expiresAt };
  }

  pendingDesiredSettings[deviceId] = pending;
}

function isDesiredSettingProtected(deviceId, desiredKey, activeValue) {
  const pending = pendingDesiredSettings[deviceId];

  if (!pending || !pending[desiredKey]) {
    return false;
  }

  const pendingSetting = pending[desiredKey];

  if (Date.now() > pendingSetting.expiresAt) {
    delete pending[desiredKey];
    return false;
  }

  if (settingValuesEqual(pendingSetting.value, activeValue)) {
    delete pending[desiredKey];
    return false;
  }

  return true;
}

async function syncDesiredSettingFromActiveState(deviceId, desiredKey, activeValue) {
  if (activeValue === null || activeValue === undefined) {
    return;
  }

  if (isDesiredSettingProtected(deviceId, desiredKey, activeValue)) {
    app.log.info({ deviceId, desiredKey, activeValue }, "Keeping recent web setting while waiting for device to apply it");
    return;
  }

  const settings = settingsForDevice(deviceId);

  if (settingValuesEqual(settings[desiredKey], activeValue)) {
    return;
  }

  await saveDeviceSettings(deviceId, {
    low_temperature: desiredKey === "desired_low_temperature" ? activeValue : settings.desired_low_temperature,
    high_temperature: desiredKey === "desired_high_temperature" ? activeValue : settings.desired_high_temperature,
    high_humidity: desiredKey === "desired_high_humidity" ? activeValue : settings.desired_high_humidity,
    telemetry_interval_sec: desiredKey === "desired_telemetry_interval_sec" ? activeValue : settings.desired_telemetry_interval_sec
  }, "device");
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
    CREATE TABLE IF NOT EXISTS password_reset_tokens (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
      token_hash TEXT UNIQUE NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      used_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS devices (
      device_id TEXT PRIMARY KEY,
      customer_id TEXT REFERENCES customers(id),
      active BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await db.query("ALTER TABLE devices ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT true");

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
    CREATE TABLE IF NOT EXISTS device_telemetry_log (
      id BIGSERIAL PRIMARY KEY,
      customer_id TEXT,
      device_id TEXT NOT NULL,
      temperature NUMERIC,
      humidity NUMERIC,
      power_source TEXT,
      ble_power_monitor_connection TEXT,
      alarm_state TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
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
    CREATE TABLE IF NOT EXISTS customer_alarm_email_recipients (
      customer_id TEXT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
      email TEXT NOT NULL,
      active BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (customer_id, email)
    )
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS device_alarm_log_created_idx
    ON device_alarm_log (created_at DESC)
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS device_telemetry_log_device_created_idx
    ON device_telemetry_log (device_id, created_at DESC)
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS device_telemetry_log_customer_created_idx
    ON device_telemetry_log (customer_id, created_at DESC)
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
    CREATE INDEX IF NOT EXISTS password_reset_tokens_user_idx
    ON password_reset_tokens (user_id, expires_at DESC)
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS devices_customer_idx
    ON devices (customer_id)
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS customer_alarm_email_recipients_customer_idx
    ON customer_alarm_email_recipients (customer_id)
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
      VALUES ($1, NULL, $2, $3, 'super_admin', true, true)
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
    WHERE active = true
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
      INSERT INTO devices (device_id, customer_id, active)
      VALUES ($1, $2, true)
      ON CONFLICT (device_id)
      DO UPDATE SET device_id = EXCLUDED.device_id
      RETURNING device_id, customer_id, active
    `,
    [deviceId, DEFAULT_CUSTOMER_ID]
  );

  if (result.rows[0].active === false) {
    return null;
  }

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

function requestError(message, statusCode = 400) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

function normalizeCustomerId(value) {
  const id = String(value || "").trim().toLowerCase().replace(/\s+/g, "-");

  if (!/^[a-z0-9_-]{2,64}$/.test(id)) {
    throw requestError("Customer ID must be 2-64 letters, numbers, dash, or underscore");
  }

  return id;
}

function normalizeBoolean(value, fallback = true) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  return value === true || value === "true" || value === 1 || value === "1";
}

function normalizeSubscriptionStatus(value, fallback = "active") {
  const status = String(value || fallback).trim().toLowerCase();

  if (!VALID_SUBSCRIPTION_STATUSES.has(status)) {
    throw requestError("Invalid subscription status");
  }

  return status;
}

function normalizeSubscriptionPlan(value, fallback = "monthly") {
  const plan = String(value || fallback).trim().toLowerCase();

  if (!VALID_SUBSCRIPTION_PLANS.has(plan)) {
    throw requestError("Invalid subscription plan");
  }

  return plan;
}

function normalizePaidUntil(value, fallback = null) {
  const input = value === undefined ? fallback : value;

  if (input === null || input === "") {
    return null;
  }

  const parsed = new Date(input);

  if (Number.isNaN(parsed.getTime())) {
    throw requestError("Invalid paid_until date");
  }

  return parsed.toISOString();
}

async function createCustomer(values) {
  if (!db) {
    throw requestError("Database is not configured", 503);
  }

  const id = normalizeCustomerId(values.id || values.customer_id);
  const name = String(values.name || "").trim();

  if (!name) {
    throw requestError("Customer name is required");
  }

  const result = await db.query(
    `
      INSERT INTO customers (
        id,
        name,
        subscription_status,
        subscription_plan,
        paid_until,
        login_enabled
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING
        id,
        name,
        subscription_status,
        subscription_plan,
        paid_until,
        login_enabled,
        updated_at
    `,
    [
      id,
      name,
      normalizeSubscriptionStatus(values.subscription_status, "active"),
      normalizeSubscriptionPlan(values.subscription_plan, "monthly"),
      normalizePaidUntil(values.paid_until, null),
      normalizeBoolean(values.login_enabled, true)
    ]
  ).catch((error) => {
    if (error.code === "23505") {
      throw requestError("Customer already exists", 409);
    }

    throw error;
  });

  return customerFromRow(result.rows[0]);
}

async function saveCustomerDetails(customerId, values) {
  if (!db) {
    throw requestError("Database is not configured", 503);
  }

  const current = await db.query(
    "SELECT * FROM customers WHERE id = $1 LIMIT 1",
    [customerId]
  );

  if (current.rowCount === 0) {
    throw requestError("Customer not found", 404);
  }

  const row = current.rows[0];
  const name = values.name === undefined ? row.name : String(values.name || "").trim();

  if (!name) {
    throw requestError("Customer name is required");
  }

  const result = await db.query(
    `
      UPDATE customers
      SET
        name = $2,
        subscription_status = $3,
        subscription_plan = $4,
        paid_until = $5,
        login_enabled = $6,
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
    [
      customerId,
      name,
      normalizeSubscriptionStatus(values.subscription_status, row.subscription_status),
      normalizeSubscriptionPlan(values.subscription_plan, row.subscription_plan),
      normalizePaidUntil(values.paid_until, row.paid_until),
      normalizeBoolean(values.login_enabled, row.login_enabled)
    ]
  );

  return customerFromRow(result.rows[0]);
}

async function deleteCustomer(customerId) {
  if (!db) {
    throw requestError("Database is not configured", 503);
  }

  const usage = await db.query(
    `
      SELECT
        (SELECT count(*)::int FROM devices WHERE customer_id = $1) AS devices,
        (SELECT count(*)::int FROM app_users WHERE customer_id = $1) AS users,
        (SELECT count(*)::int FROM customer_alarm_email_recipients WHERE customer_id = $1) AS email_recipients
    `,
    [customerId]
  );
  const counts = usage.rows[0] || {};

  if (counts.devices || counts.users || counts.email_recipients) {
    throw requestError("Customer still has devices, users, or alarm email recipients", 409);
  }

  const result = await db.query("DELETE FROM customers WHERE id = $1", [customerId]);

  if (result.rowCount === 0) {
    throw requestError("Customer not found", 404);
  }
}

function adminUserFromRow(row) {
  return {
    id: row.id,
    customer_id: row.customer_id,
    customer_name: row.customer_name || "",
    email: row.email,
    role: row.role,
    must_change_password: Boolean(row.must_change_password),
    active: Boolean(row.active),
    updated_at: row.updated_at
  };
}

function normalizeUserRole(value, fallback = "customer") {
  const role = String(value || fallback).trim().toLowerCase();

  if (!["customer", "admin", "super_admin"].includes(role)) {
    throw requestError("Invalid user role");
  }

  return role;
}

async function listAdminUsers() {
  if (!db) {
    return [];
  }

  const result = await db.query(`
    SELECT
      u.id,
      u.customer_id,
      c.name AS customer_name,
      u.email,
      u.role,
      u.must_change_password,
      u.active,
      u.updated_at
    FROM app_users u
    LEFT JOIN customers c ON c.id = u.customer_id
    ORDER BY u.role DESC, u.email
  `);

  return result.rows.map(adminUserFromRow);
}

async function createAdminUser(values) {
  if (!db) {
    throw requestError("Database is not configured", 503);
  }

  const email = normalizeEmail(values.email);
  const role = normalizeUserRole(values.role, "customer");
  const customerId = role === "super_admin"
    ? (values.customer_id ? String(values.customer_id).trim() : null)
    : String(values.customer_id || "").trim();

  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    throw requestError("Valid email is required");
  }

  if (customerId) {
    await assertCustomerExists(customerId);
  } else if (role !== "super_admin") {
    throw requestError("Customer is required for this user role");
  }

  const tempPassword = generateTempPassword();
  const result = await db.query(
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
      VALUES ($1, $2, $3, $4, $5, true, true)
      RETURNING
        id,
        customer_id,
        (SELECT name FROM customers WHERE id = $2) AS customer_name,
        email,
        role,
        must_change_password,
        active,
        updated_at
    `,
    [crypto.randomUUID(), customerId, email, hashPassword(tempPassword), role]
  ).catch((error) => {
    if (error.code === "23505") {
      throw requestError("User already exists", 409);
    }

    throw error;
  });

  return {
    user: adminUserFromRow(result.rows[0]),
    temp_password: tempPassword
  };
}

async function updateAdminUser(userId, values, currentUser) {
  if (!db) {
    throw requestError("Database is not configured", 503);
  }

  const current = await db.query(
    `
      SELECT
        id,
        customer_id,
        email,
        role,
        must_change_password,
        active
      FROM app_users
      WHERE id = $1
      LIMIT 1
    `,
    [userId]
  );

  if (current.rowCount === 0) {
    throw requestError("User not found", 404);
  }

  const row = current.rows[0];
  const role = normalizeUserRole(values.role, row.role);
  const customerId = role === "super_admin"
    ? (values.customer_id === undefined ? row.customer_id : (values.customer_id ? String(values.customer_id).trim() : null))
    : (values.customer_id === undefined ? row.customer_id : String(values.customer_id || "").trim());
  const active = normalizeBoolean(values.active, row.active);
  const mustChangePassword = normalizeBoolean(values.must_change_password, row.must_change_password);

  if (customerId) {
    await assertCustomerExists(customerId);
  } else if (role !== "super_admin") {
    throw requestError("Customer is required for this user role");
  }

  if (currentUser && currentUser.id === userId && (!active || role !== "super_admin")) {
    throw requestError("You cannot remove your own super-admin access", 409);
  }

  let tempPassword = null;
  let passwordSql = "";
  const params = [userId, customerId, role, active, mustChangePassword];

  if (values.reset_password === true || values.reset_password === "true") {
    tempPassword = generateTempPassword();
    params.push(hashPassword(tempPassword));
    passwordSql = ", password_hash = $6";
  }

  const result = await db.query(
    `
      UPDATE app_users
      SET
        customer_id = $2,
        role = $3,
        active = $4,
        must_change_password = $5,
        updated_at = now()
        ${passwordSql}
      WHERE id = $1
      RETURNING
        id,
        customer_id,
        (SELECT name FROM customers WHERE id = app_users.customer_id) AS customer_name,
        email,
        role,
        must_change_password,
        active,
        updated_at
    `,
    params
  );

  return {
    user: adminUserFromRow(result.rows[0]),
    temp_password: tempPassword
  };
}

async function sendWelcomeToAdminUser(userId, loginUrl) {
  if (!db) {
    throw requestError("Database is not configured", 503);
  }

  if (!smtpIsConfigured()) {
    throw requestError("SMTP is not configured", 503);
  }

  const client = await db.connect();
  const tempPassword = generateTempPassword();

  try {
    await client.query("BEGIN");

    const current = await client.query(
      `
        SELECT id, email, active
        FROM app_users
        WHERE id = $1
        FOR UPDATE
      `,
      [userId]
    );

    if (current.rowCount === 0) {
      throw requestError("User not found", 404);
    }

    if (!current.rows[0].active) {
      throw requestError("User is inactive", 409);
    }

    await client.query(
      `
        UPDATE app_users
        SET password_hash = $2,
            must_change_password = true,
            updated_at = now()
        WHERE id = $1
      `,
      [userId, hashPassword(tempPassword)]
    );

    const updated = await client.query(
      `
        SELECT
          u.id,
          u.customer_id,
          c.name AS customer_name,
          u.email,
          u.role,
          u.must_change_password,
          u.active,
          u.updated_at
        FROM app_users u
        LEFT JOIN customers c ON c.id = u.customer_id
        WHERE u.id = $1
      `,
      [userId]
    );

    await sendWelcomeEmail(current.rows[0].email, loginUrl, tempPassword);
    await client.query("COMMIT");

    return {
      user: adminUserFromRow(updated.rows[0])
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

function adminDeviceFromRow(row) {
  return {
    device_id: row.device_id,
    customer_id: row.customer_id,
    customer_name: row.customer_name || "",
    updated_at: row.updated_at
  };
}

async function listAdminDevices() {
  if (!db) {
    return [];
  }

  const result = await db.query(`
    SELECT
      d.device_id,
      d.customer_id,
      c.name AS customer_name,
      d.updated_at
    FROM devices d
    LEFT JOIN customers c ON c.id = d.customer_id
    WHERE d.active = true
    ORDER BY d.device_id
  `);

  return result.rows.map(adminDeviceFromRow);
}

async function assignDeviceToCustomer(deviceId, customerId) {
  if (!db) {
    throw requestError("Database is not configured", 503);
  }

  if (!isValidDeviceId(deviceId)) {
    throw requestError("Invalid device ID");
  }

  await assertCustomerExists(customerId);

  const result = await db.query(
    `
      INSERT INTO devices (device_id, customer_id, active)
      VALUES ($1, $2, true)
      ON CONFLICT (device_id)
      DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        active = true,
        updated_at = now()
      RETURNING
        device_id,
        customer_id,
        (SELECT name FROM customers WHERE id = $2) AS customer_name,
        updated_at
    `,
    [deviceId, customerId]
  );

  await refreshDeviceRegistry();
  getDevice(deviceId);
  publishWebSocketState();

  return adminDeviceFromRow(result.rows[0]);
}

function removeDeviceFromLiveState(deviceId) {
  delete deviceRegistry[deviceId];
  delete devices[deviceId];
  delete pendingDesiredSettings[deviceId];
  delete lastTelemetryLogAt[deviceId];

  if (pendingTelemetryLogTimers[deviceId]) {
    clearTimeout(pendingTelemetryLogTimers[deviceId]);
    delete pendingTelemetryLogTimers[deviceId];
  }

  for (const topic of Object.keys(latestState)) {
    if (topicDeviceId(topic) === deviceId) {
      delete latestState[topic];
    }
  }
}

async function removeDeviceFromRegistry(deviceId) {
  if (!db) {
    throw requestError("Database is not configured", 503);
  }

  if (!isValidDeviceId(deviceId)) {
    throw requestError("Invalid device ID");
  }

  const result = await db.query(
    `
      UPDATE devices
      SET active = false,
          updated_at = now()
      WHERE device_id = $1
        AND active = true
      RETURNING device_id
    `,
    [deviceId]
  );

  if (result.rowCount === 0) {
    throw requestError("Device not found", 404);
  }

  removeDeviceFromLiveState(deviceId);
  await refreshDeviceRegistry();
  publishWebSocketState();

  return {
    device_id: deviceId
  };
}

function parseNotificationEmails(value) {
  const rawValues = Array.isArray(value)
    ? value
    : String(value || "").split(/[\s,;]+/);
  const emails = [];
  const seen = new Set();

  for (const rawValue of rawValues) {
    const email = normalizeEmail(rawValue);

    if (!email) {
      continue;
    }

    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      const error = new Error(`Invalid email address: ${email}`);
      error.statusCode = 400;
      throw error;
    }

    if (!seen.has(email)) {
      seen.add(email);
      emails.push(email);
    }
  }

  return emails;
}

function notificationSettingsCustomerId(user, requestedCustomerId = "") {
  const requested = String(requestedCustomerId || "").trim();

  if (isSuperUser(user)) {
    return requested || user.customer_id || DEFAULT_CUSTOMER_ID;
  }

  if (!user.customer_id) {
    const error = new Error("Customer is required");
    error.statusCode = 403;
    throw error;
  }

  if (requested && requested !== user.customer_id) {
    const error = new Error("Forbidden");
    error.statusCode = 403;
    throw error;
  }

  return user.customer_id;
}

async function assertCustomerExists(customerId) {
  if (!db) {
    return;
  }

  const result = await db.query(
    "SELECT id FROM customers WHERE id = $1 LIMIT 1",
    [customerId]
  );

  if (result.rowCount === 0) {
    const error = new Error("Customer not found");
    error.statusCode = 404;
    throw error;
  }
}

async function getNotificationEmails(customerId) {
  if (!db) {
    return notificationEmailSettings[customerId] || [];
  }

  const result = await db.query(
    `
      SELECT email
      FROM customer_alarm_email_recipients
      WHERE customer_id = $1
        AND active = true
      ORDER BY email
    `,
    [customerId]
  );

  const emails = result.rows.map((row) => row.email);
  notificationEmailSettings[customerId] = emails;
  return emails;
}

async function saveNotificationEmails(customerId, emails) {
  notificationEmailSettings[customerId] = emails;

  if (!db) {
    return emails;
  }

  await assertCustomerExists(customerId);

  const client = await db.connect();

  try {
    await client.query("BEGIN");
    await client.query(
      "DELETE FROM customer_alarm_email_recipients WHERE customer_id = $1",
      [customerId]
    );

    for (const email of emails) {
      await client.query(
        `
          INSERT INTO customer_alarm_email_recipients (
            customer_id,
            email,
            active
          )
          VALUES ($1, $2, true)
          ON CONFLICT (customer_id, email)
          DO UPDATE SET
            active = true,
            updated_at = now()
        `,
        [customerId, email]
      );
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }

  return getNotificationEmails(customerId);
}

function smtpIsConfigured() {
  return Boolean(
    SMTP_HOST &&
    SMTP_PORT &&
    SMTP_USER &&
    SMTP_PASS &&
    SMTP_FROM
  );
}

function alertEmailIsConfigured() {
  return ALERT_EMAIL_ENABLED && smtpIsConfigured();
}

function getMailTransporter() {
  if (!mailTransporter) {
    mailTransporter = nodemailer.createTransport({
      host: SMTP_HOST,
      port: SMTP_PORT,
      secure: SMTP_SECURE,
      auth: {
        user: SMTP_USER,
        pass: SMTP_PASS
      }
    });
  }

  return mailTransporter;
}

function alarmLabel(alarmType) {
  return String(alarmType || "alarm").replace(/_/g, " ");
}

function formatIcelandTime(value = new Date()) {
  return new Date(value).toLocaleString("en-GB", {
    timeZone: "Atlantic/Reykjavik",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

async function sendAlarmEmailBatch(customerId, alarms) {
  if (!alarms.length) {
    return;
  }

  const recipients = await getNotificationEmails(customerId);

  if (recipients.length === 0) {
    return;
  }

  if (!alertEmailIsConfigured()) {
    app.log.warn({ customerId, alarms: alarms.length }, "Alarm email batch skipped because SMTP is not configured");
    return;
  }

  const uniqueDeviceCount = new Set(alarms.map((alarm) => alarm.deviceId)).size;
  const firstAlarm = alarms[0];
  const subject = alarms.length === 1
    ? `[Snjallhus] ${alarmLabel(firstAlarm.alarmType)} alarm on ${firstAlarm.deviceId}`
    : `[Snjallhus] ${alarms.length} new alarms on ${uniqueDeviceCount} devices`;
  const rows = alarms.map((alarm) => ({
    ...alarm,
    label: alarmLabel(alarm.alarmType),
    displayTime: formatIcelandTime(alarm.createdAt)
  }));
  const text = [
    "Snjallhus alarm summary",
    "",
    `New active alarms: ${alarms.length}`,
    `Devices with alarms: ${uniqueDeviceCount}`,
    `Window: ${formatIcelandTime(rows[0].createdAt)} - ${formatIcelandTime(rows[rows.length - 1].createdAt)}`,
    "",
    ...rows.map((alarm) => [
      `Time: ${alarm.displayTime}`,
      `Device: ${alarm.deviceId}`,
      `Alarm: ${alarm.label}`,
      `Value: ${alarm.alarmValue ?? ""}`,
      `Topic: ${alarm.topic}`,
      ""
    ].join("\n"))
  ].join("\n");
  const html = `
    <h2>Snjallhus alarm summary</h2>
    <p>${alarms.length} new active alarm${alarms.length === 1 ? "" : "s"} on ${uniqueDeviceCount} device${uniqueDeviceCount === 1 ? "" : "s"}.</p>
    <table border="1" cellspacing="0" cellpadding="6">
      <thead>
        <tr>
          <th align="left">Time</th>
          <th align="left">Device</th>
          <th align="left">Alarm</th>
          <th align="left">Value</th>
          <th align="left">Topic</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map((alarm) => `
          <tr>
            <td>${escapeHtml(alarm.displayTime)}</td>
            <td>${escapeHtml(alarm.deviceId)}</td>
            <td>${escapeHtml(alarm.label)}</td>
            <td>${escapeHtml(alarm.alarmValue ?? "")}</td>
            <td>${escapeHtml(alarm.topic)}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;

  await getMailTransporter().sendMail({
    from: SMTP_FROM,
    to: SMTP_USER,
    bcc: recipients,
    subject,
    text,
    html
  });

  app.log.info({ customerId, alarms: alarms.length, devices: uniqueDeviceCount, recipients: recipients.length }, "Alarm email batch sent");
}

async function flushAlarmEmailBatch(customerId) {
  const batch = alarmEmailBatches.get(customerId);

  if (!batch) {
    return;
  }

  alarmEmailBatches.delete(customerId);

  try {
    await sendAlarmEmailBatch(customerId, batch.alarms);
  } catch (error) {
    app.log.error({ error, customerId, alarms: batch.alarms.length }, "Failed to send alarm email batch");
  }
}

function queueAlarmEmail(customerId, deviceId, alarmType, alarmValue, topic) {
  let batch = alarmEmailBatches.get(customerId);

  if (!batch) {
    batch = {
      alarms: [],
      timer: null
    };
    alarmEmailBatches.set(customerId, batch);
  }

  batch.alarms.push({
    deviceId,
    alarmType,
    alarmValue,
    topic,
    createdAt: new Date().toISOString()
  });

  if (!batch.timer) {
    batch.timer = setTimeout(() => {
      flushAlarmEmailBatch(customerId);
    }, ALERT_EMAIL_BATCH_MS);

    if (typeof batch.timer.unref === "function") {
      batch.timer.unref();
    }
  }

  app.log.info(
    { customerId, deviceId, alarmType, queued: batch.alarms.length, batchMs: ALERT_EMAIL_BATCH_MS },
    "Alarm email queued"
  );
}

async function sendPasswordResetEmail(email, resetUrl) {
  if (!smtpIsConfigured()) {
    throw new Error("SMTP is not configured");
  }

  await getMailTransporter().sendMail({
    from: SMTP_FROM,
    to: email,
    subject: "Snjallhus password reset",
    text: [
      "Password reset was requested for your Snjallhus account.",
      "",
      "Open this link to choose a new password:",
      resetUrl,
      "",
      "This link is valid for 30 minutes. If you did not request this, you can ignore this email."
    ].join("\n"),
    html: `
      <h2>Snjallhus password reset</h2>
      <p>Password reset was requested for your Snjallhus account.</p>
      <p><a href="${escapeHtml(resetUrl)}">Choose a new password</a></p>
      <p>This link is valid for 30 minutes. If you did not request this, you can ignore this email.</p>
    `
  });

  app.log.info({ email }, "Password reset email sent");
}

async function sendWelcomeEmail(email, loginUrl, tempPassword) {
  if (!smtpIsConfigured()) {
    throw new Error("SMTP is not configured");
  }

  await getMailTransporter().sendMail({
    from: SMTP_FROM,
    to: email,
    subject: "Velkomin/n í Snjallhús",
    text: [
      "Velkomin/n í Snjallhús kerfið.",
      "",
      "Innskráningarslóð:",
      loginUrl,
      "",
      `Netfang: ${email}`,
      `Tímabundið lykilorð: ${tempPassword}`,
      "",
      "Við fyrstu innskráningu verður þú beðin/n um að velja nýtt lykilorð."
    ].join("\n"),
    html: `
      <h2>Velkomin/n í Snjallhús kerfið</h2>
      <p>Notandaaðgangur hefur verið stofnaður fyrir þig.</p>
      <p><a href="${escapeHtml(loginUrl)}">Opna Snjallhús</a></p>
      <p><strong>Netfang:</strong> ${escapeHtml(email)}</p>
      <p><strong>Tímabundið lykilorð:</strong> ${escapeHtml(tempPassword)}</p>
      <p>Við fyrstu innskráningu verður þú beðin/n um að velja nýtt lykilorð.</p>
    `
  });

  app.log.info({ email }, "Welcome email sent");
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

  if (isSuperUser(user)) {
    return true;
  }

  const registry = deviceRegistry[deviceId];
  return Boolean(registry && registry.customer_id === user.customer_id);
}

function deviceKnown(deviceId) {
  return Boolean(
    devices[deviceId] ||
    deviceRegistry[deviceId] ||
    deviceContacts[deviceId] ||
    deviceSettings[deviceId]
  );
}

function filterRawStateForUser(user) {
  if (isSuperUser(user)) {
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
  if (isSuperUser(user)) {
    return deviceViews;
  }

  return deviceViews.filter((device) => canAccessDevice(user, device.device_id));
}

function filterAlarmsForUser(user, alarms) {
  if (isSuperUser(user)) {
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

async function saveDeviceSettings(deviceId, values, source = "web") {
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
    if (source === "web") {
      rememberPendingDesiredSettings(deviceId, desiredSettings);
    }
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
  if (source === "web") {
    rememberPendingDesiredSettings(deviceId, savedSettings);
  }
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
  const customerId = deviceRegistry[deviceId]?.customer_id || DEFAULT_CUSTOMER_ID;

  if (!db) {
    addMemoryAlarmEvent(deviceId, alarmType, payload, topic, alarmValue);
    queueAlarmEmail(customerId, deviceId, alarmType, alarmValue, topic);
    return;
  }

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
    queueAlarmEmail(customerId, deviceId, alarmType, alarmValue, topic);
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

function telemetryRangeMs(range) {
  if (range === "7d") {
    return 7 * 24 * 60 * 60 * 1000;
  }

  if (range === "30d") {
    return 30 * 24 * 60 * 60 * 1000;
  }

  return 24 * 60 * 60 * 1000;
}

function telemetryValue(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function scheduleTelemetrySnapshot(deviceId) {
  if (!db || pendingTelemetryLogTimers[deviceId]) {
    return;
  }

  pendingTelemetryLogTimers[deviceId] = setTimeout(() => {
    delete pendingTelemetryLogTimers[deviceId];
    logTelemetrySnapshot(deviceId);
  }, 2000);
}

async function logTelemetrySnapshot(deviceId) {
  if (!db) {
    return;
  }

  const device = devices[deviceId];
  if (!device) {
    return;
  }

  const temperature = telemetryValue(device.temperature);
  const humidity = telemetryValue(device.humidity);

  if (temperature === null || humidity === null) {
    return;
  }

  const now = Date.now();
  const lastLoggedAt = lastTelemetryLogAt[deviceId] || 0;

  if (lastLoggedAt && now - lastLoggedAt < TELEMETRY_LOG_INTERVAL_MS) {
    return;
  }

  lastTelemetryLogAt[deviceId] = now;

  try {
    await db.query(
      `
        INSERT INTO device_telemetry_log (
          customer_id,
          device_id,
          temperature,
          humidity,
          power_source,
          ble_power_monitor_connection,
          alarm_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      `,
      [
        deviceRegistry[deviceId]?.customer_id || DEFAULT_CUSTOMER_ID,
        deviceId,
        temperature,
        humidity,
        device.power_source || null,
        device.ble_power_monitor_connection || null,
        device.alarm_state || null
      ]
    );
  } catch (error) {
    lastTelemetryLogAt[deviceId] = lastLoggedAt;
    app.log.error({ error, deviceId }, "Failed to log telemetry snapshot");
  }
}

async function getTelemetryHistory(deviceId, range = "24h") {
  if (!db) {
    return [];
  }

  const since = new Date(Date.now() - telemetryRangeMs(range)).toISOString();
  const result = await db.query(
    `
      SELECT
        id,
        customer_id,
        device_id,
        temperature,
        humidity,
        power_source,
        ble_power_monitor_connection,
        alarm_state,
        created_at
      FROM device_telemetry_log
      WHERE device_id = $1
        AND created_at >= $2
      ORDER BY created_at ASC, id ASC
      LIMIT 10000
    `,
    [deviceId, since]
  );

  return result.rows.map((row) => ({
    ...row,
    temperature: row.temperature === null ? null : Number(row.temperature),
    humidity: row.humidity === null ? null : Number(row.humidity)
  }));
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
  const registry = await ensureDeviceRegistered(deviceId);

  if (!registry) {
    app.log.info({ deviceId, topic }, "Ignoring MQTT message from removed device");
    return;
  }

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
    scheduleTelemetrySnapshot(deviceId);
  }

  if (group === "tele" && tag === "humidity") {
    device.humidity = numberOrNull(payloadText);
    scheduleTelemetrySnapshot(deviceId);
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
    await syncDesiredSettingFromActiveState(deviceId, "desired_telemetry_interval_sec", device.telemetry_interval_sec);
  }

  if (group === "state" && tag === "low_temperature") {
    device.low_temperature = numberOrNull(payloadText);
    await syncDesiredSettingFromActiveState(deviceId, "desired_low_temperature", device.low_temperature);
  }

  if (group === "state" && tag === "high_temperature") {
    device.high_temperature = numberOrNull(payloadText);
    await syncDesiredSettingFromActiveState(deviceId, "desired_high_temperature", device.high_temperature);
  }

  if (group === "state" && tag === "high_humidity") {
    device.high_humidity = numberOrNull(payloadText);
    await syncDesiredSettingFromActiveState(deviceId, "desired_high_humidity", device.high_humidity);
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

app.get("/assets/LOGO_Triotech.png", async (req, reply) => {
  return reply.type("image/png").send(fs.createReadStream(LOGO_FILE));
});

app.get("/", async (req, reply) => {
  return reply.type("text/html").send(publicPageHtml("home"));
});

app.get("/contact", async (req, reply) => {
  return reply.type("text/html").send(publicPageHtml("contact"));
});

app.get("/privacy", async (req, reply) => {
  return reply.type("text/html").send(publicPageHtml("privacy"));
});

app.get("/terms", async (req, reply) => {
  return reply.type("text/html").send(publicPageHtml("terms"));
});

app.get("/robots.txt", async (req, reply) => {
  return reply.type("text/plain").send(publicRobotsTxt(req));
});

app.get("/sitemap.xml", async (req, reply) => {
  return reply.type("application/xml").send(publicSitemapXml(req));
});

app.get("/app", async (req, reply) => {
  return reply.type("text/html").send(dashboardHtml);
});

app.get("/dashboard", async (req, reply) => {
  return reply.type("text/html").send(dashboardHtml);
});

app.get("/devices/:deviceId", async (req, reply) => {
  return reply.type("text/html").send(dashboardHtml);
});

app.get("/alarms", async (req, reply) => {
  return reply.type("text/html").send(dashboardHtml);
});

app.get("/settings", async (req, reply) => {
  return reply.type("text/html").send(dashboardHtml);
});

app.get("/reset-password", async (req, reply) => {
  return reply.type("text/html").send(dashboardHtml);
});

app.post("/api/v1/password-reset/request", async (req, reply) => {
  const genericResponse = {
    ok: true,
    message: "If the email exists, a reset link has been sent."
  };

  if (!db) {
    return genericResponse;
  }

  const email = normalizeEmail(req.body?.email);

  if (!email) {
    return genericResponse;
  }

  try {
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
        WHERE lower(u.email) = lower($1)
        LIMIT 1
      `,
      [email]
    );

    const row = result.rows[0];

    if (row && row.active && userCanUseDashboard(userFromRow(row))) {
      const token = crypto.randomBytes(32).toString("base64url");
      const tokenHash = hashResetToken(token);
      const expiresAt = new Date(Date.now() + PASSWORD_RESET_MAX_AGE_MS).toISOString();
      const resetUrl = `${publicBaseUrl(req)}/reset-password?token=${encodeURIComponent(token)}`;

      await db.query(
        `
          UPDATE password_reset_tokens
          SET used_at = now()
          WHERE user_id = $1
            AND used_at IS NULL
        `,
        [row.id]
      );

      await db.query(
        `
          INSERT INTO password_reset_tokens (
            id,
            user_id,
            token_hash,
            expires_at
          )
          VALUES ($1, $2, $3, $4)
        `,
        [crypto.randomUUID(), row.id, tokenHash, expiresAt]
      );

      sendPasswordResetEmail(row.email, resetUrl).catch((error) => {
        app.log.error({ error, email: row.email }, "Failed to send password reset email");
      });
    }
  } catch (error) {
    app.log.error({ error, email }, "Failed to create password reset token");
  }

  return genericResponse;
});

app.post("/api/v1/password-reset/confirm", async (req, reply) => {
  if (!db) {
    reply.code(503);
    return {
      ok: false,
      error: "Database login is not configured"
    };
  }

  const token = String(req.body?.token || "");
  const newPassword = String(req.body?.new_password || "");
  const newPasswordConfirm = String(req.body?.new_password_confirm || "");

  if (!token) {
    reply.code(400);
    return {
      ok: false,
      error: "Reset token is required"
    };
  }

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

  const tokenHash = hashResetToken(token);
  const client = await db.connect();

  try {
    await client.query("BEGIN");

    const result = await client.query(
      `
        SELECT
          t.id AS token_id,
          u.id AS user_id
        FROM password_reset_tokens t
        JOIN app_users u ON u.id = t.user_id
        WHERE t.token_hash = $1
          AND t.used_at IS NULL
          AND t.expires_at > now()
          AND u.active = true
        LIMIT 1
      `,
      [tokenHash]
    );

    const row = result.rows[0];

    if (!row) {
      await client.query("ROLLBACK");
      reply.code(400);
      return {
        ok: false,
        error: "Reset link is invalid or expired"
      };
    }

    await client.query(
      `
        UPDATE app_users
        SET password_hash = $2,
            must_change_password = false,
            updated_at = now()
        WHERE id = $1
      `,
      [row.user_id, hashPassword(newPassword)]
    );

    await client.query(
      `
        UPDATE password_reset_tokens
        SET used_at = now()
        WHERE user_id = $1
          AND used_at IS NULL
      `,
      [row.user_id]
    );

    await client.query("COMMIT");

    return {
      ok: true
    };
  } catch (error) {
    await client.query("ROLLBACK");
    app.log.error({ error }, "Failed to confirm password reset");
    reply.code(500);
    return {
      ok: false,
      error: "Failed to reset password"
    };
  } finally {
    client.release();
  }
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

app.get("/api/v1/devices/:deviceId", { preHandler: checkAuth }, async (req, reply) => {
  const deviceId = String(req.params.deviceId || "").trim();

  if (!isValidDeviceId(deviceId)) {
    reply.code(400);
    return {
      ok: false,
      error: "Invalid device ID"
    };
  }

  if (!deviceKnown(deviceId)) {
    reply.code(404);
    return {
      ok: false,
      error: "Device not found"
    };
  }

  if (!canAccessDevice(req.user, deviceId)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  return {
    ok: true,
    device: getDeviceView(getDevice(deviceId))
  };
});

app.get("/api/v1/devices/:deviceId/telemetry", { preHandler: checkAuth }, async (req, reply) => {
  const deviceId = String(req.params.deviceId || "").trim();
  const range = String(req.query?.range || "24h");

  if (!isValidDeviceId(deviceId)) {
    reply.code(400);
    return {
      ok: false,
      error: "Invalid device ID"
    };
  }

  if (!deviceKnown(deviceId)) {
    reply.code(404);
    return {
      ok: false,
      error: "Device not found"
    };
  }

  if (!canAccessDevice(req.user, deviceId)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  return {
    ok: true,
    device_id: deviceId,
    range: ["24h", "7d", "30d"].includes(range) ? range : "24h",
    telemetry_log_interval_ms: TELEMETRY_LOG_INTERVAL_MS,
    rows: await getTelemetryHistory(deviceId, range)
  };
});

app.get("/api/v1/devices/:deviceId/alarms", { preHandler: checkAuth }, async (req, reply) => {
  const deviceId = String(req.params.deviceId || "").trim();

  if (!isValidDeviceId(deviceId)) {
    reply.code(400);
    return {
      ok: false,
      error: "Invalid device ID"
    };
  }

  if (!deviceKnown(deviceId)) {
    reply.code(404);
    return {
      ok: false,
      error: "Device not found"
    };
  }

  if (!canAccessDevice(req.user, deviceId)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  const alarms = await refreshRecentAlarms();

  return {
    ok: true,
    device_id: deviceId,
    alarms: filterAlarmsForUser(req.user, alarms).filter((alarm) => alarm.device_id === deviceId)
  };
});

app.get("/api/v1/admin/customers", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
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

app.post("/api/v1/admin/customers", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  try {
    const customer = await createCustomer(req.body || {});

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

app.patch("/api/v1/admin/customers/:customerId", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  const customerId = String(req.params.customerId || "").trim();

  try {
    const customer = await saveCustomerDetails(customerId, req.body || {});

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

app.delete("/api/v1/admin/customers/:customerId", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  const customerId = String(req.params.customerId || "").trim();

  try {
    await deleteCustomer(customerId);

    return {
      ok: true
    };
  } catch (error) {
    reply.code(error.statusCode || 500);
    return {
      ok: false,
      error: error.message
    };
  }
});

app.patch("/api/v1/admin/customers/:customerId/subscription", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
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

app.get("/api/v1/admin/users", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  return {
    ok: true,
    users: await listAdminUsers()
  };
});

app.post("/api/v1/admin/users", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  try {
    return {
      ok: true,
      ...(await createAdminUser(req.body || {}))
    };
  } catch (error) {
    reply.code(error.statusCode || 500);
    return {
      ok: false,
      error: error.message
    };
  }
});

app.patch("/api/v1/admin/users/:userId", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  try {
    return {
      ok: true,
      ...(await updateAdminUser(String(req.params.userId || "").trim(), req.body || {}, req.user))
    };
  } catch (error) {
    reply.code(error.statusCode || 500);
    return {
      ok: false,
      error: error.message
    };
  }
});

app.post("/api/v1/admin/users/:userId/welcome", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  try {
    return {
      ok: true,
      sent: true,
      ...(await sendWelcomeToAdminUser(
        String(req.params.userId || "").trim(),
        `${publicBaseUrl(req)}/dashboard`
      ))
    };
  } catch (error) {
    reply.code(error.statusCode || 500);
    return {
      ok: false,
      error: error.message
    };
  }
});

app.get("/api/v1/admin/devices", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  return {
    ok: true,
    devices: await listAdminDevices()
  };
});

app.post("/api/v1/admin/devices", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  try {
    const device = await assignDeviceToCustomer(
      String(req.body?.device_id || "").trim(),
      String(req.body?.customer_id || "").trim()
    );

    return {
      ok: true,
      device
    };
  } catch (error) {
    reply.code(error.statusCode || 500);
    return {
      ok: false,
      error: error.message
    };
  }
});

app.patch("/api/v1/admin/devices/:deviceId", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  try {
    const device = await assignDeviceToCustomer(
      String(req.params.deviceId || "").trim(),
      String(req.body?.customer_id || "").trim()
    );

    return {
      ok: true,
      device
    };
  } catch (error) {
    reply.code(error.statusCode || 500);
    return {
      ok: false,
      error: error.message
    };
  }
});

app.delete("/api/v1/admin/devices/:deviceId", { preHandler: checkAuth }, async (req, reply) => {
  if (!isSuperUser(req.user)) {
    reply.code(403);
    return {
      ok: false,
      error: "Forbidden"
    };
  }

  try {
    const device = await removeDeviceFromRegistry(String(req.params.deviceId || "").trim());

    return {
      ok: true,
      device
    };
  } catch (error) {
    reply.code(error.statusCode || 500);
    return {
      ok: false,
      error: error.message
    };
  }
});

app.get("/api/v1/settings/notification-emails", { preHandler: checkAuth }, async (req, reply) => {
  try {
    const customerId = notificationSettingsCustomerId(req.user, req.query?.customer_id);
    await assertCustomerExists(customerId);

    return {
      ok: true,
      customer_id: customerId,
      emails: await getNotificationEmails(customerId)
    };
  } catch (error) {
    reply.code(error.statusCode || 500);
    return {
      ok: false,
      error: error.message
    };
  }
});

app.put("/api/v1/settings/notification-emails", { preHandler: checkAuth }, async (req, reply) => {
  try {
    const customerId = notificationSettingsCustomerId(req.user, req.body?.customer_id);
    const emails = parseNotificationEmails(req.body?.emails);
    const savedEmails = await saveNotificationEmails(customerId, emails);

    return {
      ok: true,
      customer_id: customerId,
      emails: savedEmails
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

  if (isSuperUser(req.user)) {
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

  if (isSuperUser(req.user)) {
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
  if (!isSuperUser(req.user)) {
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
