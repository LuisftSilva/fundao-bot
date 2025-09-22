// Telegram → ResIOT bridge.

const FALLBACK_TIMEOUT_SEC = 5 * 60;
const TELEGRAM_MAX = 4096;
const ADMIN_ID = "992579547";
const GH_API = "https://api.github.com";
const ALLOWLIST_FILE = "allowlist_fundao_bot.json";

let NAME_MAP_CACHE; // lazy JSON parse per isolate

// ---- Uptime storage (Gist) ----
// Snapshot "agora":
const SNAPSHOT_FILE = "gateways_uptime_last.json"; // { gwEUI: 0|1 }
// Transitions NDJSON por mês:
const MONTH_PREFIX = "gateways_uptime_transitions_"; // + YYYY-MM + ".ndjson"
// Carry mensal com estado às 00:00Z do mês:
const CARRY_PREFIX = "gateways_uptime_carry_"; // + YYYY-MM + ".json"
// Limite de conveniência para rollover (não usado no NDJSON, mas mantemos para futuro)
const MONTH_CAP_BYTES = 5_000_000;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // ---- DEBUG: validar acesso ao Gist com as env atuais ----
    if (url.pathname === "/debug/gist") {
      try {
        const id = env.GITHUB_GIST_ID || "(missing)";
        const hasTok = !!env.GITHUB_TOKEN;
        const r = await fetch(`https://api.github.com/gists/${id}`, {
          headers: {
            authorization: `Bearer ${env.GITHUB_TOKEN || ""}`,
            accept: "application/vnd.github+json",
            "user-agent": "fundao-bot/1.0",
            "x-github-api-version": "2022-11-28",
          },
        });
        const text = await r.text();
        const preview = text.slice(0, 300);
        return new Response(
          JSON.stringify(
            { ok: r.ok, status: r.status, id_seen: id, has_token: hasTok, preview },
            null,
            2
          ),
          { status: 200, headers: { "content-type": "application/json" } }
        );
      } catch (e) {
        return new Response(
          JSON.stringify({ ok: false, error: String(e) }, null, 2),
          { status: 200, headers: { "content-type": "application/json" } }
        );
      }
    }

    try {
      // health & path guards
      if (url.pathname === "/health") return new Response("ok");
      if (!url.pathname.startsWith("/webhook")) {
        return new Response("not found", { status: 404 });
      }

      // optional secret in path: /webhook/<secret>
      if (env.TELEGRAM_WEBHOOK_SECRET) {
        const seg = url.pathname.split("/").filter(Boolean)[1];
        if (seg !== env.TELEGRAM_WEBHOOK_SECRET) {
          return new Response("forbidden", { status: 403 });
        }
      }

      if (request.method !== "POST") {
        return new Response("method not allowed", { status: 405 });
      }

      if (!NAME_MAP_CACHE) NAME_MAP_CACHE = parseJson(env.GATEWAY_NAMES_JSON) || {};

      const update = await safeJson(request) || {};
      const msg = update.message ?? update.callback_query?.message;
      const chatId = msg?.chat?.id;
      const textIn = (update.message?.text || "").trim();

      // admin callbacks (approve/deny)
      if (update.callback_query?.data) {
        ctx?.waitUntil?.(handleAdminCallback(update, env));
        return new Response("OK");
      }

      // auth + commands (always 200 to Telegram)
      const work = (async () => {
        if (!chatId) return;
        const allowed = await ensureAuthorized(update, env);
        if (!allowed) return;

        const blocks = await handleCommand(textIn, env, NAME_MAP_CACHE);
        if (!blocks.length) return;
        await pAll(blocks.map((b) => () => sendText(env, chatId, b)), 5);
      })();

      ctx?.waitUntil?.(work);
      return new Response("OK");
    } catch (e) {
      console.error("unhandled:", e);
      return new Response("OK"); // never 500 to Telegram
    }
  },

  // Cron (*/5 * * * *): só grava TRANSIÇÕES (NDJSON) e mantém snapshot + carry mensal.
  async scheduled(event, env, ctx) {
    try {
      if (!NAME_MAP_CACHE) NAME_MAP_CACHE = parseJson(env.GATEWAY_NAMES_JSON) || {};
      const now = new Date();

      // 1) estados atuais
      const probe = await currentStates(env, NAME_MAP_CACHE, now);
      if (!probe.ok) { console.error("[cron] probe fail:", probe.err); return; }

      // 2) carregar último snapshot
      const prev = await gistReadJsonSafe(env, SNAPSHOT_FILE);

      // 3) detectar transições
      const ts = now.toISOString();
      const additions = [];
      const next = {};
      for (const it of probe.items) {
        next[it.gwEUI] = it.s; // 0|1
        const prevNum = to01(prev[it.gwEUI]);
        if (prevNum !== it.s) {
          // Evento mínimo para NDJSON: t, gw, s (1 ok, 0 nok)
          additions.push(ndjsonLine(ts, it.gwEUI, it.s));
        }
      }

      // 4) garantir carry do mês (se não existir)
      const mfile = monthNdjsonFile(now); // e.g. gateways_uptime_transitions_2025-09.ndjson
      const cfile = carryFile(now);       // e.g. gateways_uptime_carry_2025-09.json
      await ensureMonthlyCarry(env, now, prev, next, cfile);

      // 5) append de transições (NDJSON) e guardar snapshot
      if (additions.length) {
        await gistAppendText(env, mfile, additions.join(""));
      }
      await gistWriteJson(env, SNAPSHOT_FILE, next);

      console.log("[cron] ok. add:", additions.length, "file:", mfile);
    } catch (e) {
      console.error("scheduled error:", e);
    }
  },
};

// ---------- Allowlist via GitHub Gist ----------

function ghHeaders(env) {
  return {
    authorization: `Bearer ${env.GITHUB_TOKEN}`,
    accept: "application/vnd.github+json",
    "user-agent": "fundao-bot/1.0",
    "x-github-api-version": "2022-11-28",
  };
}

async function getAllowlist(env) {
  const r = await fetch(`${GH_API}/gists/${env.GITHUB_GIST_ID}`, { headers: ghHeaders(env) });
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    console.error("[getAllowlist] err", r.status, t);
    throw new Error(`gist get ${r.status}`);
  }
  const j = await r.json();
  const file = j.files?.[ALLOWLIST_FILE];
  const content = file?.content || "[]";
  const arr = parseJson(content) || [];
  const base = Array.isArray(arr) ? arr.map(String) : [];
  return base.includes(ADMIN_ID) ? base : [ADMIN_ID, ...base];
}

async function saveAllowlist(env, list) {
  const body = {
    files: {
      [ALLOWLIST_FILE]: {
        content: JSON.stringify(Array.from(new Set(list.map(String))), null, 2),
      },
    },
  };
  const r = await fetch(`${GH_API}/gists/${env.GITHUB_GIST_ID}`, {
    method: "PATCH",
    headers: { ...ghHeaders(env), "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    console.error("[saveAllowlist] err", r.status, t);
    throw new Error(`gist save ${r.status}`);
  }
}

async function isChatAllowed(env, chatId) {
  const list = await getAllowlist(env);
  return list.includes(String(chatId));
}

async function ensureAuthorized(update, env) {
  const msg = update.message ?? update.callback_query?.message;
  const chatId = msg?.chat?.id;
  if (!chatId) return false;

  // admin always allowed
  if (String(chatId) === ADMIN_ID) return true;

  // allowed via gist?
  try {
    if (await isChatAllowed(env, chatId)) return true;
  } catch (e) {
    console.error("[ensureAuthorized] isChatAllowed error:", e);
    return false;
  }

  // only trigger request on /start
  const textIn = (update.message?.text || "").trim();
  if (textIn !== "/start") return false;

  // notify user
  await sendText(env, chatId, "<i>Request sent to admin. Please wait for approval.</i>");

  // ask admin with inline buttons
  const u = update.message?.from || {};
  const ch = update.message?.chat || {};
  await tgApi(env, "sendMessage", {
    chat_id: ADMIN_ID,
    text: [
      "<b>New access request</b>",
      `chat_id: <code>${escapeHtml(String(ch.id))}</code>`,
      `type: ${escapeHtml(ch.type || "-")}`,
      `title: ${escapeHtml(ch.title || "")}`,
      `username: @${u.username || (ch.username || "-")}`,
      `name: ${escapeHtml(([u.first_name, u.last_name].filter(Boolean).join(" ")) || "-")}`,
      `lang: ${u.language_code || "-"}`
    ].join("\n"),
    reply_markup: {
      inline_keyboard: [[
        { text: "✅ Approve", callback_data: `approve:${ch.id}` },
        { text: "❌ Deny", callback_data: `deny:${ch.id}` }
      ]]
    },
    parse_mode: "HTML",
    disable_web_page_preview: true
  }).catch(e => console.error("notify admin err:", e));

  return false;
}

async function handleAdminCallback(update, env) {
  const cq = update.callback_query;
  const fromId = String(cq?.from?.id || "");
  const data = String(cq?.data || "");
  const messageChatId = cq?.message?.chat?.id;

  await tgApi(env, "answerCallbackQuery", { callback_query_id: cq.id }).catch(() => {});
  if (fromId !== ADMIN_ID) return;

  const m = data.match(/^(approve|deny):(-?\d+)$/);
  if (!m) return;
  const [, action, targetChatId] = m;

  if (action === "approve") {
    const list = await getAllowlist(env);
    if (!list.includes(targetChatId)) {
      list.push(targetChatId);
      await saveAllowlist(env, list);
    }
    await sendText(env, targetChatId, "✅ Authorized. You can use /status.");
    if (messageChatId) await sendText(env, messageChatId, `✅ Added ${targetChatId}`);
  } else {
    await sendText(env, targetChatId, "❌ Not authorized.");
    if (messageChatId) await sendText(env, messageChatId, `❌ Denied ${targetChatId}`);
  }
}

// ---------- Commands ----------

async function handleCommand(textIn, env, NAME_MAP) {
  if (textIn === "/start" || textIn === "/help") {
    return [[
      "<b>Commands</b>",
      "• <code>/status</code> — all gateways",
      "• <code>/status_ok</code> — only ✅",
      "• <code>/status_nok</code> — only ❌",
      "• <code>/ping</code> — test"
    ].join("\n")];
  }
  if (textIn === "/ping") return ["<b>pong</b>"];

  if (textIn?.startsWith("/status")) {
    const filter = textIn === "/status_ok" ? "OK" : textIn === "/status_nok" ? "NOK" : null;
    const { ok, rows, err } = await fetchGatewaysDirect(env, NAME_MAP);
    if (!ok) return [`<i>Failed /api/gateways</i>: ${escapeHtml(err || "error")}`];
    const html = formatRowsAsTable(rows, filter);
    return splitIntoChunks(html, TELEGRAM_MAX - 600);
  }

  return ["<i>Say</i> <code>/status</code> <i>to view the table</i>."];
}

// ---------- ResIOT ----------

// For table (/status)
async function fetchGatewaysDirect(env, NAME_MAP) {
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort("timeout"), 15_000);

    const resp = await fetch(`${env.RESIOT_BASE}/api/gateways?limit=1000`, {
      headers: {
        accept: "application/json",
        "Grpc-Metadata-Authorization": env.RESIOT_TOKEN,
        "user-agent": "fundao-bot/1.0",
      },
      signal: ctrl.signal,
    }).finally(() => clearTimeout(timer));

    if (!resp?.ok) {
      const txt = await resp.text().catch(() => String(resp?.status ?? ""));
      return { ok: false, err: `${resp?.status ?? ""} ${txt}`.trim() };
    }

    const data = await resp.json().catch(() => ({}));
    const arr = Array.isArray(data?.result) ? data.result : [];
    const rows = [];

    for (const g of arr) {
      const code = g?.name ?? "—";
      const fullName = NAME_MAP[code] || code;
      const timeoutSec = Number(g?.timeout) > 0 ? Number(g.timeout) : FALLBACK_TIMEOUT_SEC;

      // Prefer LastUplink, then Radio, then Monitor
      const lastRaw = g?.LastUplink || g?.LastAliveRadio || g?.LastAliveMonitor || "";
      const lastDate = parseResiotTime(lastRaw);
      const whenStr = normalizeToLisbon(lastRaw, lastDate);

      let emoji = "❌";
      if (lastDate instanceof Date) {
        const ageMs = Date.now() - lastDate.getTime();
        emoji = ageMs <= timeoutSec * 1000 ? "✅" : "❌";
      }

      rows.push({ name: fullName, when: whenStr, emoji });
    }

    rows.sort((a, b) => a.name.localeCompare(b.name, "pt"));
    return { ok: true, rows };
  } catch (e) {
    console.error("resiot error:", e);
    return { ok: false, err: String(e) };
  }
}

// For cron (transition recording) — same logic + gwEUI + /lastmessage fallback
async function currentStates(env, NAME_MAP, now) {
  try {
    const r = await fetch(`${env.RESIOT_BASE}/api/gateways?limit=1000`, {
      headers: { accept: "application/json", "Grpc-Metadata-Authorization": env.RESIOT_TOKEN },
    });
    if (!r.ok) return { ok: false, err: `${r.status} ${await r.text().catch(() => r.status)}` };
    const j = await r.json().catch(() => ({}));
    const arr = Array.isArray(j?.result) ? j.result : [];
    const out = [];

    for (const g of arr) {
      const code = g?.name ?? "-";
      const gwEUI = g?.gwEUI || g?.GatewayEUI || g?.eui || code;
      const fullName = NAME_MAP[code] || code;
      const timeoutSec = Number(g?.timeout) > 0 ? Number(g.timeout) : FALLBACK_TIMEOUT_SEC;

      let raw = g?.LastUplink || g?.LastAliveRadio || g?.LastAliveMonitor || null;
      let last = raw ? parseResiotTime(raw) : null;

      // optional fallback: /lastmessage
      if (!last && gwEUI && gwEUI !== code) {
        try {
          const r2 = await fetch(
            `${env.RESIOT_BASE}/api/gateways/${encodeURIComponent(gwEUI)}/lastmessage`,
            { headers: { accept: "application/json", "Grpc-Metadata-Authorization": env.RESIOT_TOKEN } }
          );
          if (r2.ok) {
            const j2 = await r2.json().catch(() => ({}));
            const maybe = j2?.date || j2?.lastSeen || j2?.LastAlive || j2?.LastUplink || null;
            if (maybe) last = parseResiotTime(String(maybe));
          }
        } catch {}
      }

      const lastIso = last ? last.toISOString() : "";
      const s = last && (now - last) <= timeoutSec * 1000 ? 1 : 0;
      out.push({ gwEUI, name: fullName, s, lastIso });
    }
    return { ok: true, items: out };
  } catch (e) {
    return { ok: false, err: String(e) };
  }
}

// ---------- Dates / formatting ----------

function parseResiotTime(s) {
  if (!s || typeof s !== "string") return null;
  if (s.startsWith("0001-01-01")) return null; // placeholder
  const cleaned = s.split(" m=")[0].trim();
  if (/\+0000\sUTC$/.test(cleaned)) {
    const iso = cleaned.replace(/\s\+0000\sUTC$/, "Z");
    const d = new Date(iso);
    return isNaN(d) ? null : d;
  }
  const d = new Date(cleaned + "Z");
  return isNaN(d) ? null : d;
}

function normalizeToLisbon(raw, parsedDate) {
  if (!parsedDate) {
    const m = (raw || "").match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})/);
    if (m) return `${m[3]}-${m[2]}-${m[1]} ${m[4]}:${m[5]}`;
    return "—";
  }
  return formatLisbonDateTime(parsedDate);
}

function formatLisbonDateTime(dt) {
  const parts = new Intl.DateTimeFormat("pt-PT", {
    timeZone: "Europe/Lisbon",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(dt);
  const get = (t) => parts.find((p) => p.type === t)?.value || "";
  return `${get("day")}-${get("month")}-${get("year")} ${get("hour")}:${get("minute")}`;
}

// ---------- Table ----------

function formatRowsAsTable(rows, filter /* "OK" | "NOK" | null */) {
  const filtered = filter ? rows.filter((r) => (filter === "OK" ? r.emoji === "✅" : r.emoji === "❌")) : rows;
  if (!filtered.length) return "<i>(sem dados)</i>";

  const HN = "Nome", HW = "Quando", HS = "Ok";
  const nameW = Math.max(HN.length, ...filtered.map((r) => (r.name || "").length));
  const whenW = Math.max(HW.length, ...filtered.map((r) => (r.when || "").length));
  const stateW = HS.length;

  const header = `${padRight(HN, nameW)}|${padRight(HW, whenW)}|${HS}`;
  const sep = `${"-".repeat(nameW)}+${"-".repeat(whenW)}+${"-".repeat(stateW)}`;
  const body = filtered
    .map((r) => `${padRight(r.name || "", nameW)}|${padRight(r.when || "", whenW)}|${r.emoji || ""}`)
    .join("\n");

  return `<pre>${escapeHtml(`${header}\n${sep}\n${body}`)}</pre>`;
}

// ---------- Gist helpers (NDJSON + carry + snapshot) ----------

function monthKey(d = new Date()) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}
function monthNdjsonFile(d = new Date()) {
  return `${MONTH_PREFIX}${monthKey(d)}.ndjson`;
}
function carryFile(d = new Date()) {
  return `${CARRY_PREFIX}${monthKey(d)}.json`;
}

// Linha NDJSON minimalista (termina com \n)
function ndjsonLine(isoTs, gw, s /* 0|1 */) {
  // chaves curtas t,gw,s
  return JSON.stringify({ t: isoTs, gw, s }) + "\n";
}

// Lê metadata do Gist
async function gistGet(env) {
  const url = `${GH_API}/gists/${env.GITHUB_GIST_ID}`;
  const r = await fetch(url, { headers: ghHeaders(env) });
  if (!r.ok) {
    const body = await r.text().catch(() => "");
    console.error("[gistGet] err", r.status, body);
    throw new Error(`gist get ${r.status}`);
  }
  return r.json();
}

// Lê conteúdo completo de um ficheiro (mesmo se truncado)
async function gistReadFileText(env, filename) {
  const meta = await gistGet(env);
  const f = meta.files?.[filename];
  if (!f) return null;
  if (f.truncated && f.raw_url) {
    const rr = await fetch(f.raw_url); // raw_url já dá o conteúdo inteiro
    if (!rr.ok) throw new Error(`raw fetch ${rr.status}`);
    return await rr.text();
  }
  return typeof f.content === "string" ? f.content : null;
}

// JSON.safe
async function gistReadJsonSafe(env, filename) {
  try {
    const text = await gistReadFileText(env, filename);
    if (!text) return {};
    return JSON.parse(text);
  } catch (e) {
    console.error("[gistReadJsonSafe]", filename, String(e));
    return {};
  }
}

// Escreve JSON (substitui)
async function gistWriteJson(env, filename, obj) {
  const url = `${GH_API}/gists/${env.GITHUB_GIST_ID}`;
  const body = JSON.stringify({ files: { [filename]: { content: JSON.stringify(obj) } } });
  const r = await fetch(url, {
    method: "PATCH",
    headers: { ...ghHeaders(env), "content-type": "application/json" },
    body,
  });
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    console.error("[gistWriteJson] err", r.status, t);
    throw new Error(`gist save ${r.status}`);
  }
}

// Append de texto (NDJSON). Faz merge do conteúdo antigo + novo e PATCH.
async function gistAppendText(env, filename, toAppend /* string */) {
  // Lê o que houver — se não existir, começa vazio
  let previous = "";
  try {
    previous = (await gistReadFileText(env, filename)) || "";
  } catch { previous = ""; }

  const next = previous + toAppend;

  const url = `${GH_API}/gists/${env.GITHUB_GIST_ID}`;
  const r = await fetch(url, {
    method: "PATCH",
    headers: { ...ghHeaders(env), "content-type": "application/json" },
    body: JSON.stringify({ files: { [filename]: { content: next } } }),
  });
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    console.error("[gistAppendText] err", r.status, t);
    throw new Error(`gist append ${r.status}`);
  }
}

// Garante carry do mês atual: se não existir, cria.
// Estratégia:
//  - Se o ficheiro do mês não existe, cria com t0=YYYY-MM-01T00:00:00Z e state baseado em `prev` (snapshot anterior).
//  - Se `prev` estiver vazio (primeiro arranque), usa `next` (snapshot atual) como fallback.
async function ensureMonthlyCarry(env, now, prevSnapshot, nextSnapshot, carryFilename) {
  // já existe?
  const meta = await gistGet(env);
  if (meta.files?.[carryFilename]) return; // ok

  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const t0 = `${y}-${m}-01T00:00:00Z`;

  const base = Object.keys(prevSnapshot || {}).length ? prevSnapshot : nextSnapshot || {};
  const state = {};
  for (const [gw, st] of Object.entries(base)) state[gw] = to01(st);

  const payload = { t0, state };
  await gistWriteJson(env, carryFilename, payload);
}

// Legacy-safe normalizer: "OK"/"NOK", booleans, "1"/"0" -> 1/0
function to01(v) {
  if (v === 1 || v === "1" || v === true || v === "OK") return 1;
  return 0;
}
// ---------- Telegram helpers ----------

async function sendText(env, chatId, html) {
  return tgApi(env, "sendMessage", {
    chat_id: chatId,
    text: html,
    parse_mode: "HTML",
    disable_web_page_preview: true,
  });
}

async function tgApi(env, method, payload) {
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${method}`;
  const r = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!r.ok) {
    const t = await r.text().catch(() => String(r.status));
    console.error("[tgApi] err", r.status, t);
    throw new Error(`${r.status} ${t}`);
  }
  return r.json().catch(() => ({}));
}

// ---------- Utils ----------

async function safeJson(req) { try { return await req.json(); } catch { return null; } }
function escapeHtml(s) { return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }
function padRight(s, w) { s = String(s); const d = w - s.length; return d > 0 ? s + " ".repeat(d) : s; }
function splitIntoChunks(str, max) { const out = []; for (let i = 0; i < str.length; i += max) out.push(str.slice(i, i + max)); return out.length ? out : [""]; }
function parseJson(s) { try { return JSON.parse(s); } catch { return null; } }
async function pAll(tasks, concurrency = 4) {
  if (!tasks.length) return;
  const q = tasks.slice();
  const running = new Set();
  const run = async (t) => {
    const p = t().finally(() => running.delete(p));
    running.add(p);
    await p;
  };
  while (q.length) {
    while (running.size < concurrency && q.length) run(q.shift());
    if (running.size) await Promise.race(running);
  }
  await Promise.all(running);
}