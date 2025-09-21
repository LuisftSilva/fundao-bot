// Minimal Telegram → ResIOT bridge. Short comments only.

const FALLBACK_TIMEOUT_SEC = 5 * 60;
const TELEGRAM_MAX = 4096;
let NAME_MAP_CACHE; // lazy JSON parse, per isolate

export default {
  async fetch(request, env) {
    // Health
    const url = new URL(request.url);
    if (url.pathname === "/health") return new Response("ok");

    // Optional webhook secret: set TELEGRAM_WEBHOOK_SECRET and use /webhook/<secret>
    if (!url.pathname.startsWith("/webhook")) return new Response("not found", { status: 404 });
    if (env.TELEGRAM_WEBHOOK_SECRET) {
      const seg = url.pathname.split("/").filter(Boolean)[1];
      if (seg !== env.TELEGRAM_WEBHOOK_SECRET) return new Response("forbidden", { status: 403 });
    }
    if (request.method !== "POST") return new Response("method not allowed", { status: 405 });

    // Lazy-load gateway name map from secret/var
    if (!NAME_MAP_CACHE) NAME_MAP_CACHE = parseJson(env.GATEWAY_NAMES_JSON) || {};

    const update = await safeJson(request);
    const msg = update?.message ?? update?.callback_query?.message;
    const chatId = msg?.chat?.id;
    const textIn = (update?.message?.text || "").trim();

    const blocks = await handleCommand(textIn, env, NAME_MAP_CACHE);
    if (chatId && blocks.length) {
      // Send in parallel (bounded)
      await pAll(
        blocks.map(b => () =>
          fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              chat_id: chatId,
              text: b,
              parse_mode: "HTML",
              disable_web_page_preview: true
            })
          })
        ),
        5 // simple concurrency limit
      );
    }
    return new Response("OK");
  }
};

async function handleCommand(text, env, NAME_MAP) {
  if (text === "/start" || text === "/help") {
    return [joinLines(
      "<b>Commands</b>",
      "• <code>/status</code> — all gateways",
      "• <code>/status_ok</code> — only ✅",
      "• <code>/status_nok</code> — only ❌",
      "• <code>/ping</code> — test"
    )];
  }
  if (text === "/ping") return ["<b>pong</b>"];

  if (text?.startsWith("/status")) {
    const filter = text === "/status_ok" ? "OK" : text === "/status_nok" ? "NOK" : null;
    const { ok, rows, err } = await fetchGateways(env, NAME_MAP);
    if (!ok) return [`<i>Failed /api/gateways</i>: ${escapeHtml(err || "error")}`];
    const html = formatRowsAsTable(rows, filter);
    return splitIntoChunks(html, TELEGRAM_MAX - 600); // headroom for HTML
  }

  return ["<i>Say</i> <code>/status</code> <i>to view the table</i>."];
}

// ---- ResIOT ----

async function fetchGateways(env, NAME_MAP) {
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort("timeout"), 15_000); // hard timeout

    const resp = await fetch(`${env.RESIOT_BASE}/api/gateways?limit=1000`, {
      headers: {
        "accept": "application/json",
        "Grpc-Metadata-Authorization": env.RESIOT_TOKEN, // per API
        "user-agent": "fundao-bot/1.0"
      },
      signal: ctrl.signal
    }).catch(e => { throw e; })
     .finally(() => clearTimeout(timer));

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

      const lastRaw = g?.LastUplink || g?.LastAliveRadio || g?.LastAliveMonitor || "";
      const lastDate = parseResiotTime(lastRaw);
      const whenStr  = normalizeToLisbon(lastRaw, lastDate);

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
    console.error("resiot error:", e); // tail logs for diagnostics
    return { ok: false, err: String(e) };
  }
}

// ---- Date / formatting ----

function parseResiotTime(s) {
  if (!s || typeof s !== "string") return null;
  if (s.startsWith("0001-01-01")) return null;
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
  const parts = new Intl.DateTimeFormat("pt-PT", {
    timeZone: "Europe/Lisbon",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false
  }).formatToParts(parsedDate);
  const get = t => parts.find(p => p.type === t)?.value || "";
  return `${get("day")}-${get("month")}-${get("year")} ${get("hour")}:${get("minute")}`;
}

// ---- Output helpers ----

function formatRowsAsTable(rows, filter /* "OK" | "NOK" | null */) {
  const filtered = filter ? rows.filter(r => (filter === "OK" ? r.emoji === "✅" : r.emoji === "❌")) : rows;
  if (!filtered.length) return "<i>(no data)</i>";

  const HN = "Nome", HW = "Quando", HS = "Ok";
  const nameW = Math.max(HN.length, ...filtered.map(r => (r.name || "").length));
  const whenW = Math.max(HW.length, ...filtered.map(r => (r.when || "").length));
  const header = `${padRight(HN, nameW)}|${padRight(HW, whenW)}|${HS}`;
  const sep    = `${"-".repeat(nameW)}+${"-".repeat(whenW)}+${"-".repeat(HS.length)}`;
  const body   = filtered.map(r => `${padRight(r.name || "", nameW)}|${padRight(r.when || "", whenW)}|${r.emoji || ""}`).join("\n");
  return `<pre>${escapeHtml(`${header}\n${sep}\n${body}`)}</pre>`;
}

function splitIntoChunks(str, max) {
  const out = []; for (let i = 0; i < str.length; i += max) out.push(str.slice(i, i + max));
  return out.length ? out : [""];
}

// ---- Generic utils ----

async function safeJson(req) { try { return await req.json(); } catch { return null; } }
function parseJson(s) { try { return JSON.parse(s); } catch { return null; } }
function escapeHtml(s) { return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
function padRight(s, w) { s = String(s); const d = w - s.length; return d > 0 ? s + " ".repeat(d) : s; }

// tiny promise pool
async function pAll(tasks, concurrency = 4) {
  const q = tasks.slice(); const running = new Set();
  const run = async t => { const p = t().finally(() => running.delete(p)); running.add(p); await p; };
  while (q.length) { while (running.size < concurrency && q.length) run(q.shift()); await Promise.race(running); }
  await Promise.all(Array.from(running));
}