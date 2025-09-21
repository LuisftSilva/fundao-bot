// Telegram → ResIOT bridge. Short comments only.

const FALLBACK_TIMEOUT_SEC = 5 * 60;
const TELEGRAM_MAX = 4096;
const ADMIN_ID = "992579547";
const GH_API = "https://api.github.com";
const ALLOWLIST_FILE = "allowlist_fundao_bot.json";

let NAME_MAP_CACHE; // lazy JSON parse per isolate

export default {
	async fetch(request, env, ctx) {
		try {
			const url = new URL(request.url);
			if (url.pathname === "/health") return new Response("ok");
			if (!url.pathname.startsWith("/webhook")) return new Response("not found", { status: 404 });

			// Optional path secret: /webhook/<secret>
			if (env.TELEGRAM_WEBHOOK_SECRET) {
				const seg = url.pathname.split("/").filter(Boolean)[1];
				if (seg !== env.TELEGRAM_WEBHOOK_SECRET) return new Response("forbidden", { status: 403 });
			}
			if (request.method !== "POST") return new Response("method not allowed", { status: 405 });

			if (!NAME_MAP_CACHE) NAME_MAP_CACHE = parseJson(env.GATEWAY_NAMES_JSON) || {};

			const update = await safeJson(request) || {};
			const msg = update.message ?? update.callback_query?.message;
			const chatId = msg?.chat?.id;
			const textIn = (update.message?.text || "").trim();

			// Admin inline buttons callbacks
			if (update.callback_query?.data) {
				ctx?.waitUntil?.(handleAdminCallback(update, env));
				return new Response("OK");
			}

			// Do work in background; always 200 to Telegram
			const work = (async () => {
				if (!chatId) return;

				// auth gate (auto-request on /start)
				const allowed = await ensureAuthorized(update, env);
				if (!allowed) return;

				const blocks = await handleCommand(textIn, env, NAME_MAP_CACHE);
				if (!blocks.length) return;

				await pAll(blocks.map(b => () => sendText(env, chatId, b)), 5);
			})();

			ctx?.waitUntil?.(work);
			return new Response("OK");
		} catch (e) {
			console.error("unhandled:", e);
			return new Response("OK"); // never 500 to Telegram
		}
	}
};

// ---------- Allowlist via GitHub Gist ----------

async function getAllowlist(env) {
	const r = await fetch(`${GH_API}/gists/${env.GITHUB_GIST_ID}`, {
		headers: {
			"authorization": `Bearer ${env.GITHUB_TOKEN}`,
			"accept": "application/vnd.github+json",
			"user-agent": "fundao-bot/1.0"
		}
	});
	if (!r.ok) throw new Error(`gist get ${r.status}`);
	const j = await r.json();
	const file = j.files[ALLOWLIST_FILE];
	const content = file?.content || "[]";
	const arr = JSON.parse(content);
	return Array.isArray(arr) ? arr.map(String) : [ADMIN_ID];
}

async function saveAllowlist(env, list) {
	const body = {
		files: {
			[ALLOWLIST_FILE]: {
				content: JSON.stringify(Array.from(new Set(list.map(String))), null, 2)
			}
		}
	};
	const r = await fetch(`${GH_API}/gists/${env.GITHUB_GIST_ID}`, {
		method: "PATCH",
		headers: {
			"authorization": `Bearer ${env.GITHUB_TOKEN}`,
			"accept": "application/vnd.github+json",
			"content-type": "application/json",
			"user-agent": "fundao-bot/1.0"
		},
		body: JSON.stringify(body)
	});
	if (!r.ok) throw new Error(`gist save ${r.status}`);
}

async function isChatAllowed(env, chatId) {
	const list = await getAllowlist(env);
	return list.includes(String(chatId));
}

async function ensureAuthorized(update, env) {
	const msg = update.message ?? update.callback_query?.message;
	const chatId = msg?.chat?.id;
	const chatType = msg?.chat?.type;
	if (!chatId) return false;

	// admin always allowed
	if (String(chatId) === ADMIN_ID) return true;

	if (await isChatAllowed(env, chatId)) return true;

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

	// stop spinner
	await tgApi(env, "answerCallbackQuery", { callback_query_id: cq.id }).catch(() => { });
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

async function fetchGatewaysDirect(env, NAME_MAP) {
	try {
		const ctrl = new AbortController();
		const timer = setTimeout(() => ctrl.abort("timeout"), 15_000);

		const resp = await fetch(`${env.RESIOT_BASE}/api/gateways?limit=1000`, {
			headers: {
				"accept": "application/json",
				"Grpc-Metadata-Authorization": env.RESIOT_TOKEN,
				"user-agent": "fundao-bot/1.0"
			},
			signal: ctrl.signal
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

			// prefer LastUplink, then Radio, then Monitor
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

// ---------- Dates / formatting ----------

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
	return formatLisbonDateTime(parsedDate);
}

function formatLisbonDateTime(dt) {
	const parts = new Intl.DateTimeFormat("pt-PT", {
		timeZone: "Europe/Lisbon",
		year: "numeric", month: "2-digit", day: "2-digit",
		hour: "2-digit", minute: "2-digit", hour12: false
	}).formatToParts(dt);
	const get = t => parts.find(p => p.type === t)?.value || "";
	return `${get("day")}-${get("month")}-${get("year")} ${get("hour")}:${get("minute")}`;
}

// ---------- Table ----------

function formatRowsAsTable(rows, filter) {
	const filtered = filter ? rows.filter(r => (filter === "OK" ? r.emoji === "✅" : r.emoji === "❌")) : rows;
	if (!filtered.length) return "<i>(sem dados)</i>";
	const HN = "Nome", HW = "Quando", HS = "Ok";
	const nameW = Math.max(HN.length, ...filtered.map(r => (r.name || "").length));
	const whenW = Math.max(HW.length, ...filtered.map(r => (r.when || "").length));
	const stateW = HS.length;
	const header = `${padRight(HN, nameW)}|${padRight(HW, whenW)}|${HS}`;
	const sep = `${"-".repeat(nameW)}+${"-".repeat(whenW)}+${"-".repeat(stateW)}`;
	const body = filtered.map(r => `${padRight(r.name || "", nameW)}|${padRight(r.when || "", whenW)}|${r.emoji || ""}`).join("\n");
	return `<pre>${escapeHtml(`${header}\n${sep}\n${body}`)}</pre>`;
}

// ---------- Telegram helpers ----------

async function sendText(env, chatId, html) {
	return tgApi(env, "sendMessage", {
		chat_id: chatId, text: html, parse_mode: "HTML", disable_web_page_preview: true
	});
}

async function tgApi(env, method, payload) {
	const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${method}`;
	const r = await fetch(url, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify(payload)
	});
	if (!r.ok) { const t = await r.text().catch(() => String(r.status)); throw new Error(`${r.status} ${t}`); }
	return r.json().catch(() => ({}));
}

// ---------- Utils ----------

async function safeJson(req) { try { return await req.json(); } catch { return null; } }
function escapeHtml(s) { return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }
function padRight(s, w) { s = String(s); const d = w - s.length; return d > 0 ? s + " ".repeat(d) : s; }
function splitIntoChunks(str, max) { const out = []; for (let i = 0; i < str.length; i += max) out.push(str.slice(i, i + max)); return out.length ? out : [""]; }
function parseJson(s) { try { return JSON.parse(s); } catch { return null; } }
function displayName(msg) {
	const u = msg?.from || {}; const p = [u.first_name, u.last_name].filter(Boolean).join(" ");
	return p || u.username || String(msg?.chat?.title || msg?.chat?.id || "");
}
async function pAll(tasks, concurrency = 4) {
	if (!tasks.length) return;
	const q = tasks.slice(); const running = new Set();
	const run = async t => { const p = t().finally(() => running.delete(p)); running.add(p); await p; };
	while (q.length) { while (running.size < concurrency && q.length) run(q.shift()); if (running.size) await Promise.race(running); }
	await Promise.all(running);
}