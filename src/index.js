// Telegram → ResIOT bridge.

const FALLBACK_TIMEOUT_SEC = 5 * 60;
const TELEGRAM_MAX = 4096;
const ADMIN_ID = "992579547";
const GH_API = "https://api.github.com";
const ALLOWLIST_FILE = "allowlist_fundao_bot.json";

let NAME_MAP_CACHE; // lazy JSON parse per isolate
const MENU_KEYBOARD = {
	keyboard: [
		[
			{ text: "📊 Status" },
			{ text: "📈 Histórico" },
		],
	],
	resize_keyboard: true,
	one_time_keyboard: false,
};
const HISTORY_PROMPTS = new Map(); // chatId -> { stage: "await_idx" | "await_days", idx?: number }

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

				const blocks = await handleCommand(chatId, textIn, env, NAME_MAP_CACHE);
				const normalizedBlocks = (blocks || [])
					.map((b) => (typeof b === "string" ? { text: b } : b))
					.filter((b) => b && typeof b.text === "string");
				if (!normalizedBlocks.length) return;
				await pAll(normalizedBlocks.map((b) => () => sendText(env, chatId, b)), 5);
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

			// Guardar em hora de Lisboa (wall-clock), sem sufixo de timezone
			const ts = lisbonWallIso(now);
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

async function handleCommand(chatId, textIn, env, NAME_MAP) {
	const chatKey = chatId ? String(chatId) : "";
	let commandText = (textIn || "").trim();
	const historyState = chatKey ? HISTORY_PROMPTS.get(chatKey) : null;

	const shortcut = detectMenuShortcut(commandText);
	if (shortcut === "status") {
		if (chatKey) HISTORY_PROMPTS.delete(chatKey);
		commandText = "/status";
	} else if (shortcut === "history") {
		if (chatKey) HISTORY_PROMPTS.set(chatKey, { stage: "await_idx" });
		return [historyAskIdxMessage()];
	}

	if (historyState && !commandText.startsWith("/")) {
		if (historyState.stage === "await_idx") {
			return handleHistoryIdxInput(chatKey, commandText);
		}
		if (historyState.stage === "await_days") {
			return await handleHistoryDaysInput(chatKey, commandText, historyState.idx, env, NAME_MAP);
		}
	} else if (historyState && commandText.startsWith("/")) {
		HISTORY_PROMPTS.delete(chatKey);
	}

	if (commandText === "/start" || commandText === "/help") {
		HISTORY_PROMPTS.delete(chatKey);
		return [{ text: "\u200b", reply_markup: MENU_KEYBOARD }];
	}
	if (commandText === "/ping") return ["<b>pong</b>"];

	if (commandText?.startsWith("/status")) {
		const filter = commandText === "/status_ok" ? "OK" : commandText === "/status_nok" ? "NOK" : null;
		const { ok, rows, err } = await fetchGatewaysDirect(env, NAME_MAP);
		if (!ok) return [`<i>Failed /api/gateways</i>: ${escapeHtml(err || "error")}`];
		const html = formatRowsAsTable(rows, filter);
		return splitIntoChunks(html, TELEGRAM_MAX - 600);
	}

	// /fetch handler
	if (commandText?.startsWith("/fetch")) {
		const parts = commandText.split(/\s+/).filter(Boolean);
		const gwEUI = parts[1] || "";
		let days = Math.max(1, Math.min(90, Number(parts[2] || 1)));
		let stepMin = Number(parts[3] || (days <= 7 ? 5 : 60));
		if (!Number.isFinite(stepMin) || stepMin <= 0) stepMin = (days <= 7 ? 5 : 60);

		if (!gwEUI) {
			return ['Uso: <code>/fetch &lt;gwEUI&gt; [dias=1] [stepMin=(5|60)]</code>'];
		}

		const now = new Date();
		const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);

		console.log('[fetch] args', { gwEUI, days, stepMin, startIso: start.toISOString(), endIso: now.toISOString() });

		const rec = await fetchSeriesForGateway(env, gwEUI, start, now, stepMin);
		if (!rec.ok) {
			console.log('[fetch] error', rec.err || 'unknown');
			return ['<i>Falha a obter série — vê os logs.</i>'];
		}

		const { slots, pctUp, eventsCount, offMs } = rec;
		const preview = slots.slice(0, 120);
		console.log('[fetch] result', {
			gwEUI, days, stepMin,
			startIso: start.toISOString(),
			endIso: now.toISOString(),
			eventsCount,
			slotsLen: slots.length,
			pctUp,
			offMs,
			slotsPreview: preview,
		});
		// Dump all 5-min states (grouped per hour) to logs
		// Lookup name/code para esta gwEUI (para responder no Telegram)
		const idxList = await buildIndexList(env, NAME_MAP);
		let name = gwEUI, code = '-';
		if (idxList.ok) {
			const found = (idxList.items || []).find(it => it.gwEUI === gwEUI);
			if (found) { name = found.name || gwEUI; code = found.code || '-'; }
		}

		const msg = [
			`<b>${escapeHtml(name)}</b>`,
			`EUI: <code>${escapeHtml(gwEUI)}</code>`,
			`Código: <code>${escapeHtml(code)}</code>`
		].join('\n');
		return [msg];
	}

	// /history <#idx> [dias]
	if (commandText?.startsWith("/history")) {
		const parts = commandText.split(/\s+/).filter(Boolean);
		const idx = Number(parts[1]);
		let days = Number(parts[2]);
		if (!Number.isFinite(idx) || idx <= 0) {
			if (chatKey) HISTORY_PROMPTS.set(chatKey, { stage: "await_idx" });
			return [historyAskIdxMessage()];
		}
		if (!Number.isFinite(days) || days <= 0) {
			if (chatKey) HISTORY_PROMPTS.set(chatKey, { stage: "await_days", idx });
			return [historyAskDaysMessage(idx)];
		}
		HISTORY_PROMPTS.delete(chatKey);
		days = clampDays(days);
		return fulfillHistoryRequest(idx, days, env, NAME_MAP);
	}

	return ["<i>Say</i> <code>/status</code> <i>to view the table</i>."];
}

function clampDays(days) {
	return Math.max(1, Math.min(90, Math.round(days)));
}

function parseFirstPositiveNumber(input) {
	const parts = String(input || "")
		.split(/\s+/)
		.map((p) => p.trim())
		.filter(Boolean);
	if (!parts.length) return null;
	const num = Number(parts[0]);
	if (!Number.isFinite(num) || num <= 0) return null;
	return Math.floor(num);
}

function handleHistoryIdxInput(chatKey, rawText) {
	const idx = parseFirstPositiveNumber(rawText);
	if (!idx) {
		return [{ text: historyInvalidIdxMessage(), reply_markup: MENU_KEYBOARD }];
	}
	HISTORY_PROMPTS.set(chatKey, { stage: "await_days", idx });
	return [historyAskDaysMessage(idx)];
}

async function handleHistoryDaysInput(chatKey, rawText, idx, env, NAME_MAP) {
	const days = parseFirstPositiveNumber(rawText);
	if (!days) {
		return [{ text: historyInvalidDaysMessage(), reply_markup: MENU_KEYBOARD }];
	}
	const clamped = clampDays(days);
	HISTORY_PROMPTS.delete(chatKey);
	return fulfillHistoryRequest(idx, clamped, env, NAME_MAP);
}

function historyAskIdxMessage() {
	return {
		text: [
			"<b>Histórico</b>",
			"Indica o número do gateway (consulta <code>/status</code> para ver a lista)."
		].join("\n"),
		reply_markup: MENU_KEYBOARD,
	};
}

function historyAskDaysMessage(idx) {
	return {
		text: [
			`<b>Gateway #${idx}</b>`,
			"Quantos dias queres analisar? (1-90)"
		].join("\n"),
		reply_markup: MENU_KEYBOARD,
	};
}

function historyInvalidIdxMessage() {
	return [
		"<i>Valor inválido.</i>",
		"Escreve apenas o número do gateway (ex.: <code>7</code>)."
	].join("\n");
}

function historyInvalidDaysMessage() {
	return [
		"<i>Dias inválidos.</i>",
		"Indica apenas o número de dias (1-90)."
	].join("\n");
}

async function fulfillHistoryRequest(idx, days, env, NAME_MAP) {
	// Reconstrói o índice (ordenado por nome) com gwEUI
	const list = await buildIndexList(env, NAME_MAP);
	if (!list.ok) return [`<i>Falha a obter índice</i>: ${escapeHtml(list.err || "erro")}`];

	const entry = list.items.find(r => r.idx === idx);
	if (!entry) return ["<i>Índice não encontrado.</i> Corre <code>/status</code> e tenta de novo."];

	const now = new Date();
	const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
	const stepMin = (days <= 7 ? 5 : 60);

	console.log('[history] args', { idx, name: entry.name, gwEUI: entry.gwEUI, days, stepMin, startIso: start.toISOString(), endIso: now.toISOString() });

	const rec = await fetchSeriesForGateway(env, entry.gwEUI, start, now, stepMin);
	if (!rec.ok) {
		console.log('[history] error', rec.err || 'unknown');
		return ['<i>Falha a obter série — vê os logs.</i>'];
	}

	const { slots, pctUp, eventsCount, offMs, transitions = [] } = rec;
	const preview = slots.slice(0, 120);
	console.log('[history] result', {
		idx,
		gwEUI: entry.gwEUI,
		name: entry.name,
		days,
		stepMin,
		startIso: start.toISOString(),
		endIso: now.toISOString(),
		eventsCount,
		slotsLen: slots.length,
		pctUp,
		offMs,
		transitionsCount: transitions.length,
		slotsPreview: preview,
	});
	logSlotsHourLines('history', start, slots, stepMin);

	return formatUptimeReport(entry, days, rec, start, now, transitions);
}

function detectMenuShortcut(text) {
	if (!text || typeof text !== "string") return null;
	const cleaned = stripDiacritics(text).toLowerCase().replace(/[^\w\s/]/g, "").trim();
	if (!cleaned || cleaned.startsWith("/")) return null;
	if (cleaned.endsWith("status")) return "status";
	if (cleaned.endsWith("historico")) return "history";
	return null;
}

function stripDiacritics(str) {
	return str.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

// ---------- ResIOT ----------

// For table (/status)
async function fetchGatewaysDirect(env, NAME_MAP) {
  const base = String(env.RESIOT_BASE || '').trim().replace(/\/+$/, '');
  const token = env.RESIOT_TOKEN || '';
  if (!base || !/^https?:\/\//i.test(base) || !token) {
    const info = { has_BASE: !!base, has_TOKEN: !!token };
    console.error('[resiot] missing/invalid env', info);
    return { ok: false, err: `Config missing: RESIOT_BASE (got="${base || '-'}") and/or RESIOT_TOKEN` };
  }

  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort('timeout'), 15_000);

    const resp = await fetch(`${base}/api/gateways?limit=1000`, {
      headers: {
        accept: 'application/json',
        'Grpc-Metadata-Authorization': token,
        'user-agent': 'fundao-bot/1.0',
      },
      signal: ctrl.signal,
    }).finally(() => clearTimeout(timer));

    if (!resp?.ok) {
      const txt = await resp.text().catch(() => String(resp?.status ?? ''));
      return { ok: false, err: `${resp?.status ?? ''} ${txt}`.trim() };
    }

    const data = await resp.json().catch(() => ({}));
    const arr = Array.isArray(data?.result) ? data.result : [];
    const rows = [];

    for (const g of arr) {
      const code = g?.name ?? '—';
      const fullName = NAME_MAP[code] || code;
      const timeoutSec = Number(g?.timeout) > 0 ? Number(g.timeout) : FALLBACK_TIMEOUT_SEC;

      // Prefer LastUplink, then Radio, then Monitor
      const lastRaw = g?.LastUplink || g?.LastAliveRadio || g?.LastAliveMonitor || '';
      const lastDate = parseResiotTime(lastRaw);
      const whenStr = normalizeToLisbon(lastRaw, lastDate);

      let emoji = '❌';
      if (lastDate instanceof Date) {
        const ageMs = Date.now() - lastDate.getTime();
        emoji = ageMs <= timeoutSec * 1000 ? '✅' : '❌';
      }

      rows.push({ name: fullName, when: whenStr, emoji });
    }

    rows.sort((a, b) => a.name.localeCompare(b.name, 'pt'));
    rows.forEach((row, i) => { row.idx = i + 1; });
    return { ok: true, rows };
  } catch (e) {
    console.error('resiot error:', e);
    return { ok: false, err: String(e) };
  }
}

// For cron (transition recording) — same logic + gwEUI + /lastmessage fallback
async function currentStates(env, NAME_MAP, now) {
  const base = String(env.RESIOT_BASE || '').trim().replace(/\/+$/, '');
  const token = env.RESIOT_TOKEN || '';
  if (!base || !/^https?:\/\//i.test(base) || !token) {
    console.error('[resiot] missing/invalid env', { has_BASE: !!base, has_TOKEN: !!token });
    return { ok: false, err: 'Config missing: RESIOT_BASE and/or RESIOT_TOKEN' };
  }

  try {
    const r = await fetch(`${base}/api/gateways?limit=1000`, {
      headers: { accept: 'application/json', 'Grpc-Metadata-Authorization': token },
    });
    if (!r.ok) return { ok: false, err: `${r.status} ${await r.text().catch(() => r.status)}` };
    const j = await r.json().catch(() => ({}));
    const arr = Array.isArray(j?.result) ? j.result : [];
    const out = [];

    for (const g of arr) {
      const code = g?.name ?? '-';
      const gwEUI = g?.gwEUI || g?.GatewayEUI || g?.eui || code;
      const fullName = NAME_MAP[code] || code;
      const timeoutSec = Number(g?.timeout) > 0 ? Number(g.timeout) : FALLBACK_TIMEOUT_SEC;

      let raw = g?.LastUplink || g?.LastAliveRadio || g?.LastAliveMonitor || null;
      let last = raw ? parseResiotTime(raw) : null;

      // optional fallback: /lastmessage
      if (!last && gwEUI && gwEUI !== code) {
        try {
          const r2 = await fetch(
            `${base}/api/gateways/${encodeURIComponent(gwEUI)}/lastmessage`,
            { headers: { accept: 'application/json', 'Grpc-Metadata-Authorization': token } }
          );
          if (r2.ok) {
            const j2 = await r2.json().catch(() => ({}));
            const maybe = j2?.date || j2?.lastSeen || j2?.LastAlive || j2?.LastUplink || null;
            if (maybe) last = parseResiotTime(String(maybe));
          }
        } catch { /* ignore */ }
      }

      const lastIso = last ? last.toISOString() : '';
      const s = last && (now - last) <= timeoutSec * 1000 ? 1 : 0;
      out.push({ gwEUI, name: fullName, s, lastIso });
    }
    return { ok: true, items: out };
  } catch (e) {
    return { ok: false, err: String(e) };
  }
}

// ---------- Debug logging for 5-min slots ----------

function timeHHmmLisbon(dt) {
	const parts = new Intl.DateTimeFormat('pt-PT', {
		timeZone: 'Europe/Lisbon',
		hour: '2-digit',
		minute: '2-digit',
		hour12: false,
	}).formatToParts(dt);
	const get = (t) => parts.find((p) => p.type === t)?.value || '';
	return `${get('hour')}:${get('minute')}`;
}

function floorToLisbonHour(dt) {
	const parts = new Intl.DateTimeFormat('en-GB', {
		timeZone: 'Europe/Lisbon',
		year: 'numeric', month: '2-digit', day: '2-digit',
		hour: '2-digit', hour12: false,
	}).formatToParts(dt);
	const get = (t) => parts.find((p) => p.type === t)?.value || '00';
	const y = Number(get('year'));
	const m = Number(get('month'));
	const d = Number(get('day'));
	const hh = Number(get('hour'));
	// Construímos um Date estável em UTC a partir dos componentes de Lisboa
	return new Date(Date.UTC(y, m - 1, d, hh, 0, 0));
}

function hourLabelLisbon(dt) {
	const parts = new Intl.DateTimeFormat('pt-PT', {
		timeZone: 'Europe/Lisbon',
		hour: '2-digit', minute: '2-digit', hour12: false,
	}).formatToParts(dt);
	const get = (t) => parts.find((p) => p.type === t)?.value || '';
	return `${get('hour')}:00`; // força minutos :00
}

/**
 * Loga TODOS os slots (0/1) agrupados por hora.
 * Para stepMin=5 → 12 dígitos por linha. Cada linha começa com a hora (Lisboa).
 */
function logSlotsHourLines(label, start, slots, stepMin) {
	const perHour = Math.max(1, Math.round(60 / Math.max(1, stepMin)));
	const stepMs = stepMin * 60 * 1000;

	// Base alinhada à hora cheia (Lisboa)
	const base = floorToLisbonHour(start);
	// Quantos slots existem entre a base e o início real
	const prePadSlots = Math.floor((start.getTime() - base.getTime()) / stepMs);
	const totalSlotsWithPad = prePadSlots + slots.length;
	const totalHours = Math.ceil(totalSlotsWithPad / perHour);

	console.log(`[${label}] slots detail: total=${slots.length} stepMin=${stepMin} perHour=${perHour} alignedBase=${base.toISOString()}`);

	for (let h = 0; h < totalHours; h++) {
		const hourStartIdx = h * perHour; // índice relativo à base alinhada
		const hourDt = new Date(base.getTime() + h * 60 * 60 * 1000);
		const hourStr = hourLabelLisbon(hourDt);

		let line = '';
		for (let i = 0; i < perHour; i++) {
			const relIdx = hourStartIdx + i; // índice desde a base
			if (relIdx < prePadSlots) {
				line += '.'; // antes da janela real
			} else {
				const idx = relIdx - prePadSlots; // índice no array real
				if (idx >= 0 && idx < slots.length) line += (slots[idx] ? '1' : '0');
				else line += '.'; // depois da janela real
			}
		}
		console.log(`[${label}] ${String(h).padStart(2, '0')} ${hourStr} ${line}`);
	}
}

// ---------- Dates / formatting ----------

// Lisbon wall-clock ISO without timezone (e.g., "2025-09-22T18:05:26")
function lisbonWallIso(d) {
	const parts = new Intl.DateTimeFormat('en-GB', {
		timeZone: 'Europe/Lisbon',
		year: 'numeric',
		month: '2-digit',
		day: '2-digit',
		hour: '2-digit',
		minute: '2-digit',
		second: '2-digit',
		hour12: false,
	}).formatToParts(d);
	const get = (t) => parts.find((p) => p.type === t)?.value || '00';
	const y = get('year');
	const m = get('month');
	const day = get('day');
	const hh = get('hour');
	const mm = get('minute');
	const ss = get('second');
	return `${y}-${m}-${day}T${hh}:${mm}:${ss}`;
}

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

	const HI = "Nº", HN = "Nome", HW = "Quando", HS = "Ok";
	const idxW = Math.max(HI.length, ...filtered.map((r) => String(r.idx || "").length));
	const nameW = Math.max(HN.length, ...filtered.map((r) => (r.name || "").length));
	const whenW = Math.max(HW.length, ...filtered.map((r) => (r.when || "").length));
	const stateW = HS.length;

	const header = `${padRight(HI, idxW)}|${padRight(HN, nameW)}|${padRight(HW, whenW)}|${HS}`;
	const sep = `${"-".repeat(idxW)}+${"-".repeat(nameW)}+${"-".repeat(whenW)}+${"-".repeat(stateW)}`;
	const body = filtered
		.map((r) => `${padRight(String(r.idx || ""), idxW)}|${padRight(r.name || "", nameW)}|${padRight(r.when || "", whenW)}|${r.emoji || ""}`)
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
	// Primeiro dia do mês às 00:00 em Lisboa, sem timezone
	const firstDayUtc = new Date(Date.UTC(y, Number(m) - 1, 1, 0, 0, 0));
	const t0 = lisbonWallIso(firstDayUtc);

	const base = Object.keys(prevSnapshot || {}).length ? prevSnapshot : nextSnapshot || {};
	const state = {};
	for (const [gw, st] of Object.entries(base)) state[gw] = to01(st);

	const payload = { t0, state };
	await gistWriteJson(env, carryFilename, payload);
}

// ---------- Fetch series for a gateway (no rendering) ----------

function enumerateMonths(start, end) {
	const out = [];
	const y0 = start.getUTCFullYear();
	const m0 = start.getUTCMonth();
	const y1 = end.getUTCFullYear();
	const m1 = end.getUTCMonth();
	for (let y = y0; y <= y1; y++) {
		const mStart = (y === y0) ? m0 : 0;
		const mEnd = (y === y1) ? m1 : 11;
		for (let m = mStart; m <= mEnd; m++) {
			out.push({ y, m: m + 1 });
		}
	}
	return out;
}

async function fetchSeriesForGateway(env, gwEUI, start, end, stepMin = 5) {

	// Parse "YYYY-MM-DDTHH:mm:ss" como hora de Lisboa (sem timezone)
	function parseLisbonWall(ts) {
		if (typeof ts !== 'string') return new Date(NaN);
		const m = ts.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$/);
		if (!m) return new Date(ts);
		const y = Number(m[1]), mo = Number(m[2]), d = Number(m[3]);
		const hh = Number(m[4]), mm = Number(m[5]), ss = Number(m[6]);
		// Construímos um Date estável a partir dos componentes (usamos UTC como baseline num instante equivalente)
		return new Date(Date.UTC(y, mo - 1, d, hh, mm, ss));
	}

	try {
		// 1) Enumerate months
		const months = enumerateMonths(start, end);

		// 2) Load monthly carry and transitions
		let initState = undefined;
		const events = [];
		const touched = [];

		for (const mm of months) {
			const key = `${mm.y}-${String(mm.m).padStart(2, '0')}`;
			const carryName = `${CARRY_PREFIX}${key}.json`;
			const transName = `${MONTH_PREFIX}${key}.ndjson`;

			const carry = await gistReadJsonSafe(env, carryName);
			if (carry && carry.state && Object.prototype.hasOwnProperty.call(carry.state, gwEUI)) {
				if (initState === undefined) initState = to01(carry.state[gwEUI]);
			}

			const text = await gistReadFileText(env, transName);
			if (text) {
				touched.push(transName);
				const lines = text.split(/\n+/);
				for (const line of lines) {
					if (!line) continue;
					try {
						const obj = JSON.parse(line);
						if (obj.gw === gwEUI) events.push(obj); // {t,gw,s}
					} catch { }
				}
			}
		}
		if (initState === undefined) initState = 0;

		// 3) Sort events and count in window
		events.sort((a, b) => parseLisbonWall(a.t) - parseLisbonWall(b.t));
		const transitionsInWindow = events.filter(e => {
			const tt = parseLisbonWall(e.t);
			return tt > start && tt <= end;
		});
		const eventsCountInWindow = transitionsInWindow.length;

		console.log('[fetch] events', { gwEUI, months: months.length, files: touched.length, eventsTotal: events.length, eventsInWindow: eventsCountInWindow });

		// 4) Determine state at start by applying events up to start
		let stateAtStart = initState;
		let iPtr = 0;
		while (iPtr < events.length && parseLisbonWall(events[iPtr].t) <= start) {
			stateAtStart = to01(events[iPtr].s);
			iPtr++;
		}

		// 5) Build intervals over [start, end]
		const intervals = [];
		let curT = new Date(start);
		let curS = stateAtStart;

		for (let k = iPtr; k < events.length; k++) {
			const tEv = parseLisbonWall(events[k].t);
			if (tEv > end) break;
			if (tEv <= curT) { curS = to01(events[k].s); continue; }
			intervals.push({ from: curT, to: new Date(tEv), s: curS });
			curT = new Date(tEv);
			curS = to01(events[k].s);
		}
		if (curT < end) intervals.push({ from: curT, to: new Date(end), s: curS });

		// 6) Integrate uptime
		const totalMs = Math.max(1, end - start);
		let upMs = 0;
		for (const iv of intervals) {
			if (iv.s) upMs += (iv.to - iv.from);
		}
		const pctUp = (upMs / totalMs) * 100;

		// 7) Rasterize into slots
		const stepMs = stepMin * 60 * 1000;
		const totalSteps = Math.max(1, Math.ceil((end - start) / stepMs));
		const slots = new Array(totalSteps).fill(0);

		let j = 0;
		for (let si = 0; si < totalSteps; si++) {
			const slotStart = new Date(start.getTime() + si * stepMs);
			const slotEnd = new Date(Math.min(end.getTime(), slotStart.getTime() + stepMs));

			let onMs = 0;
			while (j < intervals.length && intervals[j].to <= slotStart) j++;
			let k = j;
			while (k < intervals.length && intervals[k].from < slotEnd) {
				const iv = intervals[k];
				const ovStart = Math.max(slotStart.getTime(), iv.from.getTime());
				const ovEnd = Math.min(slotEnd.getTime(), iv.to.getTime());
				if (ovEnd > ovStart && iv.s) onMs += (ovEnd - ovStart);
				if (iv.to >= slotEnd) break;
				k++;
			}
			slots[si] = onMs >= (stepMs / 2) ? 1 : 0;
		}

		const offMs = Math.max(0, totalMs - upMs);
		return { ok: true, slots, pctUp, eventsCount: eventsCountInWindow, offMs, transitions: transitionsInWindow };
	} catch (e) {
		return { ok: false, err: String(e) };
	}
}

// Short duration formatter (used in logs if needed)
function fmtDur(ms) {
	ms = Math.max(0, Math.floor(ms));
	const sec = Math.floor(ms / 1000);
	const h = Math.floor(sec / 3600);
	const m = Math.floor((sec % 3600) / 60);
	const s = sec % 60;
	if (h > 0) return `${h}h ${String(m).padStart(2, '0')}m`;
	if (m > 0) return `${m}m ${String(s).padStart(2, '0')}s`;
	return `${s}s`;
}

// Format uptime report for Telegram (returns array of messages if needed)
function formatUptimeReport(entry, days, rec, start, end, transitions = []) {
	const { pctUp, offMs } = rec;
	const totalMs = end - start;
	const upMs = totalMs - offMs;

	const pctDown = 100 - pctUp;
	const durUp = fmtDur(upMs);
	const durDown = fmtDur(offMs);

	// Format dates in Lisbon time
	const startStr = formatLisbonDateTime(start);
	const endStr = formatLisbonDateTime(end);

	const headerLines = [
		`<b>${escapeHtml(entry.name)}</b>`,
		`EUI: <code>${escapeHtml(entry.gwEUI)}</code>`,
		`Código: <code>${escapeHtml(entry.code || '-')}</code>`,
		``,
		`<b>Período:</b> ${days} dia${days > 1 ? 's' : ''}`,
		`De: ${escapeHtml(startStr)}`,
		`Até: ${escapeHtml(endStr)}`,
		``,
		`<b>Uptime:</b> ${pctUp.toFixed(2)}% ✅`,
		`Tempo online: ${durUp}`,
		``,
		`<b>Downtime:</b> ${pctDown.toFixed(2)}% ❌`,
		`Tempo offline: ${durDown}`,
	];

	const MAX_LENGTH = 3800; // Safe margin below Telegram's 4096 limit
	const messages = [headerLines.join('\n')];

	if (transitions.length > 0) {
		const transitionMessages = buildTransitionMessages(transitions, MAX_LENGTH);
		if (transitionMessages.length) messages.push(...transitionMessages);
	}

	return messages;
}

function buildTransitionMessages(transitions, maxLen) {
	const lines = buildTransitionTableLines(transitions);
	if (!lines.length) return [];
	const blocks = chunkLinesIntoPreBlocks(lines, maxLen);
	const header = `<b>Transições (${transitions.length}):</b>`;
	if (!blocks.length) return [header];

	const out = [];
	const first = blocks.shift();
	if ((header + "\n" + first).length <= maxLen) {
		out.push(`${header}\n${first}`);
	} else {
		out.push(header);
		out.push(first);
	}
	if (blocks.length) out.push(...blocks);
	return out;
}

function buildTransitionTableLines(transitions) {
	if (!Array.isArray(transitions) || !transitions.length) return [];
	const rows = transitions.slice().reverse().map((tr) => {
		const { day, time } = splitTransitionTimestamp(tr.t);
		const state = tr.s === 1 ? "✅ Online" : "❌ Offline";
		return { day, time, state };
	});

	const dayLabel = "Dia";
	const timeLabel = "Hora";
	const stateLabel = "Estado";
	const dayW = Math.max(dayLabel.length, ...rows.map((r) => (r.day || "").length));
	const timeW = Math.max(timeLabel.length, ...rows.map((r) => (r.time || "").length));
	const stateW = Math.max(stateLabel.length, ...rows.map((r) => (r.state || "").length));

	const header = `${padRight(dayLabel, dayW)} | ${padRight(timeLabel, timeW)} | ${padRight(stateLabel, stateW)}`;
	const sep = `${"-".repeat(dayW)}-+-${"-".repeat(timeW)}-+-${"-".repeat(stateW)}`;
	const body = rows.map((r) => `${padRight(r.day, dayW)} | ${padRight(r.time, timeW)} | ${padRight(r.state, stateW)}`);
	return [header, sep, ...body];
}

function splitTransitionTimestamp(ts) {
	if (typeof ts !== "string") return { day: "-", time: "-" };
	const m = ts.match(/^(\d{4})-(\d{2})-(\d{2})[T\s](\d{2}):(\d{2})(?::(\d{2}))?/);
	if (!m) return { day: ts || "-", time: "-" };
	const day = `${m[3]}-${m[2]}-${m[1]}`;
	const time = `${m[4]}:${m[5]}${m[6] ? `:${m[6]}` : ""}`;
	return { day, time };
}

function chunkLinesIntoPreBlocks(lines, maxLen) {
	const blocks = [];
	let buffer = [];
	const flush = () => {
		if (!buffer.length) return;
		const text = buffer.join("\n");
		blocks.push(`<pre>${escapeHtml(text)}</pre>`);
		buffer = [];
	};

	for (const line of lines) {
		buffer.push(line);
		let candidate = `<pre>${escapeHtml(buffer.join("\n"))}</pre>`;
		if (candidate.length > maxLen) {
			buffer.pop();
			flush();
			buffer = [line];
			candidate = `<pre>${escapeHtml(buffer.join("\n"))}</pre>`;
			if (candidate.length > maxLen) {
				blocks.push(candidate);
				buffer = [];
			}
		}
	}
	flush();
	return blocks;
}

// Legacy-safe normalizer: "OK"/"NOK", booleans, "1"/"0" -> 1/0
function to01(v) {
	if (v === 1 || v === "1" || v === true || v === "OK") return 1;
	return 0;
}
// ---------- Telegram helpers ----------

async function sendText(env, chatId, payload) {
	if (typeof payload === "string") payload = { text: payload };
	const text = typeof payload?.text === "string" ? payload.text : "";
	const replyMarkup = payload?.reply_markup;
	return tgApi(env, "sendMessage", {
		chat_id: chatId,
		text,
		parse_mode: "HTML",
		disable_web_page_preview: true,
		...(replyMarkup ? { reply_markup: replyMarkup } : {}),
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
// Build index list (sorted by name) including gwEUI and user-friendly name
async function buildIndexList(env, NAME_MAP) {
  const base = String(env.RESIOT_BASE || '').trim().replace(/\/+$/, '');
  const token = env.RESIOT_TOKEN || '';
  if (!base || !/^https?:\/\//i.test(base) || !token) {
    console.error('[resiot] missing/invalid env', { has_BASE: !!base, has_TOKEN: !!token });
    return { ok: false, err: 'Config missing: RESIOT_BASE and/or RESIOT_TOKEN' };
  }

  try {
    const r = await fetch(`${base}/api/gateways?limit=1000`, {
      headers: { accept: 'application/json', 'Grpc-Metadata-Authorization': token }
    });
    if (!r.ok) return { ok: false, err: `${r.status} ${await r.text().catch(() => r.status)}` };
    const j = await r.json().catch(() => ({}));
    const arr = Array.isArray(j?.result) ? j.result : [];

    const items = arr.map(g => {
      const code = g?.name ?? '-';
      const gwEUI = g?.gwEUI || g?.GatewayEUI || g?.eui || code;
      const fullName = NAME_MAP[code] || code;
      return { gwEUI, name: fullName, code };
    });

    items.sort((a, b) => a.name.localeCompare(b.name, 'pt'));
    items.forEach((it, i) => { it.idx = i + 1; });
    return { ok: true, items };
  } catch (e) {
    return { ok: false, err: String(e) };
  }
}
