// Script to fix historical timestamps in Gist by rounding to 5-minute intervals

const GH_API = "https://api.github.com";

// Round timestamp to nearest interval (in minutes)
function roundToNearestInterval(date, intervalMinutes) {
	const ms = date.getTime();
	const intervalMs = intervalMinutes * 60 * 1000;
	const rounded = Math.round(ms / intervalMs) * intervalMs;
	return new Date(rounded);
}

// Parse Lisboa wall-clock format
function parseLisbonWall(ts) {
	if (typeof ts !== 'string') return new Date(NaN);
	const m = ts.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$/);
	if (!m) return new Date(ts);
	const y = Number(m[1]), mo = Number(m[2]), d = Number(m[3]);
	const hh = Number(m[4]), mm = Number(m[5]), ss = Number(m[6]);
	return new Date(Date.UTC(y, mo - 1, d, hh, mm, ss));
}

// Format as Lisboa wall-clock
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

function ghHeaders(token) {
	return {
		authorization: `Bearer ${token}`,
		accept: 'application/vnd.github+json',
		'user-agent': 'fundao-bot-fix/1.0',
		'x-github-api-version': '2022-11-28',
	};
}

async function getGist(gistId, token) {
	const url = `${GH_API}/gists/${gistId}`;
	const r = await fetch(url, { headers: ghHeaders(token) });
	if (!r.ok) {
		throw new Error(`Failed to get gist: ${r.status}`);
	}
	return r.json();
}

async function readFileContent(gist, filename) {
	const f = gist.files?.[filename];
	if (!f) return null;

	// If truncated, fetch raw content
	if (f.truncated && f.raw_url) {
		const r = await fetch(f.raw_url);
		if (!r.ok) throw new Error(`Failed to fetch raw content: ${r.status}`);
		return await r.text();
	}

	return f.content || null;
}

async function updateGist(gistId, token, files) {
	const url = `${GH_API}/gists/${gistId}`;
	const body = { files };

	const r = await fetch(url, {
		method: 'PATCH',
		headers: { ...ghHeaders(token), 'content-type': 'application/json' },
		body: JSON.stringify(body),
	});

	if (!r.ok) {
		const text = await r.text().catch(() => '');
		throw new Error(`Failed to update gist: ${r.status} ${text}`);
	}

	return r.json();
}

function processNdjsonContent(content) {
	const lines = content.split('\n').filter(line => line.trim());
	let fixedCount = 0;

	const fixedLines = lines.map(line => {
		try {
			const obj = JSON.parse(line);
			if (!obj.t) return line;

			// Parse and round timestamp
			const originalDate = parseLisbonWall(obj.t);
			if (isNaN(originalDate.getTime())) return line;

			const roundedDate = roundToNearestInterval(originalDate, 5);
			const roundedTs = lisbonWallIso(roundedDate);

			// Check if changed
			if (obj.t !== roundedTs) {
				fixedCount++;
				obj.t = roundedTs;
			}

			return JSON.stringify(obj);
		} catch (e) {
			console.error('Failed to process line:', line, e);
			return line;
		}
	});

	return {
		content: fixedLines.join('\n') + '\n',
		fixedCount,
		totalLines: lines.length,
	};
}

async function main() {
	// Get credentials from environment
	const gistId = process.env.GITHUB_GIST_ID;
	const token = process.env.GITHUB_TOKEN;

	if (!gistId || !token) {
		console.error('Error: Missing GITHUB_GIST_ID or GITHUB_TOKEN environment variables');
		console.error('Set them in your .env or wrangler.toml file');
		process.exit(1);
	}

	console.log('📥 Fetching Gist...');
	const gist = await getGist(gistId, token);

	// Find all transition files
	const transitionFiles = Object.keys(gist.files).filter(name =>
		name.startsWith('gateways_uptime_transitions_') && name.endsWith('.ndjson')
	);

	console.log(`Found ${transitionFiles.length} transition files:`, transitionFiles.join(', '));

	if (transitionFiles.length === 0) {
		console.log('No transition files to fix!');
		return;
	}

	// Process each file
	const updates = {};
	let totalFixed = 0;

	for (const filename of transitionFiles) {
		console.log(`\n📝 Processing ${filename}...`);

		const content = await readFileContent(gist, filename);
		if (!content) {
			console.log(`  ⚠️  Empty or missing content, skipping`);
			continue;
		}

		const result = processNdjsonContent(content);
		console.log(`  ✅ Fixed ${result.fixedCount}/${result.totalLines} timestamps`);

		if (result.fixedCount > 0) {
			updates[filename] = { content: result.content };
			totalFixed += result.fixedCount;
		}
	}

	if (Object.keys(updates).length === 0) {
		console.log('\n✨ No timestamps needed fixing!');
		return;
	}

	console.log(`\n📤 Updating Gist with ${totalFixed} fixed timestamps...`);
	await updateGist(gistId, token, updates);

	console.log('✅ Done! All historical timestamps have been fixed.');
}

main().catch(err => {
	console.error('❌ Error:', err.message);
	process.exit(1);
});
