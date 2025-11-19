// Test script to understand the 5m 01s issue

// Round timestamp to nearest interval (in minutes)
function roundToNearestInterval(date, intervalMinutes) {
	const ms = date.getTime();
	const intervalMs = intervalMinutes * 60 * 1000;
	const rounded = Math.round(ms / intervalMs) * intervalMs;
	return new Date(rounded);
}

console.log('\n=== Test: Round to nearest 5-minute interval ===');

// Test cases with cron delays
const testCases = [
	{ input: '2025-10-15T17:05:11.000Z', expected: '2025-10-15T17:05:00.000Z' },
	{ input: '2025-10-15T17:10:12.000Z', expected: '2025-10-15T17:10:00.000Z' },
	{ input: '2025-10-15T17:07:30.000Z', expected: '2025-10-15T17:10:00.000Z' },  // closer to 17:10
	{ input: '2025-10-15T17:02:29.000Z', expected: '2025-10-15T17:00:00.000Z' },  // closer to 17:00
];

for (const tc of testCases) {
	const input = new Date(tc.input);
	const rounded = roundToNearestInterval(input, 5);
	const pass = rounded.toISOString() === tc.expected;
	console.log(`${pass ? '✅' : '❌'} ${tc.input} → ${rounded.toISOString()} (expected: ${tc.expected})`);
}

// Test the fix for the actual timestamps
console.log('\n=== Test: Fix for actual timestamps ===');
const event1 = new Date('2025-10-15T17:05:11.000Z');
const event2 = new Date('2025-10-15T17:10:12.000Z');

const rounded1 = roundToNearestInterval(event1, 5);
const rounded2 = roundToNearestInterval(event2, 5);

console.log('Original event1:', event1.toISOString());
console.log('Rounded event1:', rounded1.toISOString());
console.log('Original event2:', event2.toISOString());
console.log('Rounded event2:', rounded2.toISOString());

const diffOriginal = (event2 - event1) / 1000;
const diffRounded = (rounded2 - rounded1) / 1000;

console.log('\nDifference (original):', diffOriginal, 'seconds');
console.log('Difference (rounded):', diffRounded, 'seconds');
console.log('Fixed:', diffRounded === 300 ? '✅' : '❌');

// Test script to understand the 5m 01s issue

// Simulate the parseLisbonWall function
function parseLisbonWall(ts) {
	if (typeof ts !== 'string') return new Date(NaN);
	const m = ts.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$/);
	if (!m) return new Date(ts);
	const y = Number(m[1]), mo = Number(m[2]), d = Number(m[3]);
	const hh = Number(m[4]), mm = Number(m[5]), ss = Number(m[6]);
	// Construímos um Date estável a partir dos componentes (usamos UTC como baseline num instante equivalente)
	return new Date(Date.UTC(y, mo - 1, d, hh, mm, ss));
}

// Simulate lisbonWallIso function
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

// Test 1: Round-trip a timestamp
console.log('\n=== Test 1: Round-trip timestamp ===');
const now = new Date();
console.log('Original (UTC):', now.toISOString());
console.log('Original ms:', now.getTime());

const lisbonStr = lisbonWallIso(now);
console.log('Lisboa wall-clock string:', lisbonStr);

const parsed = parseLisbonWall(lisbonStr);
console.log('Parsed back (UTC):', parsed.toISOString());
console.log('Parsed ms:', parsed.getTime());

const diff = parsed.getTime() - now.getTime();
console.log('Difference (ms):', diff);
console.log('Difference (seconds):', Math.floor(diff / 1000));

// Test 2: Simulate downtime calculation for gateway without transitions
console.log('\n=== Test 2: Gateway without transitions ===');

const days = 90;
const endTime = new Date();
const startTime = new Date(endTime.getTime() - days * 24 * 60 * 60 * 1000);

console.log('Range start:', startTime.toISOString());
console.log('Range end:', endTime.toISOString());

// Simulate: gateway has been online the whole time (no transitions)
const events = [];
const initState = 1; // online
const stateAtStart = 1;

// Build intervals (simulating lines 1375-1389)
const intervals = [];
let curT = new Date(startTime);
let curS = stateAtStart;

// No events, so we go straight to line 1389
if (curT < endTime) {
	intervals.push({ from: curT, to: new Date(endTime), s: curS });
}

console.log('Intervals count:', intervals.length);
console.log('First interval:', {
	from: intervals[0].from.toISOString(),
	to: intervals[0].to.toISOString(),
	s: intervals[0].s,
	durationMs: intervals[0].to - intervals[0].from,
	durationSec: Math.floor((intervals[0].to - intervals[0].from) / 1000),
});

// Calculate uptime (simulating lines 1391-1397)
const totalMs = Math.max(1, endTime - startTime);
let upMs = 0;
for (const iv of intervals) {
	if (iv.s) upMs += (iv.to - iv.from);
}
const pctUp = (upMs / totalMs) * 100;

console.log('Total ms:', totalMs);
console.log('Total sec:', Math.floor(totalMs / 1000));
console.log('Up ms:', upMs);
console.log('Up sec:', Math.floor(upMs / 1000));

const offMs = Math.max(0, totalMs - upMs);
console.log('Off ms:', offMs);
console.log('Off sec:', Math.floor(offMs / 1000));
console.log('% Up:', pctUp.toFixed(2));

// Test 3: Check if Date objects are being copied correctly
console.log('\n=== Test 3: Date copying ===');
const d1 = new Date();
const d2 = new Date(d1);
const d3 = new Date(d1.getTime());

console.log('d1 === d2:', d1 === d2);
console.log('d1.getTime() === d2.getTime():', d1.getTime() === d2.getTime());
console.log('d1.getTime() === d3.getTime():', d1.getTime() === d3.getTime());
