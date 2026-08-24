// pi_gateway/security/trust/cidr — msgraph source-CIDR admission gating
// (06 §8.3; DEC-017 "msgraph: CIDR allowlist … passive — no subscription
// renewal").
//
// Ported from the READ-ONLY Hermes reference:
//   gateway/platforms/msgraph_webhook.py:_parse_allowed_source_cidrs (@112) —
//     comma/list forms, ip_network(strict=False) (host bits cleared), invalid
//     entries SKIPPED with a warning; empty allowlist ⇒ "allow everything"
//     (the field predates this module's existence behavior).
//   msgraph_webhook.py:_source_allowlist_required_but_missing (@148) +
//     webhook.py:_LOOPBACK_HOSTS (@136)/_is_loopback_host (@146) — a bind
//     that is network-accessible (unset/any-interface/non-loopback host)
//     WITHOUT any configured CIDR refuses CONNECT with guidance.
//   msgraph_webhook.py:_source_ip_allowed (@316) — admission binds to the
//     SOCKET PEER (`request.remote`) ONLY. X-Forwarded-For and every other
//     forwarded header are attacker-controlled on direct ingress and are
//     never consulted: spoofing XFF cannot move an out-of-range peer into
//     the allowlist, and junk XFF cannot evict an in-range peer.

/** Loopback hostnames/IPs that may omit the CIDR allowlist (webhook.py @136). */
const LOOPBACK_HOSTS: ReadonlySet<string> = new Set([
	"127.0.0.1",
	"localhost",
	"::1",
	"ip6-localhost",
	"ip6-loopback",
]);

export interface ParsedIp {
	family: 4 | 6;
	value: bigint;
}

export interface ParsedNetwork {
	family: 4 | 6;
	/** Network address with HOST BITS CLEARED (strict=False parity). */
	network: bigint;
	prefixBits: number;
}

// ── address parsing ──────────────────────────────────────────────────────

export function parseIpv4(text: string): ParsedIp | null {
	const m = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(text);
	if (m === null) return null;
	let value = 0n;
	for (let i = 1; i <= 4; i++) {
		const octet = Number.parseInt(m[i] as string, 10);
		if (octet > 255) return null;
		// Leading zeros beyond canonical form are tolerated by ip_address.
		value = (value << 8n) | BigInt(octet);
	}
	return { family: 4, value };
}

function parseIpv6Groups(hexParts: string[]): bigint[] | null {
	const groups: bigint[] = [];
	for (const part of hexParts) {
		if (!/^[0-9A-Fa-f]{1,4}$/.test(part)) return null;
		groups.push(BigInt(Number.parseInt(part, 16)));
	}
	return groups;
}

export function parseIpv6(text: string): ParsedIp | null {
	if (!text.includes(":")) return null;
	// Zone index (fe80::1%eth0) is not an admission identity here.
	const withoutZone = text.split("%", 1)[0] as string;

	// Embedded IPv4 tail (::ffff:192.0.2.9 / 64:ff9b::1.2.3.4).
	let head = withoutZone;
	const v4Tail: bigint[] = [];
	const v4Match = /:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/.exec(withoutZone);
	if (v4Match !== null) {
		const parsed = parseIpv4(v4Match[1] as string);
		if (parsed === null) return null;
		v4Tail.push(parsed.value >> 16n, parsed.value & 0xffffn);
		head = withoutZone.slice(
			0,
			withoutZone.length - (v4Match[1] as string).length,
		);
		if (head.endsWith(":")) head = head.slice(0, -1); // drop the group separator
	}

	const doubleColon = head.split("::");
	if (doubleColon.length > 2) return null;

	let leftGroups: bigint[];
	let rightGroups: bigint[];
	if (doubleColon.length === 2) {
		const leftRaw =
			doubleColon[0] === "" ? [] : (doubleColon[0] as string).split(":");
		const rightRaw =
			doubleColon[1] === "" ? [] : (doubleColon[1] as string).split(":");
		const left = parseIpv6Groups(leftRaw);
		const right = parseIpv6Groups(rightRaw);
		if (left === null || right === null) return null;
		leftGroups = left;
		rightGroups = [...right, ...v4Tail];
	} else {
		const all = parseIpv6Groups((head === "" ? [] : head.split(":")).concat());
		if (all === null) return null;
		leftGroups = all;
		rightGroups = v4Tail;
	}

	const total = leftGroups.length + rightGroups.length;
	if (total > 8) return null;
	const missing = doubleColon.length === 2 ? 8 - total : 0;
	if (doubleColon.length === 2 && missing < 0) return null;

	const groups = [
		...leftGroups,
		...Array.from({ length: missing }, () => 0n),
		...rightGroups,
	];
	if (groups.length !== 8) return null;

	let value = 0n;
	for (const g of groups) value = (value << 16n) | g;
	return { family: 6, value };
}

export function parseIpAddress(text: string): ParsedIp | null {
	const trimmed = text.trim();
	if (trimmed.length === 0) return null;
	const bracketed =
		trimmed.startsWith("[") && trimmed.endsWith("]")
			? trimmed.slice(1, -1)
			: trimmed;
	return parseIpv4(bracketed) ?? parseIpv6(bracketed);
}

// ── network parsing (ip_network(strict=False) parity) ────────────────────

/**
 * Parse one CIDR entry ("10.0.0.0/8", "2001:db8::/32", bare IP). Host bits
 * are cleared (strict=False); invalid entries yield null (caller skips with
 * a warning — msgraph_webhook.py "Ignoring invalid allowed_source_cidrs").
 */
export function parseCidr(entry: string): ParsedNetwork | null {
	const trimmed = entry.trim();
	if (trimmed.length === 0) return null;
	const slash = trimmed.indexOf("/");
	const addrText = slash === -1 ? trimmed : trimmed.slice(0, slash);
	const prefixText = slash === -1 ? null : trimmed.slice(slash + 1);
	const ip = parseIpAddress(addrText);
	if (ip === null) return null;
	const familyBits = ip.family === 4 ? 32 : 128;
	let prefixBits = familyBits;
	if (prefixText !== null) {
		if (!/^\d+$/.test(prefixText)) return null;
		prefixBits = Number.parseInt(prefixText, 10);
		if (prefixBits > familyBits) return null;
	}
	const shift = BigInt(familyBits - prefixBits);
	const mask =
		prefixBits === 0
			? 0n
			: ~((1n << shift) - 1n) & ((1n << BigInt(familyBits)) - 1n);
	return {
		family: ip.family,
		network: ip.value & mask,
		prefixBits,
	};
}

export type CidrAllowlistParse = {
	networks: ParsedNetwork[];
	/** Entries skipped as invalid (each becomes an operator warning). */
	invalid: string[];
};

/**
 * Parse the full allowlist (string CSV or list form). Empty/missing input
 * yields an EMPTY list — callers decide between loopback exemption and
 * connect refusal via `allowlistRequiredButMissing`.
 */
export function parseCidrAllowlist(
	raw: string | readonly string[] | null | undefined,
): CidrAllowlistParse {
	if (raw === null || raw === undefined) return { networks: [], invalid: [] };
	const candidates =
		typeof raw === "string"
			? raw.split(",").map((chunk) => chunk.trim())
			: raw.map((chunk) => String(chunk).trim());
	const networks: ParsedNetwork[] = [];
	const invalid: string[] = [];
	for (const chunk of candidates) {
		if (chunk.length === 0) continue;
		const parsed = parseCidr(chunk);
		if (parsed === null) {
			invalid.push(chunk);
			continue;
		}
		networks.push(parsed);
	}
	return { networks, invalid };
}

function ipInNetwork(ip: ParsedIp, net: ParsedNetwork): boolean {
	if (ip.family !== net.family) return false;
	const familyBits = net.family === 4 ? 32 : 128;
	const shift = BigInt(familyBits - net.prefixBits);
	const mask =
		net.prefixBits === 0
			? 0n
			: (((1n << BigInt(net.prefixBits)) - 1n) << shift) &
				((1n << BigInt(familyBits)) - 1n);
	return (ip.value & mask) === (net.network & mask);
}

/** True when the peer address falls inside ANY allowlisted range. */
export function ipInNetworks(
	peer: string,
	networks: readonly ParsedNetwork[],
): boolean {
	const ip = parseIpAddress(peer);
	if (ip === null) return false; // unparseable peers fail CLOSED
	return networks.some((net) => ipInNetwork(ip, net));
}

// ── bind-time + per-request gates ────────────────────────────────────────

/** webhook.py:_is_loopback_host parity (falsy ⇒ conservatively public). */
export function isLoopbackBindHost(host: string | null | undefined): boolean {
	if (!host) return false;
	return LOOPBACK_HOSTS.has(host.trim().toLowerCase());
}

/**
 * msgraph_webhook.py:_source_allowlist_required_but_missing parity: a bind
 * reachable off-machine (unset host, non-loopback literal) REQUIRES a
 * configured CIDR allowlist — its absence refuses CONNECT with guidance.
 */
export function allowlistRequiredButMissing(
	bindHost: string | null | undefined,
	networks: readonly ParsedNetwork[],
): boolean {
	const loopbackOnly = isLoopbackBindHost(bindHost);
	return !loopbackOnly && networks.length === 0;
}

/** Minimal request surface the gate needs (socket peer only). */
export interface PeerRequest {
	/** The TCP/socket peer address (`request.remote` parity). */
	remoteAddr: string;
}

/**
 * Per-request admission (msgraph_webhook.py:_source_ip_allowed parity):
 *   * required-but-missing ⇒ deny (fail closed even before connect refusal),
 *   * empty allowlist on a loopback-only bind ⇒ admit,
 *   * otherwise admit iff the PEER address is inside some listed range.
 *
 * Forwarded headers (X-Forwarded-For et al.) are DELIBERATELY NOT part of
 * the request surface: spoof handling per spec = they cannot influence the
 * verdict in either direction.
 */
export function sourceIpAllowed(
	request: PeerRequest,
	bindHost: string | null | undefined,
	networks: readonly ParsedNetwork[],
): boolean {
	if (allowlistRequiredButMissing(bindHost, networks)) return false;
	if (networks.length === 0) return true; // loopback-only bind, no CIDRs
	return ipInNetworks(request.remoteAddr, networks);
}
