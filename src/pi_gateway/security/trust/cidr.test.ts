// Behavior contracts for msgraph CIDR source gating (06 §8.3;
// msgraph_webhook.py port): in-range admitted, out-of-range denied, XFF
// spoofing cannot move the verdict, invalid entries skip-with-warning,
// non-loopback bind without CIDRs refuses connect.

import { describe, expect, it } from "vitest";
import {
	allowlistRequiredButMissing,
	ipInNetworks,
	isLoopbackBindHost,
	parseCidrAllowlist,
	parseIpAddress,
	sourceIpAllowed,
} from "./index.js";

describe("allowlist parsing (ip_network(strict=False) parity)", () => {
	it("parses CSV + list forms; host bits are CLEARED", () => {
		const csv = parseCidrAllowlist("10.0.0.0/8, 2001:db8::/32");
		expect(csv.networks).toHaveLength(2);
		expect(csv.invalid).toEqual([]);

		const hostBitsSet = parseCidrAllowlist(["10.1.2.3/8"]);
		expect(hostBitsSet.networks[0]?.prefixBits).toBe(8);
		const net = hostBitsSet.networks[0];
		expect(net !== undefined && ipInNetworks("10.255.255.255", [net])).toBe(
			true,
		);
	});

	it("invalid entries SKIP with a warning collected — never throw", () => {
		const parsed = parseCidrAllowlist([
			"10.0.0.0/8",
			"not-a-cidr",
			"1.2.3.4/99",
			"",
		]);
		expect(parsed.networks).toHaveLength(1);
		expect(parsed.invalid).toEqual(["not-a-cidr", "1.2.3.4/99"]);
	});

	it("bare IP entries mean /32|/128; v4-in-v6 and zone ids parse", () => {
		const bare = parseCidrAllowlist(["203.0.113.9"]);
		expect(bare.networks).toHaveLength(1);
		expect(ipInNetworks("203.0.113.9", bare.networks)).toBe(true);
		expect(ipInNetworks("203.0.113.10", bare.networks)).toBe(false);

		const mapped = parseIpAddress("::ffff:192.0.2.128");
		expect(mapped).toEqual({ family: 6, value: 0xffffc0000280n });
	});
});

describe("per-request gating (_source_ip_allowed parity)", () => {
	const allow = parseCidrAllowlist(["10.0.0.0/8", "2001:db8::/32"]).networks;

	it("in-range admitted (v4 and v6)", () => {
		expect(
			sourceIpAllowed({ remoteAddr: "10.9.9.9" }, "127.0.0.1", allow),
		).toBe(true);
		expect(
			sourceIpAllowed({ remoteAddr: "2001:db8::1" }, "127.0.0.1", allow),
		).toBe(true);
	});

	it("out-of-range denied; unparseable peer fails CLOSED", () => {
		expect(
			sourceIpAllowed({ remoteAddr: "203.0.113.5" }, "127.0.0.1", allow),
		).toBe(false);
		expect(
			sourceIpAllowed({ remoteAddr: "11.0.0.1" }, "127.0.0.1", allow),
		).toBe(false);
		expect(
			sourceIpAllowed({ remoteAddr: "not-an-ip" }, "127.0.0.1", allow),
		).toBe(false);
	});

	it("XFF SPOOF handled per spec: forwarded headers CANNOT influence the verdict", () => {
		// The gate's request surface carries ONLY the socket peer
		// (`request.remote`); there is no field an XFF header could populate.
		// An attacker behind an out-of-range peer claiming an allowlisted XFF:
		const spoofedPeer = { remoteAddr: "203.0.113.5" };
		expect(sourceIpAllowed(spoofedPeer, "127.0.0.1", allow)).toBe(false);
		// Junk XFF cannot evict a genuinely in-range peer either.
		expect(
			sourceIpAllowed({ remoteAddr: "10.1.1.1" }, "127.0.0.1", allow),
		).toBe(true);
	});

	it("family mismatch denies (v4 range never admits v6 peer)", () => {
		expect(ipInNetworks("::ffff:10.1.1.1", allow)).toBe(false);
	});
});

describe("connect refusal (_source_allowlist_required_but_missing)", () => {
	it("non-loopback / unset binds REQUIRE CIDRs; loopback binds may omit", () => {
		for (const publicHost of [
			null,
			undefined,
			"",
			"0.0.0.0",
			"::",
			"192.168.1.5",
		]) {
			expect(allowlistRequiredButMissing(publicHost, [])).toBe(true);
		}
		for (const localHost of [
			"127.0.0.1",
			"localhost",
			"::1",
			"ip6-localhost",
		]) {
			expect(isLoopbackBindHost(localHost)).toBe(true);
			expect(allowlistRequiredButMissing(localHost, [])).toBe(false);
		}
		// Any configured network satisfies the requirement regardless of bind.
		const any = parseCidrAllowlist(["10.0.0.0/8"]).networks;
		expect(allowlistRequiredButMissing(null, any)).toBe(false);
		// Required-but-missing also fails every request closed (defense in
		// depth if connect refusal was bypassed).
		expect(sourceIpAllowed({ remoteAddr: "10.0.0.1" }, null, [])).toBe(false);
	});
});
