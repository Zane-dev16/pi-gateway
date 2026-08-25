// BEHAVIOR CONTRACTS — stability round cluster `a2a-fixes-r2` (a2a-1/2/5).
// Every test pins CONFORMING behavior against the READ-ONLY Hermes reference
// (plugins/platforms/a2a/*), never the pre-fix port bugs:
//
//   a2a-1  security.py:sign_push_payload signs json.dumps(payload,
//          sort_keys=True, ensure_ascii=False) bytes — Python DEFAULT
//          separators (', ' between items, ': ' after keys) — so receivers
//          that re-canonicalize per the documented sorted-keys convention
//          accept X-A2A-Signature. The compact JS stringify broke them.
//   a2a-2  protocol.py:build_agent_card provider block reads
//          A2A_PROVIDER_ORG / A2A_PROVIDER_URL with os.getenv semantics
//          (unset ORG ⇒ 'Hermes Agent'; unset/empty URL ⇒ the card url).
//   a2a-5  adapter.py:__init__/_load_served_agents/_advertised_skillsets
//          lanes: A2A_AGENT_DESCRIPTION root-description default,
//          A2A_ADVERTISED_TOOLSETS csv fallback (configured list wins), and
//          the global-config a2a_served_agents served-agent ladder with
//          PYTHON `or` fall-through ([]/{} are falsy operands).

import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";

import { makeA2aFixture } from "./a2a-fixture.js";
import {
	AGENT_CARD_PATH,
	A2A_PLUGIN_MANIFEST,
	ENV_ADVERTISED_TOOLSETS,
	ENV_AGENT_DESCRIPTION,
	ENV_PROVIDER_ORG,
	ENV_PROVIDER_URL,
	ROLE_AGENT,
	STATE_COMPLETED,
} from "./manifest.js";
import { signPushPayload, sortKeysJson } from "./security.js";

/** The reference's hardcoded default (adapter.py:_load_served_agents). */
const HERMES_DEFAULT_DESCRIPTION =
	"Hermes Agent — a general-purpose agent reachable over A2A.";

function cardOf(fx: ReturnType<typeof makeA2aFixture>, path = AGENT_CARD_PATH) {
	const resp = fx.get(path);
	expect(resp.status).toBe(200);
	return resp.json;
}

function skillsIds(card: Record<string, unknown>): string[] {
	return (card["skills"] as Array<Record<string, unknown>>).map((s) =>
		String(s["id"]),
	);
}

describe("a2a-1: push-signature canonicalization (security.py:sign_push_payload)", () => {
	it("sortKeysJson emits json.dumps(sort_keys=True, ensure_ascii=False) bytes — Python DEFAULT separators", () => {
		// The compact form '{"a":1,"b":2}' is exactly what broke receivers;
		// Python's default dump puts ', ' between items and ': ' after keys.
		expect(sortKeysJson({ b: 1, a: "x" })).toBe('{"a": "x", "b": 1}');
		// Deep sort + nested containers + empty containers.
		expect(sortKeysJson({ z: { b: [1, 2], a: null }, a: [] })).toBe(
			'{"a": [], "z": {"a": null, "b": [1, 2]}}',
		);
		expect(sortKeysJson({ t: true, f: false, e: "" })).toBe(
			'{"e": "", "f": false, "t": true}',
		);
		// ensure_ascii=False: non-ASCII stays LITERAL (no \\u escapes).
		expect(sortKeysJson({ k: "héllo → 世界" })).toBe('{"k": "héllo → 世界"}');
		// String escaping matches the Python ESCAPE map (short forms + \\u00xx).
		expect(sortKeysJson({ s: 'a"b\\c\nd\te\u0001' })).toBe(
			'{"s": "a\\"b\\\\c\\nd\\te\\u0001"}',
		);
	});

	it("signPushPayload equals an INDEPENDENT receiver-side HMAC over hand-written canonical bytes", () => {
		const secret = "s3cret";
		const payload = {
			taskId: "task-1",
			contextId: "ctx-1",
			nested: { z: 1, a: [true, null] },
		};
		// Literal expected body — key order and separators pinned BY HAND,
		// independent of sortKeysJson.
		const literal =
			'{"contextId": "ctx-1", ' +
			'"nested": {"a": [true, null], "z": 1}, ' +
			'"taskId": "task-1"}';
		const expected = createHmac("sha256", secret)
			.update(literal, "utf8")
			.digest("hex");
		expect(signPushPayload(payload, secret)).toBe(expected);
		// Unsigned mode preserved (empty secret ⇒ '').
		expect(signPushPayload(payload, "")).toBe("");
	});

	it("end-to-end push: X-A2A-Signature verifies over the literal Python-canonical form of the delivered body", async () => {
		const fx = makeA2aFixture({
			env: { A2A_PUSH_SECRET: "push-sec-r2" },
		});
		try {
			const params = fx.sendParams("notify me", {
				contextId: "ctx-canonical",
			});
			params["configuration"] = {
				taskPushNotificationConfig: { url: "https://peer.example/hook" },
			};
			const inFlight = fx.postRpc({
				method: "SendMessage",
				id: "req-sig-e2e",
				params,
			});
			await fx.scheduler.runToEnd();
			await inFlight;

			expect(fx.push.calls.length).toBe(1);
			const call = fx.push.calls[0] as {
				url: string;
				body: string;
				headers: Record<string, string>;
			};
			const body = JSON.parse(call.body) as Record<string, unknown>;
			const update = body["statusUpdate"] as Record<string, unknown>;
			const status = update["status"] as Record<string, unknown>;
			const message = status["message"] as Record<string, unknown>;
			const part = ((message["parts"] as Array<Record<string, unknown>>)[0] ??
				{}) as Record<string, unknown>;

			// Assemble the Python-canonical bytes BY HAND from the received
			// fields: every level sorted (contextId < status < taskId;
			// message < state < timestamp; contextId < messageId < parts <
			// role; mediaType < text), ', '/': ' separators throughout.
			const q = JSON.stringify;
			const expectedBody =
				`{"statusUpdate": {"contextId": ${q(update["contextId"])}, ` +
				`"status": {"message": {"contextId": ${q(message["contextId"])}, ` +
				`"messageId": ${q(message["messageId"])}, ` +
				`"parts": [{"mediaType": ${q(part["mediaType"])}, "text": ${q(part["text"])}}], ` +
				`"role": ${q(ROLE_AGENT)}}, ` +
				`"state": ${q(status["state"])}, "timestamp": ${q(status["timestamp"])}}, ` +
				`"taskId": ${q(update["taskId"])}}}`;

			expect(status["state"]).toBe(STATE_COMPLETED);
			expect(
				createHmac("sha256", "push-sec-r2")
					.update(expectedBody, "utf8")
					.digest("hex"),
			).toBe(call.headers["X-A2A-Signature"]);
		} finally {
			fx.dispose();
		}
	});
});

describe("a2a-2: Agent Card provider env lanes (protocol.py:build_agent_card)", () => {
	it("A2A_PROVIDER_ORG / A2A_PROVIDER_URL override the provider block", () => {
		const fx = makeA2aFixture({
			env: {
				[ENV_PROVIDER_ORG]: "Acme Agents",
				[ENV_PROVIDER_URL]: "https://acme.example/provider",
			},
		});
		try {
			const provider = cardOf(fx)["provider"] as Record<string, unknown>;
			expect(provider["organization"]).toBe("Acme Agents");
			expect(provider["url"]).toBe("https://acme.example/provider");
		} finally {
			fx.dispose();
		}
	});

	it("UNset env keeps the getenv defaults: organization 'Hermes Agent', url = card url", () => {
		const fx = makeA2aFixture({});
		try {
			const card = cardOf(fx);
			const provider = card["provider"] as Record<string, unknown>;
			expect(provider["organization"]).toBe("Hermes Agent");
			expect(provider["url"]).toBe(card["url"]); // convenience top-level url
		} finally {
			fx.dispose();
		}
	});

	it("ORG-only: provider.url falls back to the card url; SET-but-empty ORG passes through as '' (getenv semantics)", () => {
		const orgOnly = makeA2aFixture({
			env: { [ENV_PROVIDER_ORG]: "Org Only Inc" },
		});
		try {
			const provider = cardOf(orgOnly)["provider"] as Record<string, unknown>;
			expect(provider["organization"]).toBe("Org Only Inc");
			expect(provider["url"]).toBe("http://127.0.0.1:9900/");
		} finally {
			orgOnly.dispose();
		}
		const emptyOrg = makeA2aFixture({ env: { [ENV_PROVIDER_ORG]: "" } });
		try {
			// os.getenv returns '' when the var is SET-but-empty — the default
			// applies only when UNset.
			const provider = cardOf(emptyOrg)["provider"] as Record<string, unknown>;
			expect(provider["organization"]).toBe("");
		} finally {
			emptyOrg.dispose();
		}
	});

	it("both provider vars are declared manifest optionalEnv rows", () => {
		const names = (A2A_PLUGIN_MANIFEST.optionalEnv ?? []).map((e) => e.name);
		expect(names).toContain(ENV_PROVIDER_ORG);
		expect(names).toContain(ENV_PROVIDER_URL);
	});
});

describe("a2a-5: description / toolset / served-agent config lanes", () => {
	it("A2A_AGENT_DESCRIPTION sets the ROOT agent card description", () => {
		const fx = makeA2aFixture({
			env: { [ENV_AGENT_DESCRIPTION]: "Custom described agent." },
		});
		try {
			expect(cardOf(fx)["description"]).toBe("Custom described agent.");
		} finally {
			fx.dispose();
		}
	});

	it("UNset env falls back to the reference default description", () => {
		const fx = makeA2aFixture({});
		try {
			expect(cardOf(fx)["description"]).toBe(HERMES_DEFAULT_DESCRIPTION);
		} finally {
			fx.dispose();
		}
	});

	it("SET-but-empty A2A_AGENT_DESCRIPTION degrades to the reference default on the CARD (falsy-description guard)", () => {
		// adapter.py:_build_card: `description=agent.get("description") or
		// "Hermes Agent — ..."` — an empty stored description cannot ship.
		const fx = makeA2aFixture({ env: { [ENV_AGENT_DESCRIPTION]: "" } });
		try {
			expect(cardOf(fx)["description"]).toBe(HERMES_DEFAULT_DESCRIPTION);
		} finally {
			fx.dispose();
		}
	});

	it("A2A_ADVERTISED_TOOLSETS csv feeds card skills when configured toolsets are EMPTY", () => {
		const fx = makeA2aFixture({
			env: { [ENV_ADVERTISED_TOOLSETS]: "coding, web ,," },
		});
		try {
			expect(skillsIds(cardOf(fx))).toEqual(["toolset.coding", "toolset.web"]);
		} finally {
			fx.dispose();
		}
	});

	it("configured advertised_toolsets WIN over the env csv; neither lane ⇒ the general skill", () => {
		const configured = makeA2aFixture({
			env: { [ENV_ADVERTISED_TOOLSETS]: "coding,web" },
			config: { advertised_toolsets: ["shell"] },
		});
		try {
			expect(skillsIds(cardOf(configured))).toEqual(["toolset.shell"]);
		} finally {
			configured.dispose();
		}
		const neither = makeA2aFixture({});
		try {
			expect(skillsIds(cardOf(neither))).toEqual(["general"]);
		} finally {
			neither.dispose();
		}
	});

	it("global-config a2a_served_agents serves extra agents when extra lanes are absent", () => {
		const fx = makeA2aFixture({
			globalConfig: {
				a2a_served_agents: {
					researcher: { name: "Global Researcher", tenant: "res-global" },
				},
			},
		});
		try {
			const rc = cardOf(fx, "/researcher/.well-known/agent-card.json");
			expect(rc["name"]).toBe("Global Researcher");
			const iface = (
				rc["supportedInterfaces"] as Array<Record<string, unknown>>
			)[0] as Record<string, unknown>;
			expect(iface["tenant"]).toBe("res-global");
			// Health topology shows root + the globally-configured agent.
			const health = fx.get("/health");
			expect(
				health.json["served_agents"] as Array<Record<string, unknown>>,
			).toHaveLength(2);
		} finally {
			fx.dispose();
		}
	});

	it("nested cfg.a2a.served_agents lane works; array-shaped entries accepted", () => {
		const fx = makeA2aFixture({
			globalConfig: {
				a2a: {
					served_agents: [
						{ slug: "writer", name: "Nested Writer", local: true },
					],
				},
			},
		});
		try {
			expect(cardOf(fx, "/writer/.well-known/agent-card.json")["name"]).toBe(
				"Nested Writer",
			);
		} finally {
			fx.dispose();
		}
	});

	it("extra lanes beat global lanes; an EXTRA-lane falsy operand ([]/{}) FALLS THROUGH to global (Python `or`)", () => {
		const winner = makeA2aFixture({
			config: { agents: { researcher: { name: "Extra Researcher" } } },
			globalConfig: {
				a2a_served_agents: { researcher: { name: "Global Researcher" } },
			},
		});
		try {
			expect(
				cardOf(winner, "/researcher/.well-known/agent-card.json")["name"],
			).toBe("Extra Researcher");
		} finally {
			winner.dispose();
		}

		const fallthrough = makeA2aFixture({
			config: { agents: {} },
			globalConfig: {
				a2a_served_agents: { analyst: { name: "Fallback Analyst" } },
			},
		});
		try {
			expect(
				cardOf(fallthrough, "/analyst/.well-known/agent-card.json")["name"],
			).toBe("Fallback Analyst");
		} finally {
			fallthrough.dispose();
		}
	});

	it("the extra.served_agents alias lane resolves (lower priority than agents)", () => {
		const fx = makeA2aFixture({
			config: { served_agents: [{ slug: "beta", name: "Alias Beta" }] },
		});
		try {
			expect(cardOf(fx, "/beta/.well-known/agent-card.json")["name"]).toBe(
				"Alias Beta",
			);
		} finally {
			fx.dispose();
		}
	});
});
