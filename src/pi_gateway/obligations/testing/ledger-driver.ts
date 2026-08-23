// TEST INFRASTRUCTURE — standalone driver for two-process obligation
// contracts. Run under RAW Node with the pi_state/testing/node-ts-resolve.mjs
// hook (maps ".js" specifiers to ".ts"). Subcommands communicate via single
// `RESULT_JSON {...}` lines on stdout.
//
//   record-stuck <dbPath>            — record an obligation + beginAttempt,
//                                      then EXIT WITHOUT SETTLING: models a
//                                      gateway crash mid-send.
//   recover <dbPath> <platform>...   — one boot's sweep_recoverable +
//                                      driveClaimed through a journalling
//                                      sender; reports claims, sends, states.

import { StateStore } from "../../../pi_state/index.js";
import { DeliveryLedger } from "../index.js";
import type { DeliveryRequest } from "../sender.js";

function emit(result: unknown): void {
	process.stdout.write(`RESULT_JSON ${JSON.stringify(result)}\n`, () => {
		process.exit(0);
	});
}

async function main(): Promise<void> {
	const [, , command, dbPath, ...rest] = process.argv;
	if (!command || !dbPath) {
		emit({
			error:
				"usage: ledger-driver.ts <record-stuck|recover> <db> [platforms...]",
		});
		return;
	}
	const store = await StateStore.open(dbPath);

	if (command === "record-stuck") {
		const ledger = new DeliveryLedger(store.db);
		const id = await ledger.record({
			sessionKey: "telegram|chat|42",
			platform: "telegram",
			chatId: "42",
			threadId: null,
			content: "crash-surviving answer",
			messageRef: "msg-crash",
		});
		await ledger.beginAttempt(id);
		await store.close(false); // skip token drain; we are simulating a crash
		emit({ obligationId: id });
		return;
	}

	if (command === "recover") {
		const platforms = new Set(rest.length > 0 ? rest : ["telegram"]);
		const ledger = new DeliveryLedger(store.db);
		const sends: Array<
			Pick<
				DeliveryRequest,
				"obligationId" | "content" | "needsMarker" | "attempts"
			>
		> = [];
		const claimed = await ledger.sweepRecoverable({
			deliverablePlatforms: platforms,
		});
		const results = await ledger.driveClaimed(claimed, async (req) => {
			sends.push({
				obligationId: req.obligationId,
				content: req.content,
				needsMarker: req.needsMarker,
				attempts: req.attempts,
			});
			return { ok: true };
		});
		const states: Record<string, string | null> = {};
		for (const c of claimed)
			states[c.obligationId] = ledger.stateOf(c.obligationId);
		await store.close();
		emit({ claimedCount: claimed.length, sends, results, states });
		return;
	}

	emit({ error: `unknown command ${command}` });
}

main().catch((err) => {
	emit({ error: String(err) });
});
