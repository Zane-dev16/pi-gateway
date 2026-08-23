// pi_agent_core/checkpoints.ts — per-turn checkpoint snapshot ledger.
//
// Port of the loop's `checkpoint.new_turn()` discipline (05 §4 pseudocode):
// checkpoint snapshots are deduplicated WITHIN one turn and the dedup state
// resets at every turn boundary. The runner records per-iteration progress
// snapshots here so downstream consumers (stream plumbing, conformance
// harnesses in later phases) observe exactly-once emissions per turn.

export interface TurnCheckpointLedgerCounts {
	recorded: number;
	duplicates: number;
	/** Distinct payloads seen in the CURRENT turn. */
	distinct: number;
	turn: number;
}

export class TurnCheckpointLedger {
	private seen = new Set<string>();
	private recorded = 0;
	private duplicates = 0;
	private turn = 0;

	/** Reset per-turn dedup state (loop top: `checkpoint.new_turn()`). */
	newTurn(): void {
		this.seen.clear();
		this.turn += 1;
	}

	/**
	 * Record a payload identity for the current turn. True when it is NEW for
	 * this turn; false when it duplicates something already recorded.
	 */
	record(payloadKey: string): boolean {
		if (this.seen.has(payloadKey)) {
			this.duplicates += 1;
			return false;
		}
		this.seen.add(payloadKey);
		this.recorded += 1;
		return true;
	}

	counts(): TurnCheckpointLedgerCounts {
		return {
			recorded: this.recorded,
			duplicates: this.duplicates,
			distinct: this.seen.size,
			turn: this.turn,
		};
	}
}
