// pi_gateway/resolution/single-flight.ts — in-flight promise dedup
// (02-session-and-state.md §4.1, port of the _SessionFlight shape from
// gateway/session.py:SessionStore.get_or_create_session /
// gateway/session.py:_SessionFlight).
//
// Overlapping calls for one key share ONE in-flight slot: the owner runs the
// body, waiters share its result OR error; different keys stay concurrent.
// The flight stays registered until the owner settles, so callers arriving
// mid-body join instead of racing a second execution. Also used by the §9
// adopt-before-mint registry (concurrent clicks share the creation promise).

interface Flight {
	promise: Promise<unknown>;
	resolve: (value: unknown) => void;
	reject: (err: unknown) => void;
}

export class SingleFlightMap {
	private flights = new Map<string, Flight>();

	/**
	 * Run `body` under single-flight semantics for `key`. Concurrent callers
	 * with the same key receive (the same settled instance of) the owner's
	 * promise; callers for other keys are never blocked.
	 */
	run<T>(key: string, body: () => Promise<T>): Promise<T> {
		const existing = this.flights.get(key);
		if (existing) return existing.promise as Promise<T>;

		let resolve!: (value: unknown) => void;
		let reject!: (err: unknown) => void;
		const promise = new Promise<unknown>((res, rej) => {
			resolve = res;
			reject = rej;
		});
		const flight: Flight = { promise, resolve, reject };
		this.flights.set(key, flight);

		void (async () => {
			try {
				flight.resolve(await body());
			} catch (err) {
				flight.reject(err);
			} finally {
				// Pop only after settling: waiters arriving during the body must
				// find the slot (parity of slot.event.set() then map pop).
				if (this.flights.get(key) === flight) this.flights.delete(key);
			}
		})();
		return promise as Promise<T>;
	}

	/** Diagnostics/tests. */
	inFlightKeys(): string[] {
		return [...this.flights.keys()];
	}
}
