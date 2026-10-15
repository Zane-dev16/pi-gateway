// extensions hint contract (Owner B): /gateway setup no longer runs chat
// setup. It prints a moved-to-CLI hint and does nothing else. Status keeps
// its not-running line. Drives the registered command the way pi does and
// asserts the literal operator-visible lines.

import { describe, expect, it } from "vitest";
import registerGateway from "../../extensions/pi-gateway.js";

const SETUP_HINT =
	"gateway setup moved to the terminal: run `pi-gateway setup` (try `pi-gateway setup --list` first)";

interface CapturedCommand {
	handler: (
		args: string,
		ctx: { ui: { notify: (message: string, type?: string) => void } },
	) => Promise<void>;
}

function gatewayCommand(): CapturedCommand {
	const commands = new Map<string, CapturedCommand>();
	const pi = {
		on: () => {},
		registerCommand: (name: string, def: CapturedCommand) => {
			commands.set(name, def);
		},
	};
	(registerGateway as (api: unknown) => void)(pi);
	const cmd = commands.get("gateway");
	if (cmd === undefined) throw new Error("gateway command not registered");
	return cmd;
}

describe("/gateway setup removal", () => {
	it("setup prints the moved-to-CLI hint and touches nothing else", async () => {
		const cmd = gatewayCommand();
		const notes: string[] = [];
		await cmd.handler("setup", {
			ui: {
				notify: (message: string) => {
					notes.push(message);
				},
			},
		});
		expect(notes).toEqual([SETUP_HINT]);
	});

	it("setup with a platform arg prints the same hint", async () => {
		const cmd = gatewayCommand();
		const notes: string[] = [];
		await cmd.handler("setup telegram", {
			ui: {
				notify: (message: string) => {
					notes.push(message);
				},
			},
		});
		expect(notes).toEqual([SETUP_HINT]);
	});

	it("bare /gateway still reports status without starting anything", async () => {
		const cmd = gatewayCommand();
		const notes: string[] = [];
		await cmd.handler("", {
			ui: {
				notify: (message: string) => {
					notes.push(message);
				},
			},
		});
		expect(notes).toEqual([
			"gateway: not running — /gateway start [home] to start",
		]);
	});
});
