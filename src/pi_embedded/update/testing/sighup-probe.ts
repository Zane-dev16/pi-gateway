// TEST INFRASTRUCTURE — SIGHUP disposition probe (host-honest Linux).
//
// The child kills ITSELF with SIGHUP and reports survival:
//   - inherited SIG_IGN (the DEC-042(b) trap-wrapped exec chain) ⇒ survives,
//     prints ALIVE, exits 0;
//   - default disposition ⇒ terminated by SIGHUP (close event: signal).
// No listener is installed — the probe measures the INHERITED kernel
// disposition, which is exactly the property POSIX preserves across exec.

process.kill(process.pid, "SIGHUP");
setTimeout(() => {
	console.log("ALIVE");
	process.exit(0);
}, 250);
