# Diagram sources

The README embeds SVG files generated from these Mermaid sources:

- `packet-to-report.mmd` shows the router and its per-instrument command and event queues.
- `journaled-engine.mmd` shows the separate engine facade and persistence worker.
- `new-order-transaction.mmd` shows gateway application after admission.
- `recovery-lifecycle.mmd` shows shutdown, snapshot publication, and restart.

From the repository root, run with Node.js, npm, and PowerShell installed:

```text
pwsh -File scripts/diagrams/render.ps1
```

The script pins Mermaid CLI 11.17.0 and uses `mermaid-config.json`. The first
run may download the renderer and its headless browser. To use an existing
Chrome installation, set `PUPPETEER_EXECUTABLE_PATH` and
`PUPPETEER_SKIP_DOWNLOAD=true` before running it.

Review labels and arrows against the implementation, render every changed
source, and inspect the SVGs before committing. Keep implementation gaps in
the captions. Do not draw an unimplemented connection as a shipped path.
