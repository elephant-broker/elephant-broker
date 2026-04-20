# openclaw-plugins/shared

Cross-plugin helpers imported by both ElephantBroker plugins
(`elephantbroker-context/` and `elephantbroker-memory/`) via **relative path**:

```ts
import { stripOpenClawEnvelope } from "../../shared/envelope";
```

## Scope

One module today: `envelope.ts` — the canonical OpenClaw envelope stripper
(see `envelope.ts` header for the extraction contract and the
5-102 RC-A hardening). The module exists so both plugins cannot drift: pre-
extraction the two plugins carried byte-identical copies of this helper
(PR #5 TODOs 5-004 / 5-104 / 5-206), and a regex fix on one copy would
silently skip the other.

## Packaging constraints (5-315)

**This directory is not a publishable package.** It has no `package.json`,
no build step, and no version. Imports work by sibling-directory coincidence
inside the monorepo layout:

```
openclaw-plugins/
  shared/                       ← this directory
    envelope.ts
  elephantbroker-memory/
    src/format.ts               ← imports `../../shared/envelope`
  elephantbroker-context/
    src/engine.ts               ← imports `../../shared/envelope`
```

If either plugin is ever published independently to npm (OpenClaw plugin
registry, private registry, etc.), the relative import will break because
`shared/` is outside the plugin's package root and will not be bundled.

**Before any standalone publish, extract `shared/` into a real package** —
options:

1. **Preferred:** turn `shared/` into a proper npm package
   (`@elephantbroker/openclaw-shared` or similar) with its own `package.json`,
   build output, and version; add it as a dependency of both plugins.
2. **Alternative:** inline `envelope.ts` back into each plugin at publish
   time via a build step (bundler, rollup plugin, or manual copy script)
   — this reintroduces the drift hazard `shared/` was created to solve,
   so prefer option 1.

For now (monorepo-only, not independently published) the relative-path
import is fine and intentional.

## Change protocol

Any change to `envelope.ts` must keep the two plugins in lockstep — that's
the whole reason the module exists. Run `envelope.test.ts` on every edit;
both plugin-level test suites also import and re-exercise the helper
indirectly through their own `stripOpenClawEnvelope` re-exports.
