# OpenClaw-Side Configuration for ElephantBroker

This document describes the configuration changes required on the OpenClaw side to integrate with ElephantBroker. It covers plugin installation, tool definitions, agent instructions, environment variables, and session lifecycle expectations.

---

## Plugin Installation

### Directory Structure

Both plugins use OpenClaw's flat extension layout — all `.ts` files at root. OpenClaw loads `.ts` files directly via `jiti` (no build step needed). A `src/` directory exists as a legacy copy; OpenClaw uses the root `.ts` files.

```
~/.openclaw/extensions/
  elephantbroker-memory/
    openclaw.plugin.json     # manifest with id, kind, configSchema
    package.json             # must have "openclaw": { "extensions": ["./index.ts"] }
    index.ts                 # entry point — register(api) function
    client.ts                # HTTP client for ElephantBroker runtime API
    format.ts                # memory context formatting
    types.ts                 # TypeScript type definitions
    tools/                   # 24 tool definitions
      memory_search.ts
      memory_get.ts
      ...
  elephantbroker-context/
    openclaw.plugin.json
    package.json
    index.ts
    client.ts
    engine.ts
    types.ts
```

### Installation Steps

> **Deployment mode:** Installing **both** plugins (`elephantbroker-memory` + `elephantbroker-context`) configures **FULL mode** — the recommended operating mode for ~90% of deployments. FULL mode enables the complete stack: durable memory (Neo4j + Qdrant-backed), working set scoring (11-dimension), context assembly, compaction, and guards. Installing only `elephantbroker-memory` puts the runtime in MEMORY_ONLY mode (memory storage without context lifecycle). For all standard deployments, install both.

**Prerequisite:** Node.js **24+** (pinned via `engines.node` in each plugin's
`package.json`). Earlier versions (20, 22) may run but are not supported by
the lockfiles committed in the repo.

**1. Clone repo and symlink plugins into OpenClaw extensions:**

```bash
# Clone the repo on the gateway host
git clone https://github.com/elephant-broker/elephant-broker.git /opt/elephantbroker

# Symlink plugins — OpenClaw loads .ts files directly via jiti (no build step)
ln -s /opt/elephantbroker/openclaw-plugins/elephantbroker-memory ~/.openclaw/extensions/elephantbroker-memory
ln -s /opt/elephantbroker/openclaw-plugins/elephantbroker-context ~/.openclaw/extensions/elephantbroker-context

# Install dependencies — `npm ci` reads the committed package-lock.json and
# installs EXACTLY those versions. Errors out if the lockfile is missing or
# out of sync with package.json. This is the npm equivalent of
# `uv sync --frozen` on the DB VM and the only supported install command
# for production gateways.
cd ~/.openclaw/extensions/elephantbroker-memory && npm ci
cd ~/.openclaw/extensions/elephantbroker-context && npm ci
```

> **Why `npm ci` and not `npm install`:** `npm install` resolves package.json
> ranges to whatever's latest today, regenerates the lockfile if needed, and
> can silently install different versions on different hosts. `npm ci` is
> bit-for-bit reproducible — it reads the committed `package-lock.json` and
> installs exactly the same tree every time. Use `npm install` only when
> intentionally bumping a dep (and commit the regenerated lockfile).

Symlinks make updates easy — `git pull` in `/opt/elephantbroker` updates both
plugins. After pulling, re-run `npm ci` in each plugin directory to pick up
any lockfile changes.

**2. Add to `~/.openclaw/openclaw.json`:**

```json
{
  "env": {
    "vars": {
      "EB_GATEWAY_ID": "gw-prod",
      "EB_RUNTIME_URL": "http://DB_VM_IP:8420",
      "EB_GATEWAY_SHORT_NAME": "prod"
    }
  },
  "plugins": {
    "allow": ["elephantbroker-memory", "elephantbroker-context"],
    "slots": {
      "memory": "elephantbroker-memory",
      "contextEngine": "elephantbroker-context"
    },
    "entries": {
      "elephantbroker-memory": {
        "enabled": true,
        "config": {
          "baseUrl": "${EB_RUNTIME_URL}",
          "gatewayId": "${EB_GATEWAY_ID}",
          "gatewayShortName": "${EB_GATEWAY_SHORT_NAME}",
          "profileName": "coding"
        }
      },
      "elephantbroker-context": {
        "enabled": true,
        "config": {
          "baseUrl": "${EB_RUNTIME_URL}",
          "gatewayId": "${EB_GATEWAY_ID}",
          "gatewayShortName": "${EB_GATEWAY_SHORT_NAME}",
          "profileName": "coding"
        }
      }
    }
  }
}
```

`${VAR}` values are interpolated from `env.vars` at config load time. Available profiles: `coding`, `research`, `managerial`, `worker`, `personal_assistant`.

**3. Verify:**

```bash
openclaw plugins list          # should show both plugins
openclaw gateway start         # should load without errors
```

### Common Installation Issues

| Error | Cause | Fix |
|-------|-------|-----|
| `plugin manifest not found: .../dist/openclaw.plugin.json` | `dist/` or `src/` subdirectories confuse the scanner | Delete `dist/` and `src/` — use flat layout with `.ts` at root |
| `plugin manifest requires configSchema` | Missing `configSchema` in `openclaw.plugin.json` | Add `configSchema` JSON Schema object to manifest |
| `plugin id mismatch` | Directory name doesn't match manifest `id` | Rename directory to match `id` field (e.g. `elephantbroker-memory`) |
| `package.json missing openclaw.extensions` | Missing `openclaw` field in `package.json` | Add `"openclaw": { "extensions": ["./index.ts"] }` |
| `Cannot find module './tools/memory_search.js'` | Source files missing from root | Ensure `tools/` directory and all `.ts` files are at plugin root, not inside `src/` |

### Tools Profile Requirement

ElephantBroker registers 24 tools. The default `coding` tools profile only exposes `memory_search` and `memory_get` — blocking the other 22 tools (including `memory_store`, goal management, procedures, guards, artifacts, and admin tools).

**Required:** Set `tools.profile` to `full` or unset it entirely (defaults to `full`):

```bash
openclaw config unset tools.profile
# OR
openclaw config set tools.profile full
```

**Available profiles:**

| Profile | EB Compatible | Notes |
|---------|--------------|-------|
| `full` | Yes | All core + plugin tools exposed |
| `coding` | No | Only exposes `memory_search` + `memory_get` from memory group |
| `messaging` | No | Messaging tools only |
| `minimal` | No | session_status only |

### Gateway Configuration Commands

After editing `openclaw.json` (Step 2), run these commands to complete gateway setup:

```bash
# Register memory plugin in the memory slot
openclaw config set plugins.slots.memory elephantbroker-memory

# Set tools profile to full (CRITICAL — see above)
openclaw config unset tools.profile

# Disable OpenClaw's built-in session-memory hook (EB replaces it)
openclaw hooks disable session-memory

# Restart gateway to apply all changes
openclaw gateway restart
```

**Note:** `memorySearch.enabled` does NOT need to be set — it is an internal OpenClaw flag that has no effect on plugin-registered tools. Setting it was a red herring during initial debugging.

---

## Phase 5: Working Set Manager + Session Goals + Procedure Tools

Phase 5 adds 9 new tools to the MemoryPlugin (5 session goal management + 4 procedure lifecycle). These tools require updates to OpenClaw's TOOLS.MD and AGENTS.MD.

### New Tools (9 total)

#### Session Goals (5 tools)

| Tool | Description | Parameters |
|------|-------------|------------|
| `session_goals_list` | View the full goal tree with IDs, status, blockers, sub-goals, confidence | None |
| `session_goals_create` | Create a session goal or sub-task | `title` (required), `description`, `parent_goal_id`, `success_criteria[]` |
| `session_goals_update_status` | Complete, pause, or abandon a goal | `goal_id` (required), `status` (required: completed/paused/abandoned), `evidence` |
| `session_goals_add_blocker` | Report an obstacle on a goal | `goal_id` (required), `blocker` (required) |
| `session_goals_progress` | Record meaningful progress | `goal_id` (required), `evidence` (required) |

#### Procedures (4 tools)

| Tool | Description | Parameters |
|------|-------------|------------|
| `procedure_create` | Define a new procedure with steps and proof requirements | `name` (required), `description`, `scope`, `steps[]` with optional `proof_type`/`proof_description` |
| `procedure_activate` | Start following a procedure (creates tracked execution) | `procedure_id` (required) |
| `procedure_complete_step` | Mark a procedure step as complete with optional proof | `execution_id` (required), `step_id` (required), `proof_value` (optional) |
| `procedure_session_status` | View all procedures tracked in this session | — (uses session context) |

---

## TOOLS.MD Additions

Add the following to the agent's TOOLS.MD (or equivalent tool documentation file):

```markdown
## Session Goal Management

You have a suite of goal management tools that let you plan, organize, and track work.
**You are the goal authority** — only you decide what the goals are. The system
automatically tracks progress, blockers, and refinements from your conversation,
but it never creates root goals on its own.

### Available Tools

**`session_goals_list`** — View the full goal tree
  Always call this first before creating goals to avoid duplicates.
  Returns all goals with their IDs (needed for other tools), status, blockers, and sub-goals.

**`session_goals_create`** — Create a goal or sub-task
  You set the direction. Break complex work into sub-goals using parent_goal_id
  (get the ID from session_goals_list). Each sub-goal gets tracked and scored independently.
  Example: Create "Fix JWT token expiry" as a sub-goal of "Fix login bug"

**`session_goals_update_status`** — Mark goal as completed, paused, or abandoned
  Always provide evidence when completing ("tests pass", "deployed to staging").
  Use "paused" to switch context with intent to return. Use "abandoned" when dropped.

**`session_goals_add_blocker`** — Report a blocker
  Blocked goals get elevated priority — they are always present in your context.
  Use when you discover something preventing progress.

**`session_goals_progress`** — Record partial progress
  Note meaningful steps forward with evidence. Increases the goal's confidence.

### How It Works Together

You create and manage goals explicitly. Meanwhile, the system watches your conversation and:
- Detects when a goal is completed (auto-marks it done)
- Detects blockers (auto-adds them)
- Detects progress (auto-increases confidence)
- Detects when understanding deepens (auto-refines the description)
- Detects sub-tasks emerging (auto-creates sub-goals under your existing goals)

You don't need to do everything manually — the system helps. But YOU set the plan.

### Best Practices

- **Start of any non-trivial task:** Create a root goal
- **Break down complexity:** 2-4 sub-goals per root goal (divide et impera)
- **Check before creating:** Always session_goals_list first — avoid duplicates
- **When stuck:** Add a blocker, then pivot to an unblocked goal
- **After completing a step:** Mark the sub-goal complete, see what's next
- **User asks "what are we doing?":** session_goals_list gives the full answer
- **Completed goals persist:** Future sessions can discover what you accomplished

## Procedure Lifecycle

Procedures represent repeatable multi-step workflows. The system tracks which
procedures are available and their execution state.

**`procedure_create`** — Define a new procedure
  When you identify a repeatable workflow, formalize it as a procedure with explicit steps.
  Steps can require proof evidence (diff_hash, receipt, supervisor_sign_off).

**`procedure_activate`** — Start following a procedure
  Creates a tracked execution. Without activation, procedures in context are just guidance.
  With activation, step completion and proof submission are tracked.

**`procedure_complete_step`** — Mark a step as complete
  Provide proof_value for steps requiring evidence. The execution cannot be finalized
  without all required proofs.

**`procedure_session_status`** — View session procedure status
  Shows all procedures tracked in this session: which were surfaced, activated,
  step completion progress, and pending proofs.
```

---

## AGENTS.MD Additions

Add the following to the agent's AGENTS.MD (or equivalent agent behavior configuration):

```markdown
## Procedure Lifecycle Behavior

When a procedure surfaces in your context:
1. Create a session goal to track your progress through it
2. Call `procedure_activate` to start tracked execution
3. Work through each step, calling `procedure_complete_step` for each
4. For steps with proof requirements, collect and submit evidence
5. Check `procedure_session_status` to see what's pending

When you detect a repeatable multi-step pattern in your work:
1. Consider formalizing it as a procedure via `procedure_create`
2. Steps can require proof evidence: DIFF_HASH (commit SHA), RECEIPT (API response),
   SUPERVISOR_SIGN_OFF (confirmation text), CHUNK_REF (file reference), VERSION_RECORD
3. Procedures are scored and compete for context alongside facts and goals

## Goal Lifecycle Behavior

The agent is the planner. The extraction system is the scorekeeper.

- Root goals are ONLY created by explicit agent action via `session_goals_create`
- The extraction system NEVER creates root goals from conversation
- Sub-goals can be created by the agent (explicit) or by extraction (detected sub-task under existing parent)
- Status changes (completed, blocked, progressed, abandoned) can come from either the agent (explicit tool use) or extraction (automatic detection)
- Goal refinement (rewriting title/description) is extraction-only
```

### Breaking Behaviors (Phase 5+)

| Behavior | Detail |
|----------|--------|
| Guard 412 on unloaded session | If a session's guard rules haven't been loaded (e.g., after runtime restart), `preflight_check` raises `GuardRulesNotLoadedError` → API returns **412 Precondition Failed**. The agent should re-bootstrap the session or call `refresh_guard_rules()`. |
| `goal_create` scope removal | The `goal_create` tool always creates **session-scoped** goals. The `scope` parameter has been removed. Persistent goals are created exclusively via admin API (`POST /admin/goals`). |
| Goal confidence default 0.8 | New goals start with `confidence: 0.8` (not 1.0). This reflects "plausible but unverified" — confidence increases via `progress` evidence or sub-goal completion. |

---

## Required Environment Variables

### TS Plugins (both MemoryPlugin and ContextEnginePlugin)

| Variable | Required | Default | Read by | Purpose |
|----------|----------|---------|---------|---------|
| `EB_GATEWAY_ID` | **YES** | None (fail if missing) | Memory + Context | Unique gateway instance identifier. Example: `gw-prod-us-east-1` |
| `EB_GATEWAY_SHORT_NAME` | No | First 8 chars of `EB_GATEWAY_ID` | Memory only | Human-friendly label for display in logs and traces |
| `EB_RUNTIME_URL` | No | `http://localhost:8420` | Memory + Context | ElephantBroker runtime base URL |
| `EB_ACTOR_ID` | No | None | Memory only | Fallback actor ID for `X-EB-Actor-Id` header when OpenClaw ctx doesn't provide `actorId` |
| `EB_PROFILE` | No | `coding` | Memory only | Profile name override (also settable in plugin config) |

### Python Runtime

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `EB_GATEWAY_ID` | Recommended | `"local"` | Unique gateway instance identifier |
| `EB_NEO4J_URI` | Yes | `bolt://localhost:7687` | Neo4j Bolt connection |
| `EB_QDRANT_URL` | Yes | `http://localhost:6333` | Qdrant vector store |
| `EB_REDIS_URL` | Recommended | `redis://localhost:6379` | Redis for session goals, embedding cache, working set snapshots |
| `EB_LLM_API_KEY` | Yes | `""` | API key for LLM endpoint |
| `EB_EMBEDDING_API_KEY` | No | `""` | API key for embedding endpoint (if different from LLM) |
| `EB_HITL_CALLBACK_SECRET` | No | `""` | HMAC-SHA256 secret for HITL callback validation (same value in runtime + HITL) |
| `EB_RERANKER_ENDPOINT` | No | `http://localhost:1235` | Qwen3-Reranker-4B endpoint |
| `EB_RERANKER_API_KEY` | No | `""` | Reranker API key |
| `EB_ORG_ID` | No | `""` | Organization binding |
| `EB_TEAM_ID` | No | `""` | Team binding |

### HITL Middleware

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `HITL_HOST` | No | `0.0.0.0` | Bind address |
| `HITL_PORT` | No | `8421` | Listen port |
| `HITL_LOG_LEVEL` | No | `INFO` | Log level (debug/info/warning/error — NOT `verbose`) |
| `EB_HITL_CALLBACK_SECRET` | No | `""` | HMAC secret (must match runtime) |
| `EB_RUNTIME_URL` | No | `http://localhost:8420` | Runtime URL for callbacks |

### Logging Levels

The runtime supports a custom `VERBOSE` level (Python level 15, between DEBUG=10 and INFO=20). HITL and uvicorn do not.

| Config value | Runtime behavior | HITL behavior |
|-------------|-----------------|---------------|
| `debug` | All messages including full payloads | All messages |
| `verbose` | Per-item decisions, no DEBUG dumps | **NOT SUPPORTED** (use `info`) |
| `info` | Lifecycle milestones, budget resolution | Normal operation |
| `warning` | Errors, degraded operations only | Errors only |

### LLM Model Name

Cognee requires the `openai/` prefix to route through its OpenAI-compatible LLM client. It strips the prefix internally before sending to the LiteLLM proxy:

- Config: `model: "openai/gemini/gemini-2.5-pro"`
- Cognee sends: `gemini/gemini-2.5-pro` to LiteLLM
- ElephantBroker's own `LLMClient` also strips `openai/` before calling LiteLLM

**Without the prefix, Cognee hangs on LLM connection test at startup.**

---

## Session Lifecycle Expectations

### Session Start (`session_start` hook)

The plugin sends `POST /sessions/start` with:
- `session_key` — stable routing key (e.g., `agent:main:main`)
- `session_id` — ephemeral UUID, changes on `/new` or `/reset`
- `gateway_id` — from `EB_GATEWAY_ID`
- `agent_id` — from `PluginHookAgentContext.agentId`
- `agent_key` — `{gateway_id}:{agentId}`

### Session Reset (`before_reset` hook)

On session reset:
1. Old session's goals are flushed to Cognee graph (durable storage)
2. Redis key `eb:{gateway_id}:session_goals:{session_key}:{old_session_id}` is deleted
3. New session starts with empty goals

### Session End (`session_end` hook)

On session end:
1. All session goals (including sub-goals) are flushed to Cognee as `GoalDataPoint` nodes
2. CHILD_OF and OWNS_GOAL edges are created
3. Goal text is indexed via `cognee.add()` for future session discovery
4. Working set snapshot cache is cleaned up

### Turn Lifecycle

Each turn:
1. Messages are buffered and processed by the turn ingest pipeline
2. Fact extraction includes goal relevance tagging and goal status hints
3. Tier 1 hints (completed, abandoned, blocked, progressed) update Redis immediately
4. Tier 2 hints (refined, new_subgoal) trigger async LLM refinement tasks
5. Working set is built/rebuilt on demand via `POST /working-set/build`

### Key Pattern: Session-Scoped Redis Keys

All session-scoped Redis keys use the pattern `eb:{gateway_id}:{key_type}:{session_key}:{session_id}`:
- `eb:{gw}:session_goals:{sk}:{sid}` — session goals
- `eb:{gw}:ws_snapshot:{sk}:{sid}` — working set snapshot
- `eb:{gw}:compact_state:{sk}:{sid}` — compaction state (Phase 6)

Cross-session keys (persist across resets):
- `eb:{gw}:ingest_buffer:{sk}` — message buffer
- `eb:{gw}:recent_facts:{sk}` — recent facts for extraction context

Embedding cache is NOT gateway-scoped (deterministic per text+model):
- `eb:emb_cache:{hash}` — embedding cache

---

## Phase 6: Context Engine Lifecycle

Phase 6 adds 2 new artifact tools to the MemoryPlugin and a new ContextEnginePlugin. These enable the two-bucket artifact model (session Redis + persistent Cognee) and the full context assembly lifecycle.

### New Tools (2 — added to Memory Plugin, total: 18)

| Tool | Description | Parameters |
|------|-------------|------------|
| `artifact_search` | Search or retrieve tool output artifacts by query or ID. When context shows `[Tool output: X — → artifact_search("id")]`, use this to get full content. | `query` (required), `tool_name?`, `scope?` (session\|persistent\|all), `max_results?` |
| `create_artifact` | Save content as session (temporary) or persistent (permanent) artifact. | `content` (required), `tool_name?`, `scope?` (session\|persistent), `tags?`, `goal_id?`, `summary?` |

### AGENTS.MD Suggested Instructions

Add to agent system prompts or AGENTS.MD:

- Tool outputs are automatically captured per session. You don't need to save them manually.
- When you see `[Tool output: X — summary... → Call artifact_search("id") for full output]` in your context, call `artifact_search` with the provided ID to retrieve the full content.
- Use `create_artifact` to save important results:
  - `scope: "session"` (default) — temporary, auto-expires with session
  - `scope: "persistent"` — permanent, stored in knowledge graph, survives across sessions
- Use `artifact_search` to find past tool outputs:
  - Pass a query string to search by content
  - Pass an exact artifact ID for direct retrieval
  - Use `scope: "session"` to search only current session
  - Use `scope: "persistent"` to search permanent artifacts
  - Use `scope: "all"` (default) to search both

### TOOLS.MD Tool Definitions

- `artifact_search(query, tool_name?, scope?, max_results?)` — Search tool output artifacts
- `create_artifact(content, tool_name?, scope?, tags?, goal_id?, summary?)` — Save artifact

### GOALS.MD

GOALS.md is injected by OpenClaw into `runtimeContext.activeGoals` and serves as general/organizational guidance for the OpenClaw agent LLM. It is entirely independent from ElephantBroker's goal management system (session goals in Redis, persistent goals in Cognee graph). ElephantBroker ignores `runtimeContext.activeGoals` — it manages its own goals through the session_goals tools and extraction pipeline.

If GOALS.md adds complexity without clear benefit, leave it empty. ElephantBroker does not read, parse, or sync with GOALS.md content.

### runtimeContext

OpenClaw passes `runtimeContext` (containing activeTools, activeGoals, customData) to `compact()` and `afterTurn()`. ElephantBroker logs it as trace metadata for debugging but does NOT read, parse, or act on any fields. All state management (goals, tools, artifacts) is handled internally via Redis and Cognee. Writing to `runtimeContext.customData` is not supported — ElephantBroker has its own state stores.

### ContextEnginePlugin

The `elephantbroker-context` plugin is a separate plugin (`kind: "context-engine"`) that coexists with the memory plugin. It registers itself via `api.registerContextEngine("elephantbroker-context", () => engine)` and sets `ownsCompaction: true`, meaning OpenClaw delegates compaction decisions to ElephantBroker. Required environment variable: `EB_GATEWAY_ID`.

---

## Phase 7: Guards & Autonomy

Phase 7 adds 2 new tools to the MemoryPlugin for guard awareness, plus environment variables for HITL middleware configuration.

### New Tools (2 total)

| Tool | Description | Parameters |
|------|-------------|------------|
| `guards_list` | List active guard rules, pending approval requests, and recent guard events for the current session | None |
| `guard_status` | Get detailed guard event information including matched rules, outcome, and approval status | `guard_event_id` (required) |

---

### TOOLS.MD Additions (Phase 7)

Add the following to your agent's TOOLS.MD:

```markdown
## Guard Awareness

You have two tools for understanding what safety constraints are in effect:

**`guards_list`** — View active guard constraints
  Shows all active guard rules, any pending human approval requests, and the 10
  most recent guard events. Use this to understand why an action was blocked or
  what constraints apply to your current session.

**`guard_status`** — Check a specific guard event
  Get detailed information about a guard event by ID (from guards_list results).
  Shows the matched rules, the outcome (pass/warn/block/require_approval), and
  the approval status if the event triggered a human-in-the-loop request.

### How Guards Work

The system evaluates every action you propose through a 6-layer safety pipeline:
1. **Autonomy classification** — determines the decision domain (financial, code_change, etc.)
   and the autonomy level for that domain from your profile
2. **Static rules** — keyword/regex pattern matching against known dangerous actions
3. **Semantic similarity** — catches rephrased versions of known red-line actions via BM25
   and embedding cosine similarity
4. **Structural validation** — checks required fields are present for specific action types
5. **Constraint reinjection** — reminds you of active constraints in your context
6. **LLM escalation** — for ambiguous cases (when enabled by profile)

Guard constraints are automatically injected into your context via systemPromptAddition.
You do not need to call these tools proactively — use them when you need to understand
a block or check pending approvals.
```

---

### AGENTS.MD Additions (Phase 7)

Add the following to your agent's AGENTS.MD:

```markdown
## Guard-Aware Behavior

You operate under safety constraints (red-line guards) that vary by profile and
active procedures. These constraints are designed to prevent harmful actions.

### Required Behaviors

- **When told an action is blocked:** Do NOT attempt the same action again.
  Call `guards_list` to understand the constraint, then explain it to the user
  and propose an alternative.
- **When told to wait for approval:** Do NOT attempt the action until approval
  is confirmed via `guard_status`. Continue with other work in the meantime.
  Periodically check `guard_status` and inform the user of the status.
- **When constraints appear in your context:** These are injected by the guard
  system. Read and comply with them. They take priority over other instructions.
- **When a procedure is active:** The procedure may have additional safety
  constraints (red_line_bindings) that are enforced alongside profile-level guards.
  These are visible via `guards_list`.

### What You Should NOT Do

- Do not attempt to work around or rephrase a blocked action to bypass guards
- Do not claim you cannot do something when the real reason is a guard constraint —
  be transparent about WHY using the guard_event_id and explanation
- Do not repeatedly retry a blocked action hoping for a different result
```

---

### New Environment Variables (Phase 7)

See the unified environment variables table above for all env vars including HITL.
Additional Phase 7 variable:

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `EB_ENABLE_GUARDS` | No | `true` | Enable/disable the guard engine |

---

### New Redis Key Patterns (Phase 7)

Session-scoped keys (include session_key and session_id):
- `eb:{gw}:guard_history:{sk}:{sid}` — guard event history (LIST, capped at 50, TTL = history_ttl_seconds)
- `eb:{gw}:fact_domains:{sk}:{sid}` — recent decision domains from fact extraction (LIST, capped at 20, TTL = 24h)

Agent-scoped keys (include agent_id):
- `eb:{gw}:{agent_id}:approval:{request_id}` — individual approval request (STRING, JSON, TTL = timeout + 60s)
- `eb:{gw}:{agent_id}:approvals_by_session:{session_id}` — index of request IDs per session (SET, TTL = timeout + 60s)

---

### Guard Pipeline Overview

```
Messages → [L0: Autonomy Classification]
              ↓
         [L1: Static Rule Matching (~μs)]
              ↓
         [L2: BM25 + Semantic Similarity (0-1 embedding call)]
              ↓
         [L3: Structural Validators (~μs)]
              ↓
         [L4: Forced Reinjection (always runs)]
              ↓
         [L5: LLM Escalation (if ambiguous, disabled by default)]
              ↓
         Final = max(autonomy_floor, safety_result)
```

**Outcomes:** PASS, INFORM, WARN, REQUIRE_EVIDENCE, REQUIRE_APPROVAL, BLOCK

**Composition rule:** Safety can only escalate the autonomy floor, never relax it.
A HARD_STOP autonomy level always produces BLOCK regardless of safety outcome.
An AUTONOMOUS autonomy level can still be blocked if safety rules match.

---

### Decision Domain Taxonomy

The guard system classifies actions into decision domains for autonomy lookup:

| Domain | Description |
|--------|-------------|
| `financial` | Spending, transfers, refunds, purchases, investing |
| `data_access` | PII, sensitive records, production data, credentials |
| `communication` | External emails, public posts, messages to humans |
| `code_change` | Production deploys, database migrations, config changes |
| `scope_change` | Redefining goals, abandoning tasks, changing approach |
| `resource` | Compute allocation, storage, API quota consumption |
| `info_share` | What info the agent reveals to whom |
| `delegation` | Creating sub-agents, delegating tasks, granting permissions |
| `record_mutation` | Creating/updating/deleting persistent records |
| `uncategorized` | Fallback for unknown action types |

Each profile maps domains to autonomy levels (AUTONOMOUS, INFORM, APPROVE_FIRST, HARD_STOP).
Domain classification uses a 3-tier approach: Tier 1 static tool mapping, Tier 2 fact extraction
piggyback (decision_domain on extracted facts), Tier 3 keyword heuristics.

---

## Workspace Files: AGENTS.MD and TOOLS.MD

OpenClaw uses workspace files (`~/.openclaw/workspace/`) to configure agent behavior and tool documentation. These files are **heavily customized per deployment** — they contain persona, voice, red lines, heartbeat config, local notes, and more. ElephantBroker deployment requires **surgical edits**, not full replacement.

### Important: Additive/Surgical Edits Only

**Do NOT overwrite workspace files.** Only modify the memory-related sections. All other content (SOUL.md, USER.md, IDENTITY.md, HEARTBEAT.md, BOOTSTRAP.md, and non-memory sections of AGENTS.md/TOOLS.md) must be preserved exactly as-is.

### AGENTS.MD — 5 Surgical Changes

Edit `~/.openclaw/workspace/AGENTS.md` and make only these changes:

1. **Session Startup** — Remove steps that read memory files (`memory/YYYY-MM-DD.md`, `MEMORY.md`). Keep SOUL.md and USER.md reads.

2. **Memory section** — Replace the dual-system memory section (file-based + EB) with EB-only memory. The replacement should include:
   - `memory_store` as the immediate storage action for personal info, preferences, decisions
   - `memory_search` as the required action before answering "what do you know about..."
   - "Do not claim you cannot remember things"
   - Auto-recall on each turn (injected by EB context engine)
   - Memory categories (preference, decision, event, context, procedure, relationship, constraint)

3. **Remove "MEMORY.md - Your Long-Term Memory"** subsection entirely

4. **Remove "Write It Down - No Mental Notes"** subsection entirely

5. **Heartbeat section** — Remove only the "Memory Maintenance" subsection (memory file reads/organization). Keep all other heartbeat content (proactive checks, timing, what to check, heartbeat-state.json).

**Keep everything else verbatim:** Red Lines, External vs Internal, Group Chats, Emoji reactions, Tools/Skills/Voice, Platform Formatting, Heartbeat system, "Make It Yours", etc.

### TOOLS.MD — Add EB Tools If Missing

Edit `~/.openclaw/workspace/TOOLS.md` — add the ElephantBroker tool documentation section if not already present. Keep the existing "Local Notes" intro section and any user customizations.

EB tools to document (24 total):
- 5 memory: `memory_store`, `memory_search`, `memory_get`, `memory_update`, `memory_forget`
- 5 session goals: `session_goals_list`, `goal_create`, `session_goals_update_status`, `session_goals_add_blocker`, `session_goals_progress`
- 4 procedures: `procedure_create`, `procedure_activate`, `procedure_complete_step`, `procedure_session_status`
- 2 artifacts: `artifact_search`, `create_artifact`
- 2 guards: `guards_list`, `guard_status`
- 6 admin: `admin_create_org`, `admin_create_team`, `admin_register_actor`, `admin_add_member`, `admin_remove_member`, `admin_merge_actors`

### Reference Templates

Reference templates showing the EB-specific sections are in the repo at:
- `openclaw-plugins/elephantbroker-memory/workspace/AGENTS.md` — EB memory section template (splice into existing AGENTS.MD)
- `openclaw-plugins/elephantbroker-memory/workspace/TOOLS.md` — EB tool documentation template (add to existing TOOLS.MD)

These are **templates for the EB sections only**, not complete file replacements.
