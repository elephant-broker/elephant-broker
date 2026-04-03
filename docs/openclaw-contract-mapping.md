# OpenClaw Contract Mapping

This document maps OpenClaw's TypeScript interfaces to ElephantBroker's Python API surface.

## ContextEngine Interface

OpenClaw defines the `ContextEngine` interface that plugins must implement. ElephantBroker exposes HTTP endpoints that the thin TypeScript plugin calls.

### TypeScript Reference Types

```typescript
interface ContextEngineInfo {
  id: string;
  name: string;
  version?: string;
  ownsCompaction?: boolean;
}

interface BootstrapResult {
  bootstrapped: boolean;
  importedMessages?: AgentMessage[];
  reason?: string;
}

interface IngestResult {
  ingested: boolean;
}

interface IngestBatchResult {
  ingestedCount: number;
}

interface AssembleResult {
  messages: AgentMessage[];
  estimatedTokens: number;
  systemPromptAddition?: string;
}

interface CompactResult {
  ok: boolean;
  compacted: boolean;
  reason?: string;
  result?: {
    summary?: string;
    firstKeptEntryId?: string;
    tokensBefore: number;
    tokensAfter?: number;
    details?: string;
  };
}

type SubagentEndReason = "deleted" | "completed" | "swept" | "released";

interface SubagentSpawnPreparation {
  rollback(): Promise<void>;
}
```

### Method → Endpoint Mapping

| OpenClaw Method | HTTP Endpoint | Pydantic Result Type |
|----------------|---------------|---------------------|
| `ContextEngine.bootstrap(sessionId, sessionKey?, sessionFile)` | `POST /context/bootstrap` | `BootstrapResult` |
| `ContextEngine.ingest(sessionId, sessionKey?, message, isHeartbeat?)` | `POST /context/ingest` | `IngestResult` |
| `ContextEngine.ingestBatch(sessionId, sessionKey?, messages, isHeartbeat?)` | `POST /context/ingest/batch` | `IngestBatchResult` |
| `ContextEngine.assemble(sessionId, sessionKey?, messages, tokenBudget?)` | `POST /context/assemble` | `AssembleResult` |
| `ContextEngine.compact(sessionId, sessionKey?, sessionFile, tokenBudget?, force?, ...)` | `POST /context/compact` | `CompactResult` |
| `ContextEngine.afterTurn(sessionId, sessionKey?, sessionFile, messages, ...)` | `POST /context/after-turn` | `void (204)` |
| `ContextEngine.prepareSubagentSpawn(parentSessionKey, childSessionKey, ttlMs?)` | `POST /context/subagent/spawn` | `SubagentPacket` |
| `ContextEngine.onSubagentEnded(childSessionKey, reason)` | `POST /context/subagent/ended` | `void (204)` |

## MemorySearchManager Interface

### TypeScript Reference Types

```typescript
interface MemorySearchResult {
  path: string;
  startLine: number;
  endLine: number;
  score: number;
  snippet: string;
  source: string;
  citation?: string;
}

interface MemoryProviderStatus {
  backend: string;
  provider: string;
  files: { total: number; indexed: number };
  chunks: { total: number; indexed: number };
  vectorStatus: string;
  // ... additional fields
}
```

### Method → Endpoint Mapping

| OpenClaw Method | HTTP Endpoint | Notes |
|----------------|---------------|-------|
| `MemorySearchManager.search(query, opts?)` | `POST /memory/search` | `opts: {maxResults?, minScore?, sessionKey?}` |
| `MemorySearchManager.readFile(relPath, from?, lines?)` | `GET /memory/read` | Returns `{text, path}` |
| `MemorySearchManager.status()` | `GET /memory/status` | Returns `MemoryProviderStatus` |
| `MemorySearchManager.sync(reason?, force?, sessionFiles?, progress?)` | `POST /memory/sync` | Triggers re-indexing |
| `MemorySearchManager.probeEmbeddingAvailability()` | `GET /memory/probe/embedding` | Returns `{ok, error?}` |
| `MemorySearchManager.probeVectorAvailability()` | `GET /memory/probe/vector` | Returns boolean |

## Hook Integration Points

### `before_prompt_build`

This is the primary hook for auto-recall injection. The handler returns:

```typescript
{
  systemPrompt?: string;          // Override entire system prompt (rarely used)
  prependContext?: string;         // Prepend to user context
  prependSystemContext?: string;   // Prepend to system context
  appendSystemContext?: string;    // Append to system context
}
```

**ElephantBroker mapping:** `SystemPromptOverlay` Pydantic model maps 1:1 to this return type.

### Plugin Registration

```typescript
api.registerContextEngine(id: string, factory: () => ContextEngine);
api.registerTool(factory, opts);
api.on(hookName: string, handler: Function);
```

**Plugin slots:** `PluginKind = "memory" | "context-engine"` — exclusive slots, defaults to `memory-core` and `legacy`.

## Result Type Mapping

| OpenClaw TypeScript Type | ElephantBroker Pydantic Model | Module |
|-------------------------|------------------------------|--------|
| `BootstrapResult` | `elephantbroker.schemas.context.BootstrapResult` | `context.py` |
| `IngestResult` | `elephantbroker.schemas.context.IngestResult` | `context.py` |
| `IngestBatchResult` | `elephantbroker.schemas.context.IngestBatchResult` | `context.py` |
| `AssembleResult` | `elephantbroker.schemas.context.AssembleResult` | `context.py` |
| `CompactResult` | `elephantbroker.schemas.context.CompactResult` | `context.py` |
| `AgentMessage` | `elephantbroker.schemas.context.AgentMessage` | `context.py` |
| `SubagentEndReason` | `elephantbroker.schemas.context.SubagentEndReason` | `context.py` (Literal type) |
| Hook return object | `elephantbroker.schemas.context.SystemPromptOverlay` | `context.py` |
| `MemorySearchResult` | TBD (Phase 4) | — |
| `MemoryProviderStatus` | TBD (Phase 4) | — |

## Serialization Notes

- All Pydantic models use `model_dump(mode="json")` for serialization to match camelCase TypeScript conventions via aliases where needed
- UUIDs serialize as strings
- Datetimes serialize as ISO 8601 strings
- Optional fields serialize as `null` (not omitted) to match TypeScript `undefined` behavior at the HTTP boundary
- The TypeScript plugin is responsible for camelCase ↔ snake_case conversion at the boundary
