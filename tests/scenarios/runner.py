"""Scenario runner — registry, execution, reporting, and CLI."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Type

import httpx

from tests.scenarios.base import Scenario, ScenarioResult

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

SCENARIOS: dict[str, Type[Scenario]] = {}


def register(cls: Type[Scenario]) -> Type[Scenario]:
    """Decorator that registers a Scenario subclass by its ``name``."""
    SCENARIOS[cls.name] = cls
    return cls


from tests.scenarios.scenario_basic_memory import BasicMemoryScenario  # noqa: F401
from tests.scenarios.scenario_context_lifecycle import ContextLifecycleScenario  # noqa: F401
from tests.scenarios.scenario_multi_turn_memory import MultiTurnMemoryScenario  # noqa: F401
from tests.scenarios.scenario_goal_driven import GoalDrivenScenario  # noqa: F401
from tests.scenarios.scenario_procedure_execution import ProcedureExecutionScenario  # noqa: F401
from tests.scenarios.scenario_guard_check import GuardCheckScenario  # noqa: F401
from tests.scenarios.scenario_subagent_lifecycle import SubagentLifecycleScenario  # noqa: F401

# ---------------------------------------------------------------------------
# Phase auto-detection (OD-9)
# ---------------------------------------------------------------------------

# Probe table: (phase, endpoint, method, success_condition)
# A phase is available if the probe does NOT return 404.
# Phase 7: all guard endpoints require a session_id path param; use a dummy
#   UUID — the route returns 503 (engine not ready) or 200 (empty list),
#   both of which are != 404, confirming the route is registered.
# Phase 8: /admin/bootstrap-status requires no auth and always returns 200.
# Phase 9: /consolidation/status is GET (not POST).
_PHASE_PROBES: list[tuple[int, str, str]] = [
    (4, "/memory/store", "POST"),
    (5, "/working-set/build", "POST"),
    (6, "/context/bootstrap", "POST"),
    (7, "/guards/events/00000000-0000-0000-0000-000000000000", "GET"),
    (8, "/admin/bootstrap-status", "GET"),
    (9, "/consolidation/status", "GET"),
]


async def detect_phase(base_url: str) -> int:
    """Probe the EB runtime to determine the highest deployed phase.

    Sends lightweight requests to phase-specific endpoints. If an endpoint
    returns anything other than 404, that phase is considered available.
    Returns the highest available phase number (minimum 3, since the runtime
    itself being reachable implies at least Phase 3), or 0 if the server is
    unreachable.
    """
    headers = {"X-EB-Gateway-ID": "phase-detect"}

    async with httpx.AsyncClient(base_url=base_url, timeout=2.0) as client:
        # Health check first — if we can't reach the server, return 0
        try:
            await client.get("/health/", headers=headers)
        except (httpx.ConnectError, httpx.TimeoutException, httpx.ConnectTimeout):
            return 0

        # Server is up — Phase 3 skeleton is the baseline
        highest = 3

        for phase, endpoint, method in _PHASE_PROBES:
            try:
                if method == "GET":
                    resp = await client.get(endpoint, headers=headers)
                else:
                    resp = await client.post(endpoint, headers=headers, content=b"{}")
                # 404 means the route is not registered -> phase not deployed
                if resp.status_code != 404:
                    highest = phase
            except (httpx.ConnectError, httpx.TimeoutException, httpx.ConnectTimeout):
                # Server unreachable or too slow — stop probing further
                break
            except Exception:
                # Unexpected error on this probe — skip but keep trying
                continue

    return highest


# ---------------------------------------------------------------------------
# Execution helpers
# ---------------------------------------------------------------------------


async def run_scenario(
    name: str,
    base_url: str = "http://localhost:8420",
    gateway_id: str = "local",
) -> ScenarioResult:
    """Instantiate and execute a single registered scenario by name."""
    if name not in SCENARIOS:
        raise KeyError(f"Unknown scenario: {name}. Available: {list(SCENARIOS.keys())}")
    scenario = SCENARIOS[name](base_url=base_url, gateway_id=gateway_id)
    return await scenario.execute()


async def run_all_scenarios(
    base_url: str = "http://localhost:8420",
    gateway_id: str = "local",
    max_phase: int = 99,
) -> list[ScenarioResult]:
    """Run every registered scenario whose ``required_phase`` <= *max_phase*."""
    results: list[ScenarioResult] = []
    for name, cls in sorted(SCENARIOS.items()):
        if cls.required_phase > max_phase:
            continue
        scenario = cls(base_url=base_url, gateway_id=gateway_id)
        result = await scenario.execute()
        results.append(result)
    return results


async def run_all_scenarios_live(
    gateway_url: str,
    gateway_token: str,
    eb_url: str = "http://localhost:8420",
    agent_id: str = "main",
    max_phase: int = 99,
    scenario_name: str | None = None,
) -> list[ScenarioResult]:
    """Run scenarios against the EB runtime while an OpenClaw gateway is active.

    In live mode, scenarios still call EB APIs directly via the simulator
    (same as simulation mode), but the real OpenClaw gateway is expected to
    be running and connected to the same EB runtime. This validates that
    EB's API surface works correctly when the gateway is active.

    For scenarios that need to send messages through the real OpenClaw gateway
    (triggering the full plugin lifecycle), use LiveScenario directly.
    """
    results: list[ScenarioResult] = []
    scenarios_to_run = (
        {scenario_name: SCENARIOS[scenario_name]} if scenario_name else SCENARIOS
    )

    for name, cls in sorted(scenarios_to_run.items()):
        if cls.required_phase > max_phase:
            continue
        scenario = cls(base_url=eb_url, gateway_id="local")
        result = await scenario.execute()
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# Aggregate scoring
# ---------------------------------------------------------------------------


def compute_aggregate_reward(results: list[ScenarioResult]) -> float:
    """Return the mean reward score across all results (0.0-1.0)."""
    if not results:
        return 0.0
    return sum(r.reward_score for r in results) / len(results)


# ---------------------------------------------------------------------------
# L4 session aggregation (OD-10)
# ---------------------------------------------------------------------------


async def compute_l4_trace_health(
    base_url: str,
    gateway_id: str,
    session_ids: list[str],
) -> float:
    """Compute aggregate L4 trace health across scenario sessions.

    For each *session_id*, fetches the trace summary from the EB runtime and
    derives a per-session health score:
      - 1.0  if ``error_count == 0`` AND ``bootstrap_completed``
      - else ``max(0.0, 1.0 - error_count * 0.1)``

    Returns the mean health score across all sessions (0.0-1.0).
    """
    if not session_ids:
        return 0.0

    headers = {"X-EB-Gateway-ID": gateway_id}
    scores: list[float] = []

    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        for sid in session_ids:
            try:
                resp = await client.get(
                    f"/trace/session/{sid}/summary",
                    headers=headers,
                )
                resp.raise_for_status()
                summary = resp.json()

                error_count = len(summary.get("error_events", []))
                bootstrap_completed = summary.get("bootstrap_completed", False)

                if error_count == 0 and bootstrap_completed:
                    scores.append(1.0)
                else:
                    scores.append(max(0.0, 1.0 - error_count * 0.1))
            except Exception:
                # If we cannot fetch the summary, treat as worst-case
                scores.append(0.0)

    return sum(scores) / len(scores) if scores else 0.0


def _extract_session_ids(results: list[ScenarioResult]) -> list[str]:
    """Extract session IDs from scenario results via their trace_summary."""
    session_ids: list[str] = []
    for r in results:
        sid = r.trace_summary.get("session_id")
        if sid:
            session_ids.append(str(sid))
    return session_ids


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_report(
    results: list[ScenarioResult],
    l4_score: float | None = None,
) -> None:
    """Pretty-print a human-readable summary of scenario results."""
    width = 72
    print("=" * width)
    print("SCENARIO TEST REPORT")
    print("=" * width)

    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"\n  [{status}] {r.name}  (reward={r.reward_score:.2f}, {r.duration_ms}ms)")

        if r.steps:
            for s in r.steps:
                icon = "+" if s.passed else "-"
                msg = f" — {s.message}" if s.message else ""
                print(f"    [{icon}] {s.name}{msg}")

        if r.trace_assertions:
            print("    Trace assertions:")
            for ta in r.trace_assertions:
                icon = "+" if ta.passed else "-"
                bounds = f">={ta.min_count}"
                if ta.max_count is not None:
                    bounds += f", <={ta.max_count}"
                print(f"      [{icon}] {ta.event_type}: got {ta.actual_count} ({bounds})")

        if r.errors:
            print("    Errors:")
            for err in r.errors:
                print(f"      ! {err}")

    print("\n" + "-" * width)
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    agg = compute_aggregate_reward(results)
    print(f"  Total: {total}  Passed: {passed}  Failed: {total - passed}")
    print(f"  Aggregate reward: {agg:.3f}")
    if l4_score is not None:
        print(f"  L4 Trace Health: {l4_score:.2f}")
    print("=" * width)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="ElephantBroker scenario test runner",
    )
    parser.add_argument(
        "--scenario", type=str, default=None,
        help="Run a single scenario by name",
    )
    parser.add_argument(
        "--base-url", type=str, default="http://localhost:8420",
        help="Base URL of the ElephantBroker API (default: http://localhost:8420)",
    )
    parser.add_argument(
        "--max-phase", type=int, default=None,
        help="Only run scenarios with required_phase <= this value (auto-detected if omitted)",
    )
    parser.add_argument(
        "--detect-phase", action="store_true", default=False,
        help="Only run phase detection and print the result, then exit",
    )
    parser.add_argument(
        "--json", action="store_true", default=False,
        help="Output results as JSON instead of human-readable report",
    )
    # Live mode flags (used by live-mode runner, added later)
    parser.add_argument("--live", action="store_true", default=False,
                        help="Run in live mode against a real gateway")
    parser.add_argument("--gateway-url", type=str, default=None,
                        help="Gateway URL for live mode")
    parser.add_argument("--gateway-token", type=str, default=None,
                        help="Auth token for live mode gateway")
    parser.add_argument("--agent-id", type=str, default=None,
                        help="Agent ID for live mode")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    # --detect-phase: only run phase detection and exit
    if args.detect_phase:
        detected = asyncio.run(detect_phase(args.base_url))
        print(f"Detected phase: {detected}")
        sys.exit(0)

    # Resolve max_phase: auto-detect if not provided
    if args.max_phase is not None:
        max_phase = args.max_phase
    else:
        max_phase = asyncio.run(detect_phase(args.base_url))
        print(f"Auto-detected max phase: {max_phase}")

    gateway_id = "local"

    if args.live:
        if not args.gateway_url or not args.gateway_token:
            print("ERROR: --gateway-url and --gateway-token are required for live mode.",
                  file=sys.stderr)
            sys.exit(1)
        results = asyncio.run(run_all_scenarios_live(
            gateway_url=args.gateway_url,
            gateway_token=args.gateway_token,
            eb_url=args.base_url,
            agent_id=args.agent_id or "main",
            max_phase=max_phase,
            scenario_name=args.scenario,
        ))
    elif args.scenario:
        results = [asyncio.run(run_scenario(
            args.scenario, base_url=args.base_url, gateway_id=gateway_id,
        ))]
    else:
        results = asyncio.run(run_all_scenarios(
            base_url=args.base_url, gateway_id=gateway_id, max_phase=max_phase,
        ))

    # L4 trace health aggregation (OD-10)
    session_ids = _extract_session_ids(results)
    l4_score: float | None = None
    if session_ids:
        l4_score = asyncio.run(compute_l4_trace_health(
            base_url=args.base_url,
            gateway_id=gateway_id,
            session_ids=session_ids,
        ))

    if args.json:
        payload = [r.model_dump() for r in results]
        output = {"results": payload, "aggregate_reward": compute_aggregate_reward(results)}
        if l4_score is not None:
            output["l4_trace_health"] = l4_score
        print(json.dumps(output, indent=2))
    else:
        print_report(results, l4_score=l4_score)

    # Exit with non-zero if any scenario failed
    if any(not r.passed for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
