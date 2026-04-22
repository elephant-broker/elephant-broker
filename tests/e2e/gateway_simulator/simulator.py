"""OpenClaw Gateway Simulator -- simulates hook lifecycle against Python runtime."""
from __future__ import annotations

import uuid

import httpx


class OpenClawGatewaySimulator:
    """Simulates OpenClaw gateway hook lifecycle against the Python runtime."""

    def __init__(self, base_url: str, session_key: str = "agent:main:main",
                 gateway_id: str | None = None):
        headers = {}
        if gateway_id:
            headers["X-EB-Gateway-ID"] = gateway_id
        self.client = httpx.AsyncClient(
            base_url=base_url, timeout=30.0, headers=headers)
        self.session_key = session_key
        self.session_id = str(uuid.uuid4())
        self.gateway_id = gateway_id

    async def simulate_session_start(self):
        await self.client.post("/sessions/start", json={
            "session_key": self.session_key, "session_id": self.session_id})

    async def simulate_before_agent_start(self, prompt: str) -> list[dict]:
        r = await self.client.post("/memory/search", json={
            "query": prompt, "session_key": self.session_key, "max_results": 10,
            "auto_recall": True})
        r.raise_for_status()
        return r.json()

    async def simulate_agent_end(self, messages: list[dict]):
        await self.client.post("/memory/ingest-messages", json={
            "messages": messages, "session_key": self.session_key,
            "session_id": self.session_id, "profile_name": "coding"})

    async def simulate_tool_memory_search(self, query: str) -> list[dict]:
        r = await self.client.post("/memory/search", json={"query": query})
        r.raise_for_status()
        return r.json()

    async def simulate_tool_memory_store(self, text: str, category: str = "general"):
        r = await self.client.post("/memory/store", json={
            "fact": {"text": text, "category": category}})
        r.raise_for_status()
        return r.json()

    async def simulate_tool_memory_forget_by_id(self, fact_id: str):
        await self.client.delete(f"/memory/{fact_id}")

    async def simulate_tool_memory_forget_by_query(self, query: str) -> dict:
        results = await self.simulate_tool_memory_search(query)
        if results and results[0].get("score", 0) > 0.7:
            fact_id = results[0]["id"]  # API returns "id", not "fact_id"
            await self.client.delete(f"/memory/{fact_id}")
            return {"deleted": fact_id, "text": results[0]["text"]}
        return {"deleted": None, "reason": "no match above threshold"}

    async def simulate_tool_memory_update_by_id(self, fact_id: str, updates: dict):
        r = await self.client.patch(f"/memory/{fact_id}", json=updates)
        r.raise_for_status()
        return r.json()

    async def simulate_session_end(self):
        r = await self.client.post("/sessions/end", json={
            "session_key": self.session_key, "session_id": self.session_id})
        r.raise_for_status()
        return r.json()

    async def simulate_full_turn(self, user_msg: str, assistant_msg: str):
        recalled = await self.simulate_before_agent_start(user_msg)
        await self.simulate_agent_end([
            {"role": "user", "content": user_msg},
            {"role": "assistant", "content": assistant_msg}])
        return recalled

    # --- Phase 5: Working Set ---

    async def simulate_build_working_set(self, query: str):
        """Simulate POST /working-set/build."""
        r = await self.client.post("/working-set/build", json={
            "session_id": str(self.session_id),
            "session_key": self.session_key,
            "profile_name": "coding",
            "query": query,
        })
        return r.json()

    # --- Phase 5: Session Goals ---

    async def simulate_session_goals_create(self, title: str, parent_goal_id: str | None = None):
        """Simulate POST /goals/session."""
        r = await self.client.post("/goals/session", params={
            "session_key": self.session_key,
            "session_id": str(self.session_id),
        }, json={"title": title, "parent_goal_id": parent_goal_id})
        return r.json()

    async def simulate_session_goals_list(self):
        """Simulate GET /goals/session."""
        r = await self.client.get("/goals/session", params={
            "session_key": self.session_key,
            "session_id": str(self.session_id),
        })
        return r.json()

    async def simulate_session_goals_update_status(self, goal_id: str, status: str, evidence: str = ""):
        """Simulate PATCH /goals/session/{goal_id}."""
        r = await self.client.patch(f"/goals/session/{goal_id}", params={
            "session_key": self.session_key,
            "session_id": str(self.session_id),
        }, json={"status": status, "evidence": evidence})
        return r.json()

    async def simulate_session_goals_add_blocker(self, goal_id: str, blocker: str):
        """Simulate POST /goals/session/{goal_id}/blocker."""
        r = await self.client.post(f"/goals/session/{goal_id}/blocker", params={
            "session_key": self.session_key,
            "session_id": str(self.session_id),
        }, json={"blocker": blocker})
        return r.json()

    async def simulate_session_goals_progress(self, goal_id: str, evidence: str):
        """Simulate POST /goals/session/{goal_id}/progress."""
        r = await self.client.post(f"/goals/session/{goal_id}/progress", params={
            "session_key": self.session_key,
            "session_id": str(self.session_id),
        }, json={"evidence": evidence})
        return r.json()

    # --- Phase 5: Procedures ---

    async def simulate_procedure_create(self, name: str, steps: list[dict] | None = None):
        """Simulate POST /procedures."""
        r = await self.client.post("/procedures/", json={
            "name": name, "steps": steps or [],
        })
        return r.json()

    async def simulate_procedure_activate(self, procedure_id: str):
        """Simulate POST /procedures/{procedure_id}/activate."""
        r = await self.client.post(f"/procedures/{procedure_id}/activate", json={})
        return r.json()

    async def simulate_procedure_complete_step(self, execution_id: str, step_id: str, proof_value: str | None = None):
        """Simulate POST /procedures/{execution_id}/step/{step_id}/complete."""
        r = await self.client.post(f"/procedures/{execution_id}/step/{step_id}/complete", json={
            "proof_value": proof_value,
        })
        return r.json()

    async def simulate_procedure_status(self):
        """Simulate GET /procedures/session/status."""
        r = await self.client.get("/procedures/session/status", params={
            "session_key": self.session_key,
            "session_id": str(self.session_id),
        })
        return r.json()

    # --- Phase 6: Context Lifecycle ---

    async def simulate_context_bootstrap(
        self,
        profile_name: str = "coding",
        is_subagent: bool = False,
        parent_session_key: str | None = None,
        prior_session_id: str | None = None,
    ) -> dict:
        """Simulate POST /context/bootstrap — initialize session context."""
        r = await self.client.post("/context/bootstrap", json={
            "session_key": self.session_key,
            "session_id": self.session_id,
            "profile_name": profile_name,
            "is_subagent": is_subagent,
            "parent_session_key": parent_session_key,
            "prior_session_id": prior_session_id,
        })
        r.raise_for_status()
        return r.json()

    async def simulate_context_ingest(self, role: str = "user", content: str = "") -> dict:
        """Simulate POST /context/ingest — single-message degraded mode (AD-29)."""
        r = await self.client.post("/context/ingest", json={
            "session_id": self.session_id,
            "session_key": self.session_key,
            "message": {"role": role, "content": content},
        })
        r.raise_for_status()
        return r.json()

    async def simulate_context_ingest_batch(
        self,
        messages: list[dict],
        profile_name: str = "coding",
        is_heartbeat: bool = False,
    ) -> dict:
        """Simulate POST /context/ingest-batch — batch message ingest (primary path)."""
        r = await self.client.post("/context/ingest-batch", json={
            "session_id": self.session_id,
            "session_key": self.session_key,
            "messages": messages,
            "profile_name": profile_name,
            "is_heartbeat": is_heartbeat,
        })
        r.raise_for_status()
        return r.json()

    async def simulate_context_assemble(
        self,
        messages: list[dict] | None = None,
        query: str = "",
        token_budget: int | None = None,
        profile_name: str = "coding",
        goal_ids: list[str] | None = None,
    ) -> dict:
        """Simulate POST /context/assemble — build context (Surface A)."""
        r = await self.client.post("/context/assemble", json={
            "session_id": self.session_id,
            "session_key": self.session_key,
            "messages": messages or [],
            "query": query,
            "token_budget": token_budget,
            "profile_name": profile_name,
            "goal_ids": goal_ids,
        })
        r.raise_for_status()
        return r.json()

    async def simulate_context_build_overlay(self) -> dict:
        """Simulate POST /context/build-overlay — Surface B (system prompt overlay)."""
        r = await self.client.post("/context/build-overlay", json={
            "session_key": self.session_key,
            "session_id": self.session_id,
        })
        r.raise_for_status()
        return r.json()

    async def simulate_context_compact(
        self,
        force: bool = False,
        token_budget: int | None = None,
        current_token_count: int | None = None,
    ) -> dict:
        """Simulate POST /context/compact — trigger compaction."""
        r = await self.client.post("/context/compact", json={
            "session_id": self.session_id,
            "session_key": self.session_key,
            "force": force,
            "token_budget": token_budget,
            "current_token_count": current_token_count,
        })
        r.raise_for_status()
        return r.json()

    async def simulate_context_after_turn(
        self,
        messages: list[dict] | None = None,
        pre_prompt_message_count: int | None = None,
    ) -> dict:
        """Simulate POST /context/after-turn — post-turn successful-use tracking.

        ``pre_prompt_message_count`` mirrors the 3-state ``AfterTurnParams``
        contract (TODO-6-308, Round 1 Blind Spot Reviewer, INFO):
        ``None`` → field omitted from the JSON body, exercises the
        'derived' branch (tail-walker fallback) on the server;
        ``0`` → honored as an explicit zero (hybrid A+C first-turn
        contract), NOT falsy-collapsed to None;
        positive ``N`` → passed through verbatim as the pre-prompt
        boundary marker.
        """
        r = await self.client.post("/context/after-turn", json={
            "session_id": self.session_id,
            "session_key": self.session_key,
            "messages": messages or [],
            "pre_prompt_message_count": pre_prompt_message_count,
        })
        r.raise_for_status()
        return r.json()

    async def simulate_context_subagent_spawn(
        self,
        child_session_key: str,
        ttl_ms: int | None = None,
    ) -> dict:
        """Simulate POST /context/subagent/spawn — register subagent parent mapping."""
        r = await self.client.post("/context/subagent/spawn", json={
            "parent_session_key": self.session_key,
            "child_session_key": child_session_key,
            "ttl_ms": ttl_ms,
        })
        r.raise_for_status()
        return r.json()

    async def simulate_context_subagent_ended(
        self,
        child_session_key: str,
        reason: str = "completed",
    ) -> dict:
        """Simulate POST /context/subagent/ended — subagent lifecycle conclusion."""
        r = await self.client.post("/context/subagent/ended", json={
            "child_session_key": child_session_key,
            "reason": reason,
        })
        r.raise_for_status()
        return r.json()

    async def simulate_context_subagent_rollback(
        self,
        child_session_key: str,
        rollback_key: str,
    ) -> dict:
        """Simulate POST /context/subagent/rollback — undo spawn on failure."""
        r = await self.client.post("/context/subagent/rollback", json={
            "parent_session_key": self.session_key,
            "child_session_key": child_session_key,
            "rollback_key": rollback_key,
        })
        r.raise_for_status()
        return r.json()

    async def simulate_context_dispose(self) -> dict:
        """Simulate POST /context/dispose — cleanup session context."""
        r = await self.client.post("/context/dispose", json={
            "session_key": self.session_key,
            "session_id": self.session_id,
        })
        r.raise_for_status()
        return r.json()

    async def simulate_context_get_config(self) -> dict:
        """Simulate GET /context/config — returns assembly config."""
        r = await self.client.get("/context/config")
        r.raise_for_status()
        return r.json()

    # --- Phase 6: Composite Helpers ---

    async def simulate_full_lifecycle_turn(
        self,
        user_msg: str,
        assistant_msg: str,
        profile_name: str = "coding",
        token_budget: int | None = None,
    ) -> dict:
        """Full context lifecycle turn: ingest_batch → assemble → after_turn.
        Returns the assemble result (messages + system_prompt_addition)."""
        messages = [
            {"role": "user", "content": user_msg},
            {"role": "assistant", "content": assistant_msg},
        ]
        await self.simulate_context_ingest_batch(messages, profile_name=profile_name)
        result = await self.simulate_context_assemble(
            messages=messages,
            query=user_msg,
            token_budget=token_budget,
            profile_name=profile_name,
        )
        await self.simulate_context_after_turn(
            messages=messages,
            pre_prompt_message_count=0,
        )
        return result

    async def simulate_multi_turn_conversation(
        self,
        turns: list[tuple[str, str]],
        profile_name: str = "coding",
        compact_after: int | None = None,
    ) -> list[dict]:
        """Run multiple turns sequentially. Each tuple is (user_msg, assistant_msg).
        Optionally trigger compaction after every N turns.
        Returns list of assemble results per turn."""
        results = []
        for i, (user_msg, assistant_msg) in enumerate(turns):
            result = await self.simulate_full_lifecycle_turn(
                user_msg, assistant_msg, profile_name=profile_name,
            )
            results.append(result)
            if compact_after and (i + 1) % compact_after == 0:
                await self.simulate_context_compact(force=True)
        return results

    # --- Phase 10: Trace Inspection ---

    async def get_session_traces(self, event_types: list[str] | None = None,
                                  limit: int = 500) -> list[dict]:
        """POST /trace/query — query trace events for current session."""
        query: dict = {"session_id": str(self.session_id), "limit": limit}
        if event_types:
            query["event_types"] = event_types
        r = await self.client.post("/trace/query", json=query)
        r.raise_for_status()
        return r.json()

    async def get_session_timeline(self) -> list[dict]:
        """GET /trace/session/{session_id}/timeline — events grouped by turn."""
        r = await self.client.get(f"/trace/session/{self.session_id}/timeline")
        r.raise_for_status()
        return r.json()

    async def get_session_summary(self) -> dict:
        """GET /trace/session/{session_id}/summary — aggregated stats."""
        r = await self.client.get(f"/trace/session/{self.session_id}/summary")
        r.raise_for_status()
        return r.json()

    async def close(self):
        await self.client.aclose()
