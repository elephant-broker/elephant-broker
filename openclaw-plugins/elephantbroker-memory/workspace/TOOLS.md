# TOOLS.md - Local Notes

  Skills define _how_ tools work. This file is for _your_ specifics — the stuff that's unique to your setup.

  ## What Goes Here

  Things like:

  - Camera names and locations
  - SSH hosts and aliases
  - Preferred voices for TTS
  - Speaker/room names
  - Device nicknames
  - Anything environment-specific

  ---

  ## ElephantBroker Memory & Cognition

  You have a durable cognitive runtime (ElephantBroker) backing your memory, context, and safety systems. All information stored persists across sessions and is scored, ranked, and injected
  into your context automatically.

  ### Memory Tools

  **`memory_store`** — Save a fact, preference, decision, or any important information.
    ALWAYS use this when the user shares personal info, preferences, or asks you to remember something.

  **`memory_search`** — Search your memories by query.
    ALWAYS use this when the user asks "what do you know about...", "do you remember...", or when you need context from past conversations.

  **`memory_get`** — Retrieve a specific memory by ID.

  **`memory_update`** — Update an existing memory with new information.

  **`memory_forget`** — Permanently delete a memory (GDPR delete).

  ### Session Goal Management

  You have goal management tools to plan, organize, and track work.
  **You are the goal authority** — only you decide what the goals are. The system watches your conversation and auto-detects progress, blockers, and completion — but it never creates root goals
   on its own.

  **`session_goals_list`** — View the full goal tree with IDs, status, blockers, sub-goals, and confidence.
    Always call this first before creating goals to avoid duplicates.

  **`goal_create`** — Create a goal or sub-task with optional scope (session, actor, team, organization, global).
    Break complex work into sub-goals using parent_goal_id.

  **`session_goals_update_status`** — Mark goal as completed, paused, or abandoned.
    Always provide evidence when completing ("tests pass", "deployed to staging").

  **`session_goals_add_blocker`** — Report an obstacle on a goal.
    Blocked goals get elevated priority — they are always present in your context.

  **`session_goals_progress`** — Record partial progress with evidence.

  ### Procedure Lifecycle

  Procedures represent repeatable multi-step workflows with tracked execution and proof requirements.

  **`procedure_create`** — Define a new procedure with steps and optional proof requirements (diff_hash, receipt, supervisor_sign_off).

  **`procedure_activate`** — Start following a procedure (creates tracked execution).

  **`procedure_complete_step`** — Mark a procedure step as complete with optional proof.

  **`procedure_session_status`** — View all procedures tracked in this session.

  ### Artifacts

  **`artifact_search`** — Search or retrieve tool output artifacts by query or ID.
    When you see `[Tool output: X — summary → artifact_search("id")]` in context, use this to get the full content.

  **`create_artifact`** — Save content as session (temporary) or persistent (permanent) artifact.
    Use `scope: "persistent"` for important results that should survive across sessions.

  ### Guard Awareness

  **`guards_list`** — View active guard rules, pending human approval requests, and recent guard events.

  **`guard_status`** — Check a specific guard event by ID (from guards_list results).

  ### Admin Tools (privileged — require authority level)

  **`admin_create_org`**, **`admin_create_team`**, **`admin_register_actor`**, **`admin_add_member`**, **`admin_remove_member`**, **`admin_merge_actors`** — Organization and team management.
  Server returns 403 if insufficient authority.

  ---

  Add whatever helps you do your job. This is your cheat sheet.
