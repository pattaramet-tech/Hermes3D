/**
 * Translates the Hermes3D gateway protocol into hermes-agent's JSON-RPC 2.0 API.
 *
 * hermes-agent has no server that speaks the Hermes3D protocol — that protocol
 * came from OpenClaw. Rather than run a separate adapter process, this module
 * presents a virtual upstream to `gateway-proxy.js`: it exposes the small slice
 * of the `ws` WebSocket surface the proxy actually uses (`readyState`, `send`,
 * `close`, `terminate`, and the open/message/close/error events), so the proxy's
 * connect handling and lifecycle stay untouched.
 *
 * Hermes3D talks in agents and session keys; hermes-agent talks in runtime
 * session ids. A hermes-agent backend is a single agent, so the fleet is
 * synthesised as one entry and session keys are mapped onto runtime sessions
 * that are created or resumed on first use.
 */

const { EventEmitter } = require("node:events");
const { randomUUID } = require("node:crypto");

const { HermesAgentJsonRpcClient, redactUrl } = require("./jsonrpc-client");
const { createHermesAgentProfileApi } = require("./profile-api");
const { createOfficeSpeechSubscriber } = require("./office-speech");
const {
  KANBAN_TASK_ID_PREFIX,
  toHermes3dKanbanTaskRecord,
  toHermes3dKanbanTasks,
  toKanbanPatchBody,
  kanbanRequest,
} = require("./kanban");

/** Mirrors the numeric WebSocket readyState constants the proxy compares against. */
const CONNECTING = 0;
const OPEN = 1;
const CLOSED = 3;

const AGENT_ID = "hermes";
const AGENT_NAME = "Hermes";
const MAIN_KEY = "main";
const MAIN_SESSION_KEY = `agent:${AGENT_ID}:${MAIN_KEY}`;

/** hermes-agent's `session.create` / `session.resume` can be slow on a cold profile. */
const SESSION_RPC_TIMEOUT_MS = 60_000;

/**
 * The slice of the `ws` WebSocket surface `gateway-proxy.js` relies on.
 *
 * @typedef {import("node:events").EventEmitter & {
 *   readyState: number,
 *   send: (raw: string) => void,
 *   close: (code?: number, reason?: string) => void,
 *   terminate: () => void,
 * }} HermesAgentUpstream
 */

const resOk = (id, payload) => ({ type: "res", id, ok: true, payload: payload ?? {} });
const resErr = (id, code, message) => ({ type: "res", id, ok: false, error: { code, message } });

const asString = (value, fallback = "") =>
  typeof value === "string" && value.trim() ? value.trim() : fallback;

const errorMessage = (err) => {
  if (!err) return "hermes-agent request failed";
  if (typeof err === "string") return err;
  return err.message || String(err);
};

/** hermes-agent history rows use `text`; Hermes3D expects `content`. */
const toHermes3dMessages = (messages) => {
  if (!Array.isArray(messages)) return [];
  return messages
    .filter((m) => m && (m.role === "user" || m.role === "assistant"))
    .map((m) => ({
      role: m.role,
      content: typeof m.text === "string" ? m.text : String(m.content ?? ""),
    }));
};

const parseTimestampMs = (value) => {
  if (typeof value !== "string" || !value.trim()) return undefined;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? undefined : parsed;
};

const DURATION_UNIT_MS = { s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 };

/**
 * hermes-agent stores a schedule as one string; Hermes3D wants a tagged union.
 *
 * The string is a 5-field cron expression, a duration such as "30m", or an ISO
 * timestamp for a one-shot job.
 */
const toHermes3dSchedule = (raw) => {
  if (raw && typeof raw === "object") {
    const kind = asString(raw.kind).toLowerCase();
    if (kind === "every" && Number.isFinite(Number(raw.everyMs))) {
      return { kind: "every", everyMs: Number(raw.everyMs) };
    }
    if (kind === "at" && asString(raw.at)) {
      return { kind: "at", at: asString(raw.at) };
    }
    if (kind === "cron" && asString(raw.expr)) {
      return { kind: "cron", expr: asString(raw.expr) };
    }
    if (kind === "interval" && Number.isFinite(Number(raw.minutes))) {
      return { kind: "every", everyMs: Number(raw.minutes) * 60_000 };
    }
    if (kind === "once" && asString(raw.run_at)) {
      return { kind: "at", at: asString(raw.run_at) };
    }
    raw = raw.display || raw.value || raw.expr || raw.run_at;
  }
  const value = asString(raw);
  if (!value) return { kind: "cron", expr: "" };

  const duration = /^every\s+(\d+)\s*([smhd])$/i.exec(value) || /^(\d+)\s*([smhd])$/i.exec(value);
  if (duration) {
    const unit = DURATION_UNIT_MS[duration[2].toLowerCase()];
    if (unit) return { kind: "every", everyMs: Number(duration[1]) * unit };
  }

  // The REST API may return the human-readable one-shot form while retaining
  // the structured run_at value in `schedule`; do not mistake it for cron.
  const once = /^once\s+(?:in|at)\s+(.+)$/i.exec(value);
  if (once) {
    const timestamp = parseTimestampMs(once[1]);
    if (timestamp !== undefined) return { kind: "at", at: once[1] };
  }

  if (value.split(/\s+/).length === 5) return { kind: "cron", expr: value };

  const at = parseTimestampMs(value);
  if (at !== undefined) return { kind: "at", at: value };

  return { kind: "cron", expr: value };
};

const CRON_STATUSES = new Set(["ok", "error", "skipped"]);

/**
 * Convert Hermes3D's tagged schedule union to the string accepted by the
 * native hermes cron tool. The native runner owns parsing, validation, and
 * persistence; the bridge only translates the wire shape.
 */
const toNativeCronSchedule = (schedule) => {
  if (!schedule || typeof schedule !== "object") {
    throw new Error("Cron schedule is required.");
  }
  if (schedule.kind === "at") {
    const value = asString(schedule.at);
    if (!value) throw new Error("Cron run time is required.");
    return value;
  }
  if (schedule.kind === "cron") {
    const value = asString(schedule.expr);
    if (!value) throw new Error("Cron expression is required.");
    return value;
  }
  if (schedule.kind === "every") {
    const everyMs = Number(schedule.everyMs);
    if (!Number.isFinite(everyMs) || everyMs <= 0 || everyMs % 60_000 !== 0) {
      throw new Error("Native Hermes Cron supports intervals in whole minutes.");
    }
    return `every ${everyMs / 60_000}m`;
  }
  throw new Error(`Unsupported cron schedule kind "${asString(schedule.kind)}".`);
};

/** Hermes3D delivery object from the native runner's delivery string. */
const toHermes3dDelivery = (raw) => {
  const value = Array.isArray(raw) ? raw.map((item) => asString(item)).filter(Boolean).join(",") : asString(raw);
  if (!value || value.toLowerCase() === "local" || value.toLowerCase() === "none") {
    return { mode: "none" };
  }
  const separator = value.indexOf(":");
  const channel = separator === -1 ? value : value.slice(0, separator);
  const to = separator === -1 ? "" : value.slice(separator + 1);
  return {
    mode: "announce",
    channel,
    ...(to ? { to } : {}),
  };
};

/** Native cron uses one delivery string rather than Hermes3D's tagged object. */
const toNativeCronDelivery = (delivery) => {
  if (!delivery || delivery.mode === "none") return "local";
  const channel = asString(delivery.channel);
  const to = asString(delivery.to);
  if (!channel || channel === "local" || channel === "none" || channel === "last") {
    return to ? `origin:${to}` : "origin";
  }
  return to ? `${channel}:${to}` : channel;
};

const cronPayloadText = (payload, required = false) => {
  if (!payload || typeof payload !== "object") {
    if (required) throw new Error("Cron payload is required.");
    return "";
  }
  const kind = asString(payload.kind);
  if (kind !== "agentTurn" && kind !== "systemEvent") {
    throw new Error(`Unsupported cron payload kind "${kind || "unknown"}".`);
  }
  const text = asString(kind === "systemEvent" ? payload.text : payload.message);
  if (!text) throw new Error("Cron payload text must be non-empty.");
  return text;
};

/**
 * Build the profile-scoped payload for the native cron runner.
 *
 * `profile` is deliberately omitted for the default agent: on hermes-agent an
 * omitted profile means the gateway's launch profile. Named profiles must be
 * explicit or jobs silently land in the wrong scheduler/home.
 */
const toNativeCronParams = (params, agent, action) => {
  const profile = asString(agent?.profile);
  const scope = profile ? { profile } : {};
  const result = { action, ...scope };
  if (params?.name !== undefined) result.name = asString(params.name);
  if (params?.jobId !== undefined) result.name = asString(params.jobId);
  if (params?.schedule !== undefined) result.schedule = toNativeCronSchedule(params.schedule);
  if (params?.payload !== undefined) result.prompt = cronPayloadText(params.payload);
  if (params?.delivery !== undefined) result.deliver = toNativeCronDelivery(params.delivery);
  if (params?.repeat !== undefined) result.repeat = params.repeat;
  if (params?.includeDisabled !== undefined) result.include_disabled = Boolean(params.includeDisabled);
  return result;
};

/**
 * Translate hermes-agent cron rows into Hermes3D's `CronJobSummary`.
 *
 * The two shapes disagree on nearly every field: hermes-agent uses `job_id`, a
 * schedule string, `prompt_preview`, ISO timestamps, and a `state` *string*,
 * while Hermes3D expects `id`, a schedule object, a `payload` object, epoch
 * milliseconds, and a `state` object. Forwarding the raw rows crashes the
 * office task board, which reads `job.payload.kind` unguarded.
 */
const toHermes3dCronJobs = (jobs, agentId = AGENT_ID) => {
  if (!Array.isArray(jobs)) return [];
  return jobs
    .filter((job) => job && typeof job === "object")
    .map((job) => {
      const nextRunAtMs = parseTimestampMs(job.next_run_at);
      const lastRunAtMs = parseTimestampMs(job.last_run_at);
      const lastStatus = asString(job.last_status).toLowerCase();
      const lastError = asString(job.last_fire_error) || asString(job.last_delivery_error);
      const message =
        asString(job.prompt_preview) || asString(job.prompt) || asString(job.name) || "Scheduled job";

      return {
        id: asString(job.job_id) || asString(job.id),
        name: asString(job.name) || asString(job.job_id) || "Scheduled job",
        agentId,
        description: asString(job.prompt_preview) || asString(job.prompt) || undefined,
        enabled: job.enabled !== false,
        updatedAtMs: lastRunAtMs ?? nextRunAtMs ?? Date.now(),
        schedule: toHermes3dSchedule(job.schedule ?? job.schedule_display),
        sessionTarget: "isolated",
        wakeMode: "next-heartbeat",
        payload: { kind: "agentTurn", message },
        state: {
          ...(nextRunAtMs !== undefined ? { nextRunAtMs } : {}),
          ...(lastRunAtMs !== undefined ? { lastRunAtMs } : {}),
          ...(asString(job.state).toLowerCase() === "running"
            ? { runningAtMs: Date.now() }
            : {}),
          ...(CRON_STATUSES.has(lastStatus) ? { lastStatus } : {}),
          ...(lastError ? { lastError } : {}),
        },
        delivery: toHermes3dDelivery(job.deliver),
      };
    })
    .filter((job) => job.id);
};

/** Used when the backend predates `profiles.list` or the call fails. */
const fallbackAgent = () => ({
  id: AGENT_ID,
  name: AGENT_NAME,
  workspace: "",
  identity: { name: AGENT_NAME, emoji: "🤖" },
  role: "",
  profile: "",
});

/** Title-case a profile directory name for display ("allan" -> "Allan"). */
const toDisplayName = (name) =>
  name ? name.charAt(0).toUpperCase() + name.slice(1) : "";

/**
 * Turn hermes-agent's profile list into Hermes3D agents.
 *
 * A profile is the backend's unit of identity — its own model, skills, memory
 * and sessions — which is exactly what Hermes3D calls an agent. Collapsing them
 * into one entry hides the operator's whole fleet, so each profile gets a desk.
 *
 * The `profile` field is what routes sessions back; the default profile carries
 * an empty string because omitting `profile` means "launch profile" upstream.
 */
const toHermes3dAgents = (profiles) => {
  if (!Array.isArray(profiles)) return [];
  const agents = profiles
    .filter((p) => p && typeof p === "object" && asString(p.name))
    .map((p) => {
      const name = asString(p.name);
      const isDefault = p.is_default === true;
      const display = asString(p.display_name) || toDisplayName(name);
      // The long profile descriptions read as a role ("Allan — technical
      // planner…"); keep the part after the dash so the desk label stays short.
      const description = asString(p.description);
      const role = description.includes("—")
        ? description.split("—").slice(1).join("—").trim()
        : description;
      return {
        id: name,
        name: display,
        workspace: asString(p.path),
        identity: { name: display, emoji: isDefault ? "🤖" : "🧑‍💻" },
        role,
        model: asString(p.model),
        isDefault,
        profile: isDefault ? "" : name,
      };
    });
  return agents;
};

const resolveDefaultAgentId = (agents) => {
  const explicit = agents.find((a) => a.isDefault);
  return explicit?.id ?? agents[0]?.id ?? AGENT_ID;
};

const HERMES3D_ROLE_FILE_NAMES = [
  "AGENTS.md",
  "SOUL.md",
  "IDENTITY.md",
  "USER.md",
  "TOOLS.md",
  "HEARTBEAT.md",
  "MEMORY.md",
];

const roleFileMarker = (name, side) => `<!-- HERMES3D_FILE:${name}:${side} -->`;

const parseRoleFileBundle = (rawSoul) => {
  const raw = typeof rawSoul === "string" ? rawSoul : "";
  const bundle = new Map();
  let foundManagedSection = false;
  for (const name of HERMES3D_ROLE_FILE_NAMES) {
    const begin = roleFileMarker(name, "BEGIN");
    const end = roleFileMarker(name, "END");
    const start = raw.indexOf(begin);
    const stop = raw.indexOf(end);
    if (start < 0 || stop < start) continue;
    foundManagedSection = true;
    bundle.set(name, raw.slice(start + begin.length, stop).trim());
  }
  if (!foundManagedSection && raw.trim()) {
    bundle.set("SOUL.md", raw.trim());
  }
  return bundle;
};

const renderRoleFileBundle = (bundle) => {
  const sections = HERMES3D_ROLE_FILE_NAMES
    .filter((name) => bundle.has(name))
    .map((name) => {
      const content = String(bundle.get(name) ?? "").trim();
      return [roleFileMarker(name, "BEGIN"), content, roleFileMarker(name, "END")].join("\n");
    });
  return [
    "# Hermes3D Company Role",
    "",
    "This SOUL.md contains the role files managed by Hermes3D for this profile.",
    "",
    ...sections,
    "",
  ].join("\n");
};

function createHermesAgentUpstream(options) {
  const {
    url,
    token,
    handshakeTimeoutMs,
    profileApi: suppliedProfileApi,
    log = () => {},
    logError = () => {},
  } = options || {};

  const upstream = /** @type {HermesAgentUpstream} */ (new EventEmitter());
  upstream.readyState = CONNECTING;

  let client;
  try {
    client = new HermesAgentJsonRpcClient({ url, token, handshakeTimeoutMs, log });
  } catch (err) {
    // Defer so the caller can attach listeners before the failure lands.
    setImmediate(() => upstream.emit("error", err));
    upstream.readyState = CLOSED;
    upstream.send = () => {};
    upstream.close = () => {};
    upstream.terminate = () => {};
    return upstream;
  }

  const profileApi = suppliedProfileApi || createHermesAgentProfileApi({ url, token });
  const virtualRoleFiles = new Map();

  /** sessionKey -> { runtimeId, storedId, title } */
  const sessions = new Map();
  /** runtime session id -> sessionKey */
  const sessionKeyByRuntimeId = new Map();
  /**
   * Hermes3D agents, one per hermes-agent profile.
   *
   * Profiles are the backend's fleet: each is a fully isolated instance with
   * its own model, skills, memory, and session store. Resolved once at connect
   * because the set only changes when the operator creates or deletes one.
   */
  let agentRoster = [fallbackAgent()];
  let defaultAgentId = AGENT_ID;
  /** Optional feed of turns driven from other clients; see ./office-speech.js. */
  let officeSpeech = null;
  /** runId -> { sessionKey, runtimeId, buffer, aborted } */
  const activeRuns = new Map();
  /** sessionKey -> runId, so session-scoped events find their run. */
  const runBySessionKey = new Map();

  let seq = 0;
  let closed = false;

  const emitFrame = (frame) => {
    if (closed) return;
    upstream.emit("message", JSON.stringify(frame));
  };

  const emitEvent = (event, payload) => {
    emitFrame({ type: "event", event, seq: seq++, payload });
  };

  const emitChat = (runId, sessionKey, state, extra) => {
    emitEvent("chat", { runId, sessionKey, state, ...extra });
  };

  // --- session mapping ------------------------------------------------------

  const rememberSession = (sessionKey, result) => {
    const runtimeId = asString(result?.session_id);
    if (!runtimeId) throw new Error("hermes-agent did not return a session id.");
    const entry = {
      runtimeId,
      storedId: asString(result?.stored_session_id) || asString(result?.session_key),
      title: asString(result?.info?.title) || asString(result?.title),
    };
    sessions.set(sessionKey, entry);
    sessionKeyByRuntimeId.set(runtimeId, sessionKey);
    return entry;
  };

  /**
   * Split `agent:<agentId>:<tail>` into the agent and the stored session id.
   */
  const parseSessionKey = (sessionKey) => {
    const parts = String(sessionKey ?? "").split(":");
    if (parts[0] !== "agent" || parts.length < 3) {
      return { agentId: defaultAgentId, tail: "" };
    }
    return { agentId: parts[1] || defaultAgentId, tail: parts.slice(2).join(":") };
  };

  /**
   * The `profile` value to send upstream for an agent.
   *
   * Empty means "the launch profile", which is what hermes-agent expects for
   * the default; naming it explicitly is unnecessary and, for a backend with no
   * profiles at all, would be rejected.
   */
  const profileForAgent = (agentId) => {
    const agent = agentRoster.find((a) => a.id === agentId);
    return agent ? asString(agent.profile) : "";
  };

  /** Main session of whichever agent is default; the roster is known only at runtime. */
  const defaultMainKey = () => `agent:${defaultAgentId}:${MAIN_KEY}`;

  /**
   * Resolve the runtime session backing a Hermes3D session key, creating or
   * resuming one on hermes-agent the first time the key is used.
   *
   * The agent segment of the key selects the profile, so a prompt sent to
   * `agent:allan:main` runs with Allan's model, skills, and session store.
   */
  const ensureSession = async (sessionKey) => {
    const existing = sessions.get(sessionKey);
    if (existing?.runtimeId) return existing;

    const { agentId, tail } = parseSessionKey(sessionKey);
    const profile = profileForAgent(agentId);
    const scope = profile ? { profile } : {};

    // A key of the form `agent:<id>:<storedId>` refers to a stored hermes-agent
    // session; anything else starts a fresh one.
    if (tail && tail !== MAIN_KEY) {
      try {
        const resumed = await client.request(
          "session.resume",
          { session_id: tail, omit_messages: false, ...scope },
          SESSION_RPC_TIMEOUT_MS
        );
        return rememberSession(sessionKey, resumed);
      } catch (err) {
        log(`[hermes-agent] resume of "${tail}" failed, creating a new session: ${errorMessage(err)}`);
      }
    }

    const created = await client.request("session.create", scope, SESSION_RPC_TIMEOUT_MS);
    log(`[hermes-agent] session for "${sessionKey}" -> profile "${profile || "(default)"}"`);
    return rememberSession(sessionKey, created);
  };

  // --- upstream event fan-out ----------------------------------------------

  client.on("event", (type, runtimeSessionId, payload) => {
    const sessionKey = sessionKeyByRuntimeId.get(runtimeSessionId);
    if (!sessionKey) return;
    const runId = runBySessionKey.get(sessionKey);
    const run = runId ? activeRuns.get(runId) : null;

    switch (type) {
      case "message.start":
        if (run) run.buffer = "";
        return;

      case "message.delta": {
        if (!run || run.aborted) return;
        const text = typeof payload?.text === "string" ? payload.text : "";
        if (!text) return;
        run.buffer += text;
        emitChat(runId, sessionKey, "delta", {
          message: { role: "assistant", content: run.buffer },
        });
        return;
      }

      case "message.complete": {
        if (!run) return;
        const finalText =
          typeof payload?.text === "string" && payload.text ? payload.text : run.buffer;
        if (run.aborted) {
          emitChat(runId, sessionKey, "aborted", {});
        } else if (payload?.status === "error" || payload?.error) {
          emitChat(runId, sessionKey, "error", {
            errorMessage: asString(payload?.error, "hermes-agent reported an error"),
          });
        } else {
          emitChat(runId, sessionKey, "final", {
            stopReason: "end_turn",
            message: { role: "assistant", content: finalText },
          });
          emitEvent("presence", {
            sessions: {
              recent: [{ key: sessionKey, updatedAt: Date.now() }],
              byAgent: [
                {
                  agentId: parseSessionKey(sessionKey).agentId,
                  recent: [{ key: sessionKey, updatedAt: Date.now() }],
                },
              ],
            },
          });
        }
        activeRuns.delete(runId);
        runBySessionKey.delete(sessionKey);
        return;
      }

      case "tool.start":
        if (!run) return;
        emitEvent("agent", {
          runId,
          sessionKey,
          stream: "tool",
          data: { phase: "start", name: asString(payload?.name), text: asString(payload?.context) },
        });
        return;

      case "tool.complete":
        if (!run) return;
        emitEvent("agent", {
          runId,
          sessionKey,
          stream: "tool",
          data: { phase: "complete", name: asString(payload?.name), text: asString(payload?.summary) },
        });
        return;

      case "reasoning.delta":
      case "thinking.delta":
        if (!run) return;
        emitEvent("agent", {
          runId,
          sessionKey,
          stream: "reasoning",
          data: { phase: "delta", text: typeof payload?.text === "string" ? payload.text : "" },
        });
        return;

      case "status.update":
        if (!run) return;
        emitEvent("agent", {
          runId,
          sessionKey,
          stream: "lifecycle",
          data: { phase: asString(payload?.kind, "status"), text: asString(payload?.text) },
        });
        return;

      case "approval.request":
        emitEvent("exec.approval.requested", {
          id: asString(payload?.request_id),
          request: { command: asString(payload?.command), cwd: asString(payload?.cwd) },
          createdAtMs: Date.now(),
          expiresAtMs: Date.now() + 120_000,
        });
        return;

      case "error":
        if (!run) return;
        emitChat(runId, sessionKey, "error", {
          errorMessage: asString(payload?.message, "hermes-agent reported an error"),
        });
        activeRuns.delete(runId);
        runBySessionKey.delete(sessionKey);
        return;

      default:
    }
  });

  // --- method dispatch ------------------------------------------------------

  const applyProfileRoster = (profiles) => {
    const mapped = toHermes3dAgents(profiles);
    if (mapped.length === 0) return false;
    agentRoster = mapped;
    defaultAgentId = resolveDefaultAgentId(mapped);
    log(`[hermes-agent] ${mapped.length} profile(s) mapped to agents: ${mapped.map((a) => a.id).join(", ")}`);
    return true;
  };

  /**
   * Load the fleet once per connection.
   *
   * Newer hermes-agent builds expose `profiles.list` over JSON-RPC. Older
   * production builds may only expose the authenticated REST profile API, so
   * fall back to that before collapsing the office to one synthetic agent.
   */
  const loadAgentRoster = async () => {
    try {
      const result = await client.request("profiles.list", {}, SESSION_RPC_TIMEOUT_MS);
      if (applyProfileRoster(result?.profiles)) return;
      log("[hermes-agent] profiles.list returned nothing; trying profile REST API");
    } catch (err) {
      log(`[hermes-agent] profiles.list unavailable (${errorMessage(err)}); trying profile REST API`);
    }

    try {
      const profiles = await profileApi.listProfiles();
      if (applyProfileRoster(profiles)) {
        log("[hermes-agent] profile roster loaded through REST fallback");
        return;
      }
      log("[hermes-agent] profile REST API returned nothing; using a single agent");
    } catch (err) {
      log(`[hermes-agent] profile REST API unavailable (${errorMessage(err)}); using a single agent`);
    }
  };

  /** Refresh after profile CRUD without requiring a second JSON-RPC method round trip. */
  const refreshAgentRoster = async () => {
    const profiles = await profileApi.listProfiles();
    if (!applyProfileRoster(profiles)) {
      throw new Error("hermes-agent profile API returned no profiles.");
    }
  };

  const cronAgentByJobId = new Map();

  const resolveCronAgent = async (agentId, jobId) => {
    const requested = asString(agentId);
    const reference = asString(jobId);
    if (requested) {
      const agent = requireAgent(requested);
      if (!reference) return agent;

      const known = cronAgentByJobId.get(reference);
      if (known && known.id !== agent.id) {
        throw new Error(`Cron job "${reference}" belongs to agent "${known.id}", not "${agent.id}".`);
      }
      if (known) return agent;

      // Do not trust a caller-supplied agentId for an existing job. Verify the
      // reference in that profile before mutating it; otherwise a stale UI can
      // accidentally target the launch profile (or a different profile).
      const rows = await listCronJobsForAgent(agent, true);
      const matches = rows.filter((job) => {
        const id = asString(job?.job_id || job?.id);
        const name = asString(job?.name);
        return id === reference || name === reference;
      });
      if (matches.length === 0) throw new Error(`Unknown cron job "${reference}" for agent "${agent.id}".`);
      if (matches.length > 1) throw new Error(`Ambiguous cron job reference "${reference}" for agent "${agent.id}".`);
      cronAgentByJobId.set(reference, agent);
      return agent;
    }
    if (!reference) return requireAgent(defaultAgentId);
    const known = cronAgentByJobId.get(reference);
    if (known) return known;

    // A fresh browser can act on a job before it has rendered cron.list. Find
    // the owner rather than defaulting to the launch profile. Multiple owners
    // are an integrity error, not permission to pick the first one.
    const candidates = await Promise.all(
      agentRoster.map(async (agent) => {
        try {
          const jobs = await listCronJobsForAgent(agent, true);
          return jobs.some((job) => {
            const id = asString(job?.job_id || job?.id);
            return id === reference || asString(job?.name) === reference;
          })
            ? agent
            : undefined;
        } catch {
          return undefined;
        }
      }),
    );
    const owners = candidates.filter(Boolean);
    if (owners.length > 1) {
      throw new Error(`Ambiguous cron job ownership for "${reference}".`);
    }
    const resolved = owners[0];
    if (!resolved) throw new Error(`Unknown cron job "${reference}".`);
    cronAgentByJobId.set(reference, resolved);
    return resolved;
  };

  const requireAgent = (agentId) => {
    const resolved = agentRoster.find((agent) => agent.id === asString(agentId));
    if (!resolved) throw new Error(`Unknown hermes-agent profile "${asString(agentId)}".`);
    return resolved;
  };

  const cronProfileForAgent = (agent) => asString(agent?.profile) || "default";
  const hasProfileCronApi = (method) => typeof profileApi?.[method] === "function";
  const canFallbackToRpc = (err) => {
    const message = errorMessage(err);
    return message.includes("ECONNREFUSED") || /HTTP (404|405|426)/.test(message);
  };
  const isNamedCronProfile = (agent) => Boolean(asString(agent?.profile));
  const nativeCronActions = new Set(["list", "add", "remove", "pause", "resume"]);

  const cronResultError = (result, fallback) => {
    if (!result || typeof result !== "object") return fallback;
    return (
      asString(result.error?.message) ||
      asString(result.error) ||
      asString(result.detail) ||
      asString(result.message) ||
      fallback
    );
  };

  // Both adapters are allowed to return the native wrapper ({ jobs: [] }),
  // while the profile REST helper normally unwraps the JSON array. Treat an
  // explicit failure as a failure instead of turning it into an empty board.
  const cronRows = (result, action) => {
    if (result?.success === false || result?.ok === false) {
      throw new Error(cronResultError(result, `Hermes Cron ${action} failed.`));
    }
    if (Array.isArray(result)) return result;
    if (Array.isArray(result?.jobs)) return result.jobs;
    if (result === undefined || result === null) {
      throw new Error(`Hermes Cron ${action} returned no result.`);
    }
    return [];
  };

  const requireCronSuccess = (result, action) => {
    if (result?.success === false || result?.ok === false) {
      throw new Error(cronResultError(result, `Hermes Cron ${action} failed.`));
    }
    return result;
  };

  const filterCronJobs = (jobs, includeDisabled) => {
    const rows = cronRows(jobs, "list");
    return includeDisabled ? rows : rows.filter((job) => job?.enabled !== false);
  };

  const listCronJobsForAgent = async (agent, includeDisabled = true) => {
    if (hasProfileCronApi("listCronJobs")) {
      try {
        const rows = await profileApi.listCronJobs(cronProfileForAgent(agent), includeDisabled);
        return filterCronJobs(rows, includeDisabled);
      } catch (err) {
        // A named profile must never fall through to the launch profile.
        if (isNamedCronProfile(agent) || !canFallbackToRpc(err)) throw err;
      }
    }
    if (isNamedCronProfile(agent)) {
      throw new Error(`Profile-scoped Hermes Cron REST API is unavailable for "${cronProfileForAgent(agent)}".`);
    }
    const result = await client.request(
      "cron.manage",
      toNativeCronParams({ includeDisabled }, agent, "list"),
    );
    return filterCronJobs(result, includeDisabled);
  };

  const validateCronAction = (action, params) => {
    if (action === "add") {
      if (!asString(params.name)) throw new Error("Cron job name is required.");
      toNativeCronSchedule(params.schedule);
      cronPayloadText(params.payload, true);
    } else if (action === "update") {
      if (params.name === undefined && params.schedule === undefined && params.payload === undefined && params.delivery === undefined) {
        throw new Error("Cron update requires at least one field.");
      }
      if (params.schedule !== undefined) toNativeCronSchedule(params.schedule);
      if (params.payload !== undefined) cronPayloadText(params.payload, true);
    }
    if (action !== "list" && action !== "add" && !asString(params.jobId)) {
      throw new Error("Cron job id is required.");
    }
  };

  const runCronAction = async (action, params, agent) => {
    validateCronAction(action, params);
    const jobId = asString(params.jobId);
    const profile = cronProfileForAgent(agent);
    const restUnavailable = (err) => {
      if (isNamedCronProfile(agent)) {
        throw new Error(`Profile-scoped Hermes Cron REST API is unavailable for "${profile}": ${errorMessage(err)}`);
      }
      if (!canFallbackToRpc(err)) throw err;
    };

    if (action === "list" && hasProfileCronApi("listCronJobs")) {
      return listCronJobsForAgent(agent, params.includeDisabled !== false);
    }
    if (action === "add" && hasProfileCronApi("createCronJob")) {
      try {
        return requireCronSuccess(await profileApi.createCronJob(profile, {
          name: asString(params.name),
          schedule: toNativeCronSchedule(params.schedule),
          prompt: cronPayloadText(params.payload, true),
          deliver: toNativeCronDelivery(params.delivery),
        }), action);
      } catch (err) {
        restUnavailable(err);
      }
    }
    if (action === "update" && hasProfileCronApi("updateCronJob")) {
      const updates = {};
      if (params.name !== undefined) updates.name = asString(params.name);
      if (params.schedule !== undefined) updates.schedule = toNativeCronSchedule(params.schedule);
      if (params.payload !== undefined) updates.prompt = cronPayloadText(params.payload, true);
      if (params.delivery !== undefined) updates.deliver = toNativeCronDelivery(params.delivery);
      try {
        return requireCronSuccess(await profileApi.updateCronJob(profile, jobId, updates), action);
      } catch (err) {
        restUnavailable(err);
      }
    }
    if (action === "remove" && hasProfileCronApi("deleteCronJob")) {
      try {
        return requireCronSuccess(await profileApi.deleteCronJob(profile, jobId), action);
      } catch (err) {
        restUnavailable(err);
      }
    }
    if (action === "pause" && hasProfileCronApi("pauseCronJob")) {
      try {
        return requireCronSuccess(await profileApi.pauseCronJob(profile, jobId), action);
      } catch (err) {
        restUnavailable(err);
      }
    }
    if (action === "resume" && hasProfileCronApi("resumeCronJob")) {
      try {
        return requireCronSuccess(await profileApi.resumeCronJob(profile, jobId), action);
      } catch (err) {
        restUnavailable(err);
      }
    }
    if (action === "run" && hasProfileCronApi("triggerCronJob")) {
      try {
        return requireCronSuccess(await profileApi.triggerCronJob(profile, jobId), action);
      } catch (err) {
        restUnavailable(err);
      }
    }
    if (isNamedCronProfile(agent)) {
      throw new Error(`Profile-scoped Hermes Cron REST API is unavailable for "${profile}".`);
    }
    if (!nativeCronActions.has(action)) {
      throw new Error(`Native Hermes Cron does not support the "${action}" action.`);
    }
    const result = await client.request(
      "cron.manage",
      toNativeCronParams(params, agent, action),
    );
    return requireCronSuccess(result, action);
  };

  const loadRoleFiles = async (agentId) => {
    if (virtualRoleFiles.has(agentId)) return virtualRoleFiles.get(agentId);
    const agent = requireAgent(agentId);
    const soul = await profileApi.getSoul(agent.id);
    const bundle = parseRoleFileBundle(soul?.content);
    virtualRoleFiles.set(agentId, bundle);
    return bundle;
  };

  const persistRoleFiles = async (agentId, bundle) => {
    const agent = requireAgent(agentId);
    await profileApi.setSoul(agent.id, renderRoleFileBundle(bundle));
    virtualRoleFiles.set(agentId, bundle);
  };

  /**
   * Relay a turn published by the office bridge plugin.
   *
   * The plugin names the profile that spoke, and Hermes3D names each agent
   * after its profile, so the two line up directly. A turn from a profile this
   * connection does not know about is dropped rather than guessed at.
   */
  const handlePublishedTurn = (turn) => {
    const agent = agentRoster.find((a) => a.id === turn.profile);
    if (!agent) {
      log(`[office-speech] no agent for profile "${turn.profile}"; turn ignored`);
      return;
    }
    emitEvent("office.speech", {
      agentId: agent.id,
      name: agent.name,
      text: turn.text,
      atMs: turn.atMs,
      sessionId: turn.sessionId,
    });
  };

  const startOfficeSpeech = () => {
    if (officeSpeech) return;
    officeSpeech = createOfficeSpeechSubscriber({
      url,
      token,
      onTurn: handlePublishedTurn,
      log,
    });
  };

  const handleConnect = async (id) => {
    await loadAgentRoster();
    // Only worth subscribing once the roster exists to map turns onto.
    startOfficeSpeech();
    const agents = agentRoster.map((a) => ({
      agentId: a.id,
      name: a.name,
      isDefault: a.id === defaultAgentId,
    }));
    return resOk(id, {
      type: "hello-ok",
      protocol: 3,
      // The client trusts this over the configured type once connected, so
      // report what the backend actually is — hermes-agent capabilities
      // (native kanban, profile fleet) hang off this detection.
      adapterType: "hermes-agent",
      features: {
        methods: [
          "agents.list",
          "agents.create",
          "agents.update",
          "agents.delete",
          "agents.files.get",
          "agents.files.set",
          "sessions.list",
          "sessions.preview",
          "sessions.patch",
          "sessions.reset",
          "chat.send",
          "chat.abort",
          "chat.history",
          "agent.wait",
          "status",
          "config.get",
          "config.set",
          "config.patch",
          "exec.approvals.get",
          "exec.approvals.set",
          "exec.approval.resolve",
          "wake",
          "skills.status",
          "models.list",
          "tasks.list",
          "tasks.update",
          "cron.list",
          "cron.add",
          "cron.update",
          "cron.remove",
          "cron.pause",
          "cron.resume",
          "cron.run",
        ],
        events: ["chat", "agent", "presence", "heartbeat", "cron"],
      },
      snapshot: {
        health: { agents, defaultAgentId },
        sessionDefaults: { mainKey: MAIN_KEY },
      },
      auth: { role: "operator", scopes: ["operator.admin", "operator.approvals"] },
      policy: { tickIntervalMs: 30_000 },
    });
  };

  const handleMethod = async (method, params, id) => {
    const p = params || {};

    switch (method) {
      case "agents.list":
        return resOk(id, {
          defaultId: defaultAgentId,
          mainKey: MAIN_KEY,
          agents: agentRoster.map(({ id: agentId, name, workspace, identity, role }) => ({
            id: agentId,
            name,
            workspace,
            identity,
            role,
          })),
        });

      case "agents.create": {
        try {
          const displayName = asString(p.name);
          if (!displayName) return resErr(id, "hermes_agent.agent_name_required", "Agent name is required.");
          const created = await profileApi.createProfile(displayName);
          await refreshAgentRoster();
          const agent = agentRoster.find((entry) => entry.id === created.name);
          virtualRoleFiles.delete(created.name);
          return resOk(id, {
            ok: true,
            agentId: created.name,
            name: agent?.name || displayName,
            workspace: agent?.workspace || created.path || "",
          });
        } catch (err) {
          return resErr(id, "hermes_agent.agent_create_failed", errorMessage(err));
        }
      }

      case "agents.update": {
        try {
          const agentId = asString(p.agentId);
          const displayName = asString(p.name);
          const agent = requireAgent(agentId);
          if (agent.isDefault) {
            return resErr(id, "hermes_agent.default_profile_immutable", "The default Hermes profile cannot be renamed.");
          }
          if (!displayName) return resErr(id, "hermes_agent.agent_name_required", "Agent name is required.");
          const renamed = await profileApi.renameProfile(agent.id, displayName);
          const existingFiles = virtualRoleFiles.get(agent.id);
          if (existingFiles) {
            virtualRoleFiles.delete(agent.id);
            virtualRoleFiles.set(renamed.name, existingFiles);
          }
          await refreshAgentRoster();
          return resOk(id, { ok: true, agentId: renamed.name, name: displayName });
        } catch (err) {
          return resErr(id, "hermes_agent.agent_update_failed", errorMessage(err));
        }
      }

      case "agents.delete": {
        try {
          const agentId = asString(p.agentId);
          const agent = requireAgent(agentId);
          if (agent.isDefault) {
            return resErr(id, "hermes_agent.default_profile_immutable", "The default Hermes profile cannot be deleted.");
          }
          await profileApi.deleteProfile(agent.id);
          virtualRoleFiles.delete(agent.id);
          await refreshAgentRoster();
          return resOk(id, { ok: true, removedBindings: 0 });
        } catch (err) {
          return resErr(id, "hermes_agent.agent_delete_failed", errorMessage(err));
        }
      }

      case "agents.files.get": {
        try {
          const agentId = asString(p.agentId);
          const name = asString(p.name);
          if (!HERMES3D_ROLE_FILE_NAMES.includes(name)) {
            return resErr(id, "hermes_agent.file_unsupported", `Unsupported agent file "${name}".`);
          }
          const agent = requireAgent(agentId);
          const bundle = await loadRoleFiles(agent.id);
          const content = bundle.get(name);
          const workspace = agent.workspace || "";
          return resOk(id, {
            workspace,
            file: {
              missing: content === undefined,
              content: content ?? "",
              path: workspace ? `${workspace.replace(/[\\/]+$/, "")}/${name}` : null,
            },
          });
        } catch (err) {
          return resErr(id, "hermes_agent.file_read_failed", errorMessage(err));
        }
      }

      case "agents.files.set": {
        try {
          const agentId = asString(p.agentId);
          const name = asString(p.name);
          if (!HERMES3D_ROLE_FILE_NAMES.includes(name)) {
            return resErr(id, "hermes_agent.file_unsupported", `Unsupported agent file "${name}".`);
          }
          const bundle = new Map(await loadRoleFiles(agentId));
          bundle.set(name, typeof p.content === "string" ? p.content : "");
          await persistRoleFiles(agentId, bundle);
          return resOk(id, { ok: true });
        } catch (err) {
          return resErr(id, "hermes_agent.file_write_failed", errorMessage(err));
        }
      }

      case "config.get": {
        const defaultAgent = agentRoster.find((agent) => agent.id === defaultAgentId) ?? agentRoster[0];
        const workspace = asString(defaultAgent?.workspace);
        const configPath = workspace
          ? `${workspace.replace(/[\\/]+$/, "")}/config.yaml`
          : "/tmp/hermes-agent/config.yaml";
        return resOk(id, {
          config: { gateway: { reload: { mode: "hot" } } },
          hash: "hermes-agent",
          exists: true,
          path: configPath,
        });
      }

      case "config.patch":
      case "config.set":
        return resOk(id, { hash: "hermes-agent" });

      case "sessions.list": {
        // Each profile keeps its own session store, so the stored rows have to
        // be read per agent; they're local SQLite reads, so fan out in parallel.
        const perAgent = await Promise.all(
          agentRoster.map(async (agent) => {
            const profile = asString(agent.profile);
            let stored = [];
            try {
              const result = await client.request("session.list", {
                limit: 20,
                ...(profile ? { profile } : {}),
              });
              stored = Array.isArray(result?.sessions) ? result.sessions : [];
            } catch (err) {
              log(`[hermes-agent] session.list for "${agent.id}" failed: ${errorMessage(err)}`);
            }
            return [
              {
                key: `agent:${agent.id}:${MAIN_KEY}`,
                agentId: agent.id,
                updatedAt: Date.now(),
                displayName: "Main",
                origin: { label: agent.name, provider: "hermes" },
                modelProvider: "hermes",
              },
              ...stored.map((s) => ({
                key: `agent:${agent.id}:${asString(s.id)}`,
                agentId: agent.id,
                updatedAt: typeof s.started_at === "number" ? s.started_at * 1000 : null,
                displayName: asString(s.title, "Session"),
                origin: { label: agent.name, provider: "hermes" },
                modelProvider: "hermes",
              })),
            ];
          })
        );
        return resOk(id, { sessions: perAgent.flat() });
      }

      case "sessions.preview": {
        const keys = Array.isArray(p.keys) ? p.keys : [];
        const limit = typeof p.limit === "number" ? p.limit : 8;
        const maxChars = typeof p.maxChars === "number" ? p.maxChars : 240;
        const previews = await Promise.all(
          keys.map(async (key) => {
            const entry = sessions.get(key);
            if (!entry?.runtimeId) return { key, status: "empty", items: [] };
            try {
              const history = await client.request("session.history", {
                session_id: entry.runtimeId,
              });
              const items = toHermes3dMessages(history?.messages)
                .slice(-limit)
                .map((m) => ({
                  role: m.role,
                  text: m.content.slice(0, maxChars),
                  timestamp: Date.now(),
                }));
              return { key, status: items.length ? "ok" : "empty", items };
            } catch {
              return { key, status: "empty", items: [] };
            }
          })
        );
        return resOk(id, { ts: Date.now(), previews });
      }

      case "sessions.patch": {
        const key = asString(p.key, defaultMainKey());
        const model = typeof p.model === "string" ? p.model.trim() : "";
        if (model) {
          try {
            const entry = await ensureSession(key);
            await client.request("config.set", {
              key: "model",
              value: model,
              session_id: entry.runtimeId,
            });
          } catch (err) {
            log(`[hermes-agent] model switch failed: ${errorMessage(err)}`);
          }
        }
        return resOk(id, {
          ok: true,
          key,
          entry: { thinkingLevel: p.thinkingLevel },
          resolved: { model: model || undefined, modelProvider: "hermes" },
        });
      }

      case "sessions.reset": {
        const key = asString(p.key, defaultMainKey());
        const entry = sessions.get(key);
        if (entry?.runtimeId) {
          sessionKeyByRuntimeId.delete(entry.runtimeId);
          try {
            await client.request("session.close", { session_id: entry.runtimeId });
          } catch {}
        }
        sessions.delete(key);
        return resOk(id, { ok: true });
      }

      case "chat.send": {
        const sessionKey = asString(p.sessionKey, defaultMainKey());
        const text =
          typeof p.message === "string" ? p.message.trim() : String(p.message ?? "").trim();
        const runId = asString(p.idempotencyKey) || randomUUID();
        if (!text) return resOk(id, { status: "no-op", runId });

        let entry;
        try {
          entry = await ensureSession(sessionKey);
        } catch (err) {
          return resErr(id, "hermes_agent.session_failed", errorMessage(err));
        }

        activeRuns.set(runId, { sessionKey, runtimeId: entry.runtimeId, buffer: "", aborted: false });
        runBySessionKey.set(sessionKey, runId);

        try {
          await client.request("prompt.submit", { session_id: entry.runtimeId, text });
        } catch (err) {
          activeRuns.delete(runId);
          runBySessionKey.delete(sessionKey);
          return resErr(id, "hermes_agent.prompt_failed", errorMessage(err));
        }

        return resOk(id, { status: "started", runId });
      }

      case "chat.abort": {
        const runId = asString(p.runId);
        const sessionKey = asString(p.sessionKey);
        const targets = runId
          ? [runId]
          : [...activeRuns.entries()]
              .filter(([, run]) => run.sessionKey === sessionKey)
              .map(([rid]) => rid);

        let aborted = 0;
        for (const rid of targets) {
          const run = activeRuns.get(rid);
          if (!run) continue;
          run.aborted = true;
          aborted += 1;
          try {
            await client.request("session.interrupt", { session_id: run.runtimeId });
          } catch (err) {
            log(`[hermes-agent] interrupt failed: ${errorMessage(err)}`);
          }
        }
        return resOk(id, { ok: true, aborted });
      }

      case "chat.history": {
        const sessionKey = asString(p.sessionKey, defaultMainKey());
        const entry = sessions.get(sessionKey);
        if (!entry?.runtimeId) return resOk(id, { sessionKey, messages: [] });
        try {
          const history = await client.request("session.history", {
            session_id: entry.runtimeId,
          });
          return resOk(id, { sessionKey, messages: toHermes3dMessages(history?.messages) });
        } catch (err) {
          log(`[hermes-agent] session.history failed: ${errorMessage(err)}`);
          return resOk(id, { sessionKey, messages: [] });
        }
      }

      case "agent.wait": {
        const runId = asString(p.runId);
        const timeoutMs = typeof p.timeoutMs === "number" ? p.timeoutMs : 30_000;
        const start = Date.now();
        while (activeRuns.has(runId) && Date.now() - start < timeoutMs) {
          await new Promise((r) => setTimeout(r, 100));
        }
        return resOk(id, { status: activeRuns.has(runId) ? "running" : "done" });
      }

      case "status": {
        const recent = [...sessions.keys()].map((key) => ({ key, updatedAt: Date.now() }));
        const byAgent = agentRoster.map((agent) => ({
          agentId: agent.id,
          recent: recent.filter((entry) => parseSessionKey(entry.key).agentId === agent.id),
        }));
        return resOk(id, { sessions: { recent, byAgent } });
      }

      case "wake": {
        const text = asString(p.text);
        if (!text) return resOk(id, { ok: true });
        try {
          const entry = await ensureSession(defaultMainKey());
          await client.request("prompt.submit", { session_id: entry.runtimeId, text });
        } catch (err) {
          log(`[hermes-agent] wake failed: ${errorMessage(err)}`);
        }
        return resOk(id, { ok: true });
      }

      case "models.list": {
        try {
          const result = await client.request("model.options", {});
          const options = Array.isArray(result?.options) ? result.options : [];
          const models = options
            .map((o) => asString(o?.id) || asString(o?.slug) || asString(o?.model))
            .filter(Boolean)
            .map((modelId) => ({ id: modelId, name: modelId }));
          return resOk(id, { models: models.length ? models : [{ id: "hermes", name: "hermes" }] });
        } catch {
          return resOk(id, { models: [{ id: "hermes", name: "hermes" }] });
        }
      }

      case "skills.status": {
        try {
          const result = await client.request("skills.manage", { action: "list" });
          const skills = Array.isArray(result?.skills) ? result.skills : [];
          return resOk(id, { skills });
        } catch {
          return resOk(id, { skills: [] });
        }
      }

      case "cron.list": {
        const requestedAgentId = asString(p.agentId);
        const agents = requestedAgentId
          ? [requireAgent(requestedAgentId)]
          : agentRoster;
        try {
          const perAgent = await Promise.all(
            agents.map(async (agent) => {
              const jobs = await listCronJobsForAgent(agent, p.includeDisabled !== false);
              return toHermes3dCronJobs(jobs, agent.id);
            }),
          );
          const flattened = perAgent.flat();
          const owners = new Map();
          for (const job of flattened) {
            const previous = owners.get(job.id);
            if (previous && previous !== job.agentId) {
              throw new Error(`Ambiguous cron job ownership for "${job.id}".`);
            }
            if (previous) {
              throw new Error(`Duplicate cron job id "${job.id}" in agent "${job.agentId}".`);
            }
            owners.set(job.id, job.agentId);
          }
          cronAgentByJobId.clear();
          for (const job of flattened) {
            const owner = agentRoster.find((agent) => agent.id === job.agentId);
            if (owner) cronAgentByJobId.set(job.id, owner);
          }
          return resOk(id, { jobs: flattened });
        } catch (err) {
          return resErr(id, "hermes_agent.cron_list_failed", errorMessage(err));
        }
      }

      case "cron.add":
      case "cron.update":
      case "cron.remove":
      case "cron.pause":
      case "cron.resume":
      case "cron.run": {
        try {
          const agentId = asString(p.agentId);
          const action = method.slice("cron.".length);
          const actionParams = {
            name: p.name,
            jobId: p.id,
            schedule: p.schedule,
            payload: p.payload,
            delivery: p.delivery,
            repeat: p.repeat,
          };
          validateCronAction(action, actionParams);
          const agent = await resolveCronAgent(agentId, p.id);
          const result = await runCronAction(action, actionParams, agent);
          if (result?.success === false || result?.ok === false) {
            const semanticError =
              asString(result?.error?.message) ||
              asString(result?.error) ||
              asString(result?.message);
            throw new Error(semanticError || `Hermes cron ${action} failed.`);
          }
          const rawJob = result?.job || (action !== "remove" ? result : undefined);
          const mappedJob = rawJob
            ? toHermes3dCronJobs([rawJob], agent.id)[0]
            : undefined;
          if (mappedJob?.id) cronAgentByJobId.set(mappedJob.id, agent);
          if (action === "remove") {
            cronAgentByJobId.delete(asString(p.id));
            return resOk(id, { ok: true, removed: true });
          }
          if (action === "run") {
            return resOk(id, { ok: true, ran: true, ...(mappedJob ? { job: mappedJob } : {}) });
          }
          if (!mappedJob) {
            return resErr(
              id,
              `hermes_agent.cron_${action}_failed`,
              `hermes-agent did not return the ${action} cron job.`,
            );
          }
          return resOk(id, mappedJob);
        } catch (err) {
          return resErr(id, `hermes_agent.cron_${method.slice("cron.".length)}_failed`, errorMessage(err));
        }
      }

      case "exec.approvals.get":
        return resOk(id, {
          path: "",
          exists: true,
          hash: "hermes-agent",
          file: {
            version: 1,
            defaults: { security: "full", ask: "off", autoAllowSkills: true },
            agents: {},
          },
        });

      case "exec.approvals.set":
        return resOk(id, { hash: "hermes-agent" });

      case "exec.approval.resolve": {
        const requestId = asString(p.id);
        const decision = asString(p.decision, "deny");
        const runtimeId = [...sessions.values()][0]?.runtimeId;
        if (requestId && runtimeId) {
          try {
            await client.request("approval.respond", {
              session_id: runtimeId,
              request_id: requestId,
              choice: decision === "allow" ? "once" : "deny",
            });
          } catch (err) {
            log(`[hermes-agent] approval.respond failed: ${errorMessage(err)}`);
          }
        }
        return resOk(id, { ok: true });
      }

      // Kanban is built into hermes-agent — the board rides the same origin
      // and session token as the JSON-RPC gateway, so the office task board
      // reflects the real `hermes kanban` board with nothing to install.
      case "tasks.list": {
        try {
          const includeArchived = p.includeArchived === false ? "false" : "true";
          const board = await kanbanRequest({
            wsUrl: url,
            token,
            useLoopbackHost: client.usedLoopbackHost,
            method: "GET",
            path: `/board?include_archived=${includeArchived}`,
          });
          return resOk(id, { tasks: toHermes3dKanbanTasks(board) });
        } catch (err) {
          // A hidden/disabled kanban plugin or older backend is not an error
          // state for the office — the board just has no hermes tasks.
          log(`[hermes-agent] kanban board unavailable: ${errorMessage(err)}`);
          return resOk(id, { tasks: [] });
        }
      }

      case "tasks.update": {
        const rawId = asString(p.id);
        if (!rawId.startsWith(KANBAN_TASK_ID_PREFIX)) {
          return resErr(
            id,
            "hermes_agent.tasks_update_unsupported",
            "Only Hermes kanban tasks can be updated on this backend.",
          );
        }
        const taskId = rawId.slice(KANBAN_TASK_ID_PREFIX.length);
        try {
          const result = await kanbanRequest({
            wsUrl: url,
            token,
            useLoopbackHost: client.usedLoopbackHost,
            method: "PATCH",
            path: `/tasks/${encodeURIComponent(taskId)}`,
            body: toKanbanPatchBody(p),
          });
          const record = toHermes3dKanbanTaskRecord(result?.task);
          if (!record) {
            return resErr(
              id,
              "hermes_agent.tasks_update_failed",
              "hermes-agent did not return the updated task.",
            );
          }
          return resOk(id, record);
        } catch (err) {
          return resErr(id, "hermes_agent.tasks_update_failed", errorMessage(err));
        }
      }

      default:
        log(`[hermes-agent] unhandled method: ${method}`);
        return resOk(id, {});
    }
  };

  // --- virtual WebSocket surface -------------------------------------------

  upstream.send = (raw) => {
    let frame;
    try {
      frame = JSON.parse(String(raw ?? ""));
    } catch {
      return;
    }
    if (!frame || frame.type !== "req") return;

    const { id, method, params } = frame;
    const respond = (result) => emitFrame(result);

    if (method === "connect") {
      handleConnect(id).then(respond, (err) =>
        respond(resErr(id, "hermes_agent.connect_failed", errorMessage(err)))
      );
      return;
    }

    handleMethod(method, params, id).then(respond, (err) => {
      logError(`[hermes-agent] method "${method}" failed.`, err);
      respond(resErr(id, "hermes_agent.request_failed", errorMessage(err)));
    });
  };

  const stopOfficeSpeech = () => {
    officeSpeech?.close();
    officeSpeech = null;
  };

  upstream.close = (code, reason) => {
    closed = true;
    upstream.readyState = CLOSED;
    stopOfficeSpeech();
    client.close(code, reason);
  };

  upstream.terminate = () => {
    closed = true;
    upstream.readyState = CLOSED;
    stopOfficeSpeech();
    client.terminate();
  };

  client.on("ready", () => {
    upstream.readyState = OPEN;
    log(`[hermes-agent] JSON-RPC gateway ready at ${redactUrl(client.url)}`);
    upstream.emit("open");
  });

  client.on("close", (code, reason) => {
    closed = true;
    upstream.readyState = CLOSED;
    stopOfficeSpeech();
    upstream.emit("close", code, Buffer.from(String(reason ?? "")));
  });

  client.on("error", (err) => {
    upstream.emit("error", err);
  });

  client.connect();

  return upstream;
}

module.exports = {
  createHermesAgentUpstream,
  toHermes3dMessages,
  toHermes3dCronJobs,
  toHermes3dSchedule,
  toHermes3dDelivery,
  toNativeCronSchedule,
  toNativeCronDelivery,
  toNativeCronParams,
  toHermes3dAgents,
  resolveDefaultAgentId,
  MAIN_SESSION_KEY,
  AGENT_ID,
};
