// @vitest-environment node
import { afterEach, describe, expect, it } from "vitest";
import { createServer, type Server as HttpServer } from "node:http";
import { WebSocketServer, type WebSocket as WsSocket } from "ws";
import type { AddressInfo } from "node:net";

const { buildJsonRpcUrl, redactUrl } = await import("../../server/hermes-agent/jsonrpc-client");
const { createHermesAgentUpstream, toHermes3dMessages } = await import(
  "../../server/hermes-agent/bridge"
);

type Frame = Record<string, unknown>;
type RpcHandler = (params: Frame, emit: (type: string, payload: Frame) => void) => Frame | void;

/** Read a dotted path out of a decoded frame without widening everything to `any`. */
const at = (source: unknown, path: string): unknown =>
  path
    .split(".")
    .reduce<unknown>((acc, key) => (acc as Record<string, unknown> | undefined)?.[key], source);

const servers: WebSocketServer[] = [];
const restServers: HttpServer[] = [];
const upstreams: { terminate: () => void }[] = [];

afterEach(async () => {
  for (const upstream of upstreams.splice(0)) {
    try {
      upstream.terminate();
    } catch {}
  }
  for (const server of restServers.splice(0)) {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
  for (const server of servers.splice(0)) {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

/**
 * Minimal stand-in for hermes-agent's /api/ws: emits gateway.ready on connect,
 * answers JSON-RPC requests from `handlers`, and lets a handler push events.
 */
const startFakeHermesAgent = async (handlers: Record<string, RpcHandler>) => {
  const wss = new WebSocketServer({ port: 0 });
  servers.push(wss);
  await new Promise<void>((resolve) => wss.on("listening", () => resolve()));

  const received: Frame[] = [];

  wss.on("connection", (ws: WsSocket, req) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    received.push({ __connect: true, path: url.pathname, token: url.searchParams.get("token") });

    const send = (obj: unknown) => ws.send(JSON.stringify(obj));
    const emit = (type: string, payload: Frame) =>
      send({ jsonrpc: "2.0", method: "event", params: { type, session_id: "s1", payload } });

    send({ jsonrpc: "2.0", method: "event", params: { type: "gateway.ready", payload: {} } });

    ws.on("message", (raw) => {
      const request = JSON.parse(String(raw)) as Frame;
      received.push(request);
      const handler = handlers[String(request.method)];
      if (!handler) {
        send({
          jsonrpc: "2.0",
          id: request.id,
          error: { code: -32601, message: "unknown method" },
        });
        return;
      }
      const result = handler((request.params ?? {}) as Frame, emit);
      send({ jsonrpc: "2.0", id: request.id, result: result ?? {} });
    });
  });

  const { port } = wss.address() as AddressInfo;
  return { url: `ws://127.0.0.1:${port}`, received };
};

const startFakeProfileApi = async () => {
  const requests: { method: string; path: string; body?: Frame }[] = [];
  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://127.0.0.1");
    let body: Frame | undefined;
    if (req.method !== "GET" && req.method !== "DELETE") {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(Buffer.from(chunk));
      body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) as Frame : undefined;
    }
    requests.push({ method: req.method ?? "", path: `${url.pathname}${url.search}`, body });
    const profile = url.searchParams.get("profile") ?? "default";
    const jobId = profile === "coordinator" ? "coordinator-job" : "default-job";
    let result: unknown;
    if (req.method === "GET") {
      result = [
        { job_id: jobId, name: `${profile} active`, schedule: "every 1h", prompt: "Do work", deliver: "origin" },
        { job_id: `${jobId}-disabled`, name: `${profile} disabled`, enabled: false, schedule: "every 1h", prompt: "Disabled" },
      ];
    } else if (req.method === "POST" && url.pathname.endsWith("/trigger")) {
      result = { job_id: jobId, name: `${profile} active`, schedule: "every 1h", prompt: "Do work", deliver: "origin" };
    } else if (req.method === "POST") {
      result = { job_id: "created-job", name: body?.name, schedule: body?.schedule, prompt: body?.prompt, deliver: body?.deliver };
    } else if (req.method === "PUT") {
      result = { job_id: jobId, name: body?.updates && (body.updates as Frame).name, schedule: "every 1h", prompt: "Updated" };
    } else {
      result = { success: true };
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(result));
  });
  restServers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const { port } = server.address() as AddressInfo;
  return { url: `http://127.0.0.1:${port}`, requests };
};

/** Drive the bridge and collect the frames it sends back toward the browser. */
const openBridge = async (url: string, token = "", profileApi?: unknown) => {
  const frames: Frame[] = [];
  const upstream = createHermesAgentUpstream({ url, token, ...(profileApi ? { profileApi } : {}) });
  upstreams.push(upstream);
  upstream.on("message", (raw: string) => frames.push(JSON.parse(raw) as Frame));

  await new Promise<void>((resolve, reject) => {
    upstream.on("open", () => resolve());
    upstream.on("error", reject);
    setTimeout(() => reject(new Error("bridge did not open")), 5000);
  });

  const send = (frame: Frame) => upstream.send(JSON.stringify(frame));

  const waitFor = async (predicate: (frame: Frame) => boolean, label: string) => {
    const start = Date.now();
    while (Date.now() - start < 5000) {
      const hit = frames.find(predicate);
      if (hit) return hit;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw new Error(`timed out waiting for ${label}; saw ${JSON.stringify(frames)}`);
  };

  return { upstream, frames, send, waitFor };
};

describe("buildJsonRpcUrl", () => {
  it("appends the gateway path and maps https to wss", () => {
    expect(buildJsonRpcUrl("https://host.ts.net:8443", "abc")).toBe(
      "wss://host.ts.net:8443/api/ws?token=abc",
    );
  });

  it("keeps a path the caller already supplied", () => {
    expect(buildJsonRpcUrl("wss://host.ts.net:8443/api/ws", "")).toBe(
      "wss://host.ts.net:8443/api/ws",
    );
  });

  it("maps http to ws and tolerates a trailing slash", () => {
    expect(buildJsonRpcUrl("http://localhost:9119/", "t")).toBe(
      "ws://localhost:9119/api/ws?token=t",
    );
  });

  it("rejects a scheme that is not http(s) or ws(s)", () => {
    expect(() => buildJsonRpcUrl("ftp://host", "")).toThrow(/Unsupported scheme/);
  });

  it("keeps the token out of logged URLs", () => {
    expect(redactUrl("wss://h/api/ws?token=secret")).toBe("wss://h/api/ws?token=***");
  });
});

describe("loopback Host fallback", () => {
  /**
   * Stands in for a loopback-bound hermes-agent behind Tailscale Serve, which
   * forwards the client's Host verbatim: the tailnet name is refused with 4403
   * and only a loopback Host gets through.
   */
  const startHostStrictAgent = async () => {
    const wss = new WebSocketServer({ port: 0 });
    servers.push(wss);
    await new Promise<void>((resolve) => wss.on("listening", () => resolve()));

    const hostsSeen: string[] = [];
    wss.on("connection", (ws: WsSocket, req) => {
      const host = String(req.headers.host ?? "");
      hostsSeen.push(host);
      const hostOnly = host.split(":")[0].toLowerCase();
      if (!["localhost", "127.0.0.1", "::1"].includes(hostOnly)) {
        ws.close(4403, "host_mismatch");
        return;
      }
      ws.send(JSON.stringify({ jsonrpc: "2.0", method: "event", params: { type: "gateway.ready", payload: {} } }));
    });

    const { port } = wss.address() as AddressInfo;
    return { port, hostsSeen };
  };

  it("retries with a loopback Host when the backend refuses the forwarded one", async () => {
    const { port, hostsSeen } = await startHostStrictAgent();
    const { HermesAgentJsonRpcClient } = await import("../../server/hermes-agent/jsonrpc-client");

    // 127.0.0.1 resolves, but the Host header carries a name the backend rejects.
    const client = new HermesAgentJsonRpcClient({ url: `ws://127.0.0.1:${port}`, token: "t" });
    client.hostHeader = "box.ts.net";
    client.loopbackHostFallback = true;

    const ready = new Promise<void>((resolve, reject) => {
      client.on("ready", () => resolve());
      client.on("close", (code: number) => reject(new Error(`closed ${code}`)));
      setTimeout(() => reject(new Error("never became ready")), 5000);
    });
    client.connect();
    await ready;

    expect(hostsSeen[0]).toBe("box.ts.net");
    expect(hostsSeen[1]).toBe("localhost");
    expect(client.usedLoopbackHost).toBe(true);
    client.terminate();
  });

  it("retries when the upgrade is refused with HTTP 403 before accepting", async () => {
    // How a loopback-bound hermes-agent actually refuses a foreign Host on
    // /api/ws: the handshake is rejected outright rather than accepted-then-closed.
    const hostsSeen: string[] = [];
    const wss = new WebSocketServer({
      port: 0,
      verifyClient: ({ req }, done) => {
        const host = String(req.headers.host ?? "");
        hostsSeen.push(host);
        done(host.split(":")[0].toLowerCase() === "localhost", 403);
      },
    });
    servers.push(wss);
    await new Promise<void>((resolve) => wss.on("listening", () => resolve()));
    wss.on("connection", (ws: WsSocket) => {
      ws.send(JSON.stringify({ jsonrpc: "2.0", method: "event", params: { type: "gateway.ready", payload: {} } }));
    });
    const { port } = wss.address() as AddressInfo;

    const { HermesAgentJsonRpcClient } = await import("../../server/hermes-agent/jsonrpc-client");
    const client = new HermesAgentJsonRpcClient({ url: `ws://127.0.0.1:${port}`, token: "t" });
    client.hostHeader = "box.ts.net";

    const ready = new Promise<void>((resolve, reject) => {
      client.on("ready", () => resolve());
      client.on("error", (e: Error) => reject(e));
      setTimeout(() => reject(new Error("never became ready")), 5000);
    });
    client.connect();
    await ready;

    expect(hostsSeen[0]).toBe("box.ts.net");
    expect(hostsSeen[1]).toBe("localhost");
    client.terminate();
  });

  it("gives up after one retry rather than looping", async () => {
    const wss = new WebSocketServer({ port: 0 });
    servers.push(wss);
    await new Promise<void>((resolve) => wss.on("listening", () => resolve()));
    let attempts = 0;
    wss.on("connection", (ws: WsSocket) => {
      attempts += 1;
      ws.close(4403, "host_mismatch");
    });
    const { port } = wss.address() as AddressInfo;

    const { HermesAgentJsonRpcClient } = await import("../../server/hermes-agent/jsonrpc-client");
    const client = new HermesAgentJsonRpcClient({ url: `ws://127.0.0.1:${port}`, token: "t" });

    const closed = new Promise<number>((resolve) => client.on("close", (code: number) => resolve(code)));
    client.connect();
    expect(await closed).toBe(4403);
    expect(attempts).toBe(2);
  });
});

describe("profiles as agents", () => {
  // Verbatim rows from a live hermes-agent `profiles.list`.
  const backendProfiles = [
    {
      name: "default",
      path: "/Users/lukeai1/.hermes",
      is_default: true,
      model: "claude-haiku-4-5-20251001",
      description: "",
      display_name: "",
    },
    {
      name: "coordinator",
      path: "/Users/lukeai1/.hermes/profiles/coordinator",
      is_default: false,
      model: "claude-haiku-4-5-20251001",
      description:
        "Coordinator — technical planner and business systems analyst for Smartways. Converts Jira tickets into plans.",
      display_name: "",
    },
    {
      name: "andrew",
      path: "/Users/lukeai1/.hermes/profiles/andrew",
      is_default: false,
      model: "claude-opus-4-8",
      description: "Andrew — senior full-stack software developer for Smartways.",
      display_name: "",
    },
  ];

  it("gives every profile its own agent", async () => {
    const { toHermes3dAgents } = await import("../../server/hermes-agent/bridge");
    const agents = toHermes3dAgents(backendProfiles);
    expect(agents.map((a) => a.id)).toEqual(["default", "coordinator", "andrew"]);
    expect(agents.map((a) => a.name)).toEqual(["Default", "Coordinator", "Andrew"]);
  });

  it("routes non-default agents by profile and leaves the default unnamed", async () => {
    const { toHermes3dAgents } = await import("../../server/hermes-agent/bridge");
    const [def, coordinator] = toHermes3dAgents(backendProfiles);
    // An empty profile means "launch profile" upstream; naming it is wrong.
    expect(def.profile).toBe("");
    expect(coordinator.profile).toBe("coordinator");
  });

  it("uses the description after the dash as the role", async () => {
    const { toHermes3dAgents } = await import("../../server/hermes-agent/bridge");
    const coordinator = toHermes3dAgents(backendProfiles)[1];
    expect(coordinator.role.startsWith("technical planner")).toBe(true);
  });

  it("picks the flagged profile as the default agent", async () => {
    const { toHermes3dAgents, resolveDefaultAgentId } = await import(
      "../../server/hermes-agent/bridge"
    );
    expect(resolveDefaultAgentId(toHermes3dAgents(backendProfiles))).toBe("default");
  });

  it("ignores unusable rows", async () => {
    const { toHermes3dAgents } = await import("../../server/hermes-agent/bridge");
    expect(toHermes3dAgents([null, {}, "x", { name: "" }])).toEqual([]);
    expect(toHermes3dAgents(undefined)).toEqual([]);
  });

  it("advertises every profile and creates sessions against the right one", async () => {
    const createCalls: Frame[] = [];
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: backendProfiles }),
      "session.create": (params) => {
        createCalls.push(params);
        return { session_id: `rt-${createCalls.length}` };
      },
      "prompt.submit": () => ({}),
    });
    const bridge = await openBridge(agent.url);

    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");

    bridge.send({ type: "req", id: "a1", method: "agents.list", params: {} });
    const listed = await bridge.waitFor((f) => f.type === "res" && f.id === "a1", "agents.list");
    expect((at(listed, "payload.agents") as unknown[]).length).toBe(3);

    // A prompt aimed at Coordinator's desk must run under Coordinator's profile.
    bridge.send({
      type: "req",
      id: "s1",
      method: "chat.send",
      params: { sessionKey: "agent:coordinator:main", message: "hi" },
    });
    await bridge.waitFor((f) => f.type === "res" && f.id === "s1", "chat.send");
    expect(createCalls.at(-1)).toEqual({ profile: "coordinator" });

    // The default agent must NOT send a profile — that means "launch profile".
    bridge.send({
      type: "req",
      id: "s2",
      method: "chat.send",
      params: { sessionKey: "agent:default:main", message: "hi" },
    });
    await bridge.waitFor((f) => f.type === "res" && f.id === "s2", "chat.send default");
    expect(createCalls.at(-1)).toEqual({});
  });

  it("falls back to the REST profile API when the backend has no profiles.list", async () => {
    const agent = await startFakeHermesAgent({});
    const profileApi = {
      listProfiles: async () => backendProfiles,
    };
    const bridge = await openBridge(agent.url, "", profileApi);
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");
    expect((at(res, "payload.snapshot.health.agents") as unknown[]).length).toBe(3);
    expect(at(res, "payload.snapshot.health.defaultAgentId")).toBe("default");
  });

  it("falls back to a single agent when both profile discovery paths fail", async () => {
    const agent = await startFakeHermesAgent({});
    const profileApi = {
      listProfiles: async () => {
        throw new Error("REST profile API unavailable");
      },
    };
    const bridge = await openBridge(agent.url, "", profileApi);
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");
    expect((at(res, "payload.snapshot.health.agents") as unknown[]).length).toBe(1);
    expect(at(res, "payload.snapshot.health.defaultAgentId")).toBe("hermes");
  });
});

describe("toHermes3dCronJobs", () => {
  // Verbatim row shape returned by a live hermes-agent `cron.manage` list.
  const agentJob = {
    job_id: "f43da87997a8",
    name: "Daily token spend - morning briefing",
    prompt_preview: "Run `hermes insights --days 1` and extract today's data.",
    schedule: "0 7 * * *",
    repeat: "forever",
    deliver: "origin",
    next_run_at: "2026-08-19T07:00:00-05:00",
    last_run_at: "2026-08-18T07:00:40.472539-05:00",
    last_status: "ok",
    last_delivery_error: null,
    last_fire_error: null,
    enabled: true,
    state: "scheduled",
  };

  it("fills in the fields the office task board reads unguarded", async () => {
    const { toHermes3dCronJobs } = await import("../../server/hermes-agent/bridge");
    const [job] = toHermes3dCronJobs([agentJob]);

    // These four are exactly what crashed the office page when forwarded raw.
    expect(job.id).toBe("f43da87997a8");
    expect(job.payload).toEqual({ kind: "agentTurn", message: agentJob.prompt_preview });
    expect(job.schedule).toEqual({ kind: "cron", expr: "0 7 * * *" });
    expect(typeof job.state).toBe("object");
    expect(job.state.lastStatus).toBe("ok");
    expect(job.state.nextRunAtMs).toBe(Date.parse(agentJob.next_run_at));
    expect(Number.isFinite(job.updatedAtMs)).toBe(true);
  });

  it("marks a running job so the board can show it as working", async () => {
    const { toHermes3dCronJobs } = await import("../../server/hermes-agent/bridge");
    const [job] = toHermes3dCronJobs([{ ...agentJob, state: "running" }]);
    expect(typeof job.state.runningAtMs).toBe("number");
  });

  it("surfaces a failure so the board can flag it", async () => {
    const { toHermes3dCronJobs } = await import("../../server/hermes-agent/bridge");
    const [job] = toHermes3dCronJobs([
      { ...agentJob, last_status: "error", last_fire_error: "boom" },
    ]);
    expect(job.state.lastStatus).toBe("error");
    expect(job.state.lastError).toBe("boom");
  });

  it("drops rows with no id and tolerates junk", async () => {
    const { toHermes3dCronJobs } = await import("../../server/hermes-agent/bridge");
    expect(toHermes3dCronJobs([{}, null, "nope", { name: "no id" }])).toEqual([]);
    expect(toHermes3dCronJobs(undefined)).toEqual([]);
  });

  it("reads the other schedule spellings hermes-agent emits", async () => {
    const { toHermes3dSchedule } = await import("../../server/hermes-agent/bridge");
    expect(toHermes3dSchedule("30m")).toEqual({ kind: "every", everyMs: 1_800_000 });
    expect(toHermes3dSchedule("every 2h")).toEqual({ kind: "every", everyMs: 7_200_000 });
    expect(toHermes3dSchedule("0 9 * * *")).toEqual({ kind: "cron", expr: "0 9 * * *" });
    expect(toHermes3dSchedule("2026-06-01T09:00:00Z")).toEqual({
      kind: "at",
      at: "2026-06-01T09:00:00Z",
    });
  });
});

describe("native cron bridge", () => {
  it("translates schedules and delivery while scoping named profiles", async () => {
    const {
      toNativeCronDelivery,
      toNativeCronParams,
      toNativeCronSchedule,
    } = await import("../../server/hermes-agent/bridge");

    expect(toNativeCronSchedule({ kind: "at", at: "2026-06-01T09:00:00Z" })).toBe(
      "2026-06-01T09:00:00Z",
    );
    expect(toNativeCronSchedule({ kind: "every", everyMs: 7_200_000 })).toBe("every 120m");
    expect(toNativeCronSchedule({ kind: "cron", expr: "0 9 * * *" })).toBe("0 9 * * *");
    expect(toNativeCronDelivery({ mode: "announce", channel: "telegram", to: "chat-1" })).toBe(
      "telegram:chat-1",
    );
    expect(toNativeCronDelivery({ mode: "none" })).toBe("local");

    expect(
      toNativeCronParams(
        {
          name: "Daily",
          schedule: { kind: "every", everyMs: 3_600_000 },
          payload: { kind: "agentTurn", message: "Summarize updates." },
          delivery: { mode: "announce", channel: "origin" },
        },
        { profile: "coordinator" },
        "add",
      ),
    ).toEqual({
      action: "add",
      profile: "coordinator",
      name: "Daily",
      schedule: "every 60m",
      prompt: "Summarize updates.",
      deliver: "origin",
    });

    expect(toNativeCronParams({ jobId: "job-1" }, { profile: "" }, "run")).toEqual({
      action: "run",
      name: "job-1",
    });
  });

  it("uses the profile REST API with exact paths and filters disabled rows", async () => {
    const { createHermesAgentProfileApi } = await import("../../server/hermes-agent/profile-api");
    const rest = await startFakeProfileApi();
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({
        profiles: [
          { name: "default", is_default: true, path: "/home/hermes/.hermes" },
          { name: "coordinator", is_default: false, path: "/home/hermes/.hermes/profiles/coordinator" },
        ],
      }),
    });
    const bridge = await openBridge(
      agent.url,
      "",
      createHermesAgentProfileApi({ url: rest.url, token: "[REDACTED]" }),
    );
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");

    bridge.send({ type: "req", id: "l1", method: "cron.list", params: { includeDisabled: false } });
    const listed = await bridge.waitFor((f) => f.type === "res" && f.id === "l1", "cron.list");
    expect(at(listed, "payload.jobs")).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: "default-job", agentId: "default" }),
        expect.objectContaining({ id: "coordinator-job", agentId: "coordinator" }),
      ]),
    );
    expect(at(listed, "payload.jobs")).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ id: "default-job-disabled" })]),
    );

    bridge.send({
      type: "req",
      id: "a1",
      method: "cron.add",
      params: {
        agentId: "coordinator",
        name: "New job",
        schedule: { kind: "every", everyMs: 3_600_000 },
        payload: { kind: "agentTurn", message: "Do work" },
        delivery: { mode: "none" },
      },
    });
    await bridge.waitFor((f) => f.type === "res" && f.id === "a1", "cron.add");

    bridge.send({
      type: "req",
      id: "u1",
      method: "cron.update",
      params: {
        agentId: "coordinator",
        id: "coordinator-job",
        name: "Updated job",
        payload: { kind: "systemEvent", text: "Updated event" },
      },
    });
    await bridge.waitFor((f) => f.type === "res" && f.id === "u1", "cron.update");

    bridge.send({ type: "req", id: "r1", method: "cron.run", params: { id: "coordinator-job" } });
    await bridge.waitFor((f) => f.type === "res" && f.id === "r1", "cron.run");
    bridge.send({ type: "req", id: "d1", method: "cron.remove", params: { id: "coordinator-job" } });
    await bridge.waitFor((f) => f.type === "res" && f.id === "d1", "cron.remove");

    expect(rest.requests.map(({ method, path }) => ({ method, path }))).toEqual([
      { method: "GET", path: "/api/cron/jobs?profile=default&include_disabled=false" },
      { method: "GET", path: "/api/cron/jobs?profile=coordinator&include_disabled=false" },
      { method: "POST", path: "/api/cron/jobs?profile=coordinator" },
      { method: "GET", path: "/api/cron/jobs?profile=coordinator" },
      { method: "PUT", path: "/api/cron/jobs/coordinator-job?profile=coordinator" },
      { method: "GET", path: "/api/cron/jobs?profile=default" },
      { method: "GET", path: "/api/cron/jobs?profile=coordinator" },
      { method: "POST", path: "/api/cron/jobs/coordinator-job/trigger?profile=coordinator" },
      { method: "GET", path: "/api/cron/jobs?profile=default" },
      { method: "GET", path: "/api/cron/jobs?profile=coordinator" },
      { method: "DELETE", path: "/api/cron/jobs/coordinator-job?profile=coordinator" },
    ]);
    expect(rest.requests[2].body).toEqual({
      name: "New job",
      schedule: "every 60m",
      prompt: "Do work",
      deliver: "local",
    });
    expect(rest.requests[4].body).toEqual({ updates: { name: "Updated job", prompt: "Updated event" } });
  });

  it("rejects invalid create and update payloads before contacting REST", async () => {
    const { createHermesAgentProfileApi } = await import("../../server/hermes-agent/profile-api");
    const rest = await startFakeProfileApi();
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: [{ name: "coordinator", path: "/home/hermes/.hermes/profiles/coordinator" }] }),
    });
    const bridge = await openBridge(
      agent.url,
      "",
      createHermesAgentProfileApi({ url: rest.url, token: "[REDACTED]" }),
    );
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");

    for (const [id, method, params] of [
      ["bad-kind", "cron.add", { agentId: "coordinator", name: "bad", schedule: { kind: "every", everyMs: 60_000 }, payload: { kind: "unknown", message: "x" } }],
      ["empty-prompt", "cron.add", { agentId: "coordinator", name: "bad", schedule: { kind: "every", everyMs: 60_000 }, payload: { kind: "agentTurn", message: "  " } }],
      ["bad-update", "cron.update", { agentId: "coordinator", id: "coordinator-job", payload: { kind: "systemEvent", text: "" } }],
    ] as const) {
      bridge.send({ type: "req", id, method, params });
      const frame = await bridge.waitFor((f) => f.type === "res" && f.id === id, method);
      expect(at(frame, "error.code")).toBe(`hermes_agent.cron_${method.slice("cron.".length)}_failed`);
    }
    expect(rest.requests).toEqual([]);
  });

  it("falls back to supported JSON-RPC actions for the default profile", async () => {
    const calls: Frame[] = [];
    const profiles = [
      { name: "default", is_default: true, path: "/home/hermes/.hermes" },
    ];
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles }),
      "cron.manage": (params) => {
        calls.push(params);
        if (params.action === "list") {
          return {
            jobs: [
              {
                job_id: "default-job",
                name: "Default job",
                schedule: "every 1h",
                prompt_preview: "Do work",
                deliver: "local",
              },
            ],
          };
        }
        return { success: true, job: { job_id: "new-job", name: params.name, schedule: params.schedule, prompt_preview: params.prompt } };
      },
    });
    const bridge = await openBridge(agent.url);
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");

    bridge.send({ type: "req", id: "l1", method: "cron.list", params: {} });
    const listed = await bridge.waitFor((f) => f.type === "res" && f.id === "l1", "cron.list");
    expect(at(listed, "payload.jobs")).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: "default-job", agentId: "default" }),
      ]),
    );
    expect(calls.filter((call) => call.action === "list")).toEqual([
      { action: "list", include_disabled: true },
    ]);

    bridge.send({
      type: "req",
      id: "a1",
      method: "cron.add",
      params: {
        agentId: "default",
        name: "New job",
        schedule: { kind: "every", everyMs: 3_600_000 },
        payload: { kind: "agentTurn", message: "Do work" },
        delivery: { mode: "none" },
      },
    });
    await bridge.waitFor((f) => f.type === "res" && f.id === "a1", "cron.add");
    expect(calls.at(-1)).toEqual({
      action: "add",
      name: "New job",
      schedule: "every 60m",
      prompt: "Do work",
      deliver: "local",
    });

    bridge.send({ type: "req", id: "r1", method: "cron.pause", params: { id: "default-job" } });
    await bridge.waitFor((f) => f.type === "res" && f.id === "r1", "cron.pause");
    expect(calls.at(-1)).toEqual({ action: "pause", name: "default-job" });
  });
  it("fails closed for a named profile when REST is unavailable", async () => {
    const rpcCalls: Frame[] = [];
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({
        profiles: [
          { name: "default", is_default: true, path: "/home/hermes/.hermes" },
          { name: "coordinator", is_default: false, path: "/home/hermes/.hermes/profiles/coordinator" },
        ],
      }),
      "cron.manage": (params) => { rpcCalls.push(params); return { jobs: [] }; },
    });
    const profileApi = {
      listCronJobs: async () => { throw new Error("connect ECONNREFUSED"); },
    };
    const bridge = await openBridge(agent.url, "", profileApi);
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");
    bridge.send({ type: "req", id: "l1", method: "cron.list", params: { agentId: "coordinator" } });
    const frame = await bridge.waitFor((f) => f.type === "res" && f.id === "l1", "cron.list");
    expect(at(frame, "error.code")).toBe("hermes_agent.cron_list_failed");
    expect(String(at(frame, "error.message"))).toContain("ECONNREFUSED");
    expect(rpcCalls).toEqual([]);
  });

  it("does not use profile-blind RPC fallback for unsupported actions", async () => {
    const rpcCalls: Frame[] = [];
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: [{ name: "default", is_default: true, path: "/home/hermes/.hermes" }] }),
      "cron.manage": (params) => { rpcCalls.push(params); return { success: true }; },
    });
    const profileApi = {
      listCronJobs: async () => [{ job_id: "job-1", name: "Job", schedule: "every 1h", prompt: "Work" }],
      triggerCronJob: async () => { throw new Error("HTTP 404: not found"); },
    };
    const bridge = await openBridge(agent.url, "", profileApi);
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");
    bridge.send({ type: "req", id: "r1", method: "cron.run", params: { agentId: "default", id: "job-1" } });
    const frame = await bridge.waitFor((f) => f.type === "res" && f.id === "r1", "cron.run");
    expect(at(frame, "error.code")).toBe("hermes_agent.cron_run_failed");
    expect(String(at(frame, "error.message"))).toContain('does not support the "run" action');
    expect(rpcCalls).toEqual([]);
  });

  it("rejects duplicate job ids across profiles without guessing ownership", async () => {
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({
        profiles: [
          { name: "default", is_default: true, path: "/home/hermes/.hermes" },
          { name: "coordinator", is_default: false, path: "/home/hermes/.hermes/profiles/coordinator" },
        ],
      }),
    });
    const profileApi = {
      listCronJobs: async () => [{ job_id: "shared-job", name: "Shared", schedule: "every 1h", prompt: "Work" }],
    };
    const bridge = await openBridge(agent.url, "", profileApi);
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");
    bridge.send({ type: "req", id: "l1", method: "cron.list", params: {} });
    const frame = await bridge.waitFor((f) => f.type === "res" && f.id === "l1", "cron.list");
    expect(at(frame, "error.code")).toBe("hermes_agent.cron_list_failed");
    expect(String(at(frame, "error.message"))).toContain("Ambiguous cron job ownership");
  });

  it("fails closed when an unscoped ownership scan has incomplete profile evidence", async () => {
    const triggerCalls: string[] = [];
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({
        profiles: [
          { name: "default", is_default: true, path: "/home/hermes/.hermes" },
          { name: "coordinator", is_default: false, path: "/home/hermes/.hermes/profiles/coordinator" },
        ],
      }),
    });
    const profileApi = {
      listCronJobs: async (profile: string) => {
        if (profile === "default") throw new Error("default profile unreadable");
        return [{ job_id: "target-job", name: "Target", schedule: "every 1h", prompt: "Work" }];
      },
      triggerCronJob: async (profile: string) => {
        triggerCalls.push(profile);
        return { job_id: "target-job", name: "Target", schedule: "every 1h", prompt: "Work" };
      },
    };
    const bridge = await openBridge(agent.url, "", profileApi);
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");

    bridge.send({ type: "req", id: "r1", method: "cron.run", params: { id: "target-job" } });
    const frame = await bridge.waitFor((f) => f.type === "res" && f.id === "r1", "cron.run");
    expect(at(frame, "error.code")).toBe("hermes_agent.cron_run_failed");
    expect(String(at(frame, "error.message"))).toContain("default profile unreadable");
    expect(triggerCalls).toEqual([]);
  });

  it("does not let an explicitly scoped name cache bias a later unscoped mutation", async () => {
    const updateCalls: string[] = [];
    const triggerCalls: string[] = [];
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({
        profiles: [
          { name: "default", is_default: true, path: "/home/hermes/.hermes" },
          { name: "coordinator", is_default: false, path: "/home/hermes/.hermes/profiles/coordinator" },
        ],
      }),
    });
    const profileApi = {
      listCronJobs: async (profile: string) => [
        {
          job_id: `${profile}-job`,
          name: "Shared name",
          schedule: "every 1h",
          prompt: "Work",
        },
      ],
      updateCronJob: async (profile: string) => {
        updateCalls.push(profile);
        return { job_id: `${profile}-job`, name: "Shared name", schedule: "every 1h", prompt: "Work" };
      },
      triggerCronJob: async (profile: string) => {
        triggerCalls.push(profile);
        return { job_id: `${profile}-job`, name: "Shared name", schedule: "every 1h", prompt: "Work" };
      },
    };
    const bridge = await openBridge(agent.url, "", profileApi);
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");

    bridge.send({
      type: "req",
      id: "u1",
      method: "cron.update",
      params: { agentId: "default", id: "Shared name", name: "Shared name" },
    });
    const updated = await bridge.waitFor((f) => f.type === "res" && f.id === "u1", "cron.update");
    expect(updated.ok).toBe(true);
    expect(updateCalls).toEqual(["default"]);

    bridge.send({ type: "req", id: "r1", method: "cron.run", params: { id: "Shared name" } });
    const frame = await bridge.waitFor((f) => f.type === "res" && f.id === "r1", "cron.run");
    expect(at(frame, "error.code")).toBe("hermes_agent.cron_run_failed");
    expect(String(at(frame, "error.message"))).toContain("Ambiguous cron job ownership");
    expect(triggerCalls).toEqual([]);
  });

  it("invalidates cached ownership when the profile roster is renamed or deleted", async () => {
    let profiles = [
      { name: "default", display_name: "Default", is_default: true, path: "/home/hermes/.hermes" },
      { name: "coordinator", display_name: "Coordinator", is_default: false, path: "/home/hermes/.hermes/profiles/coordinator" },
    ];
    const triggerCalls: string[] = [];
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles }),
    });
    const profileApi = {
      listProfiles: async () => profiles,
      renameProfile: async () => {
        profiles = profiles.map((profile) =>
          profile.name === "coordinator"
            ? { ...profile, name: "dispatcher", display_name: "Dispatcher", path: "/home/hermes/.hermes/profiles/dispatcher" }
            : profile,
        );
        return { name: "dispatcher", path: "/home/hermes/.hermes/profiles/dispatcher" };
      },
      deleteProfile: async (name: string) => {
        profiles = profiles.filter((profile) => profile.name !== name);
        return { ok: true };
      },
      listCronJobs: async (profile: string) =>
        profile === "default"
          ? []
          : [{ job_id: "coordinator-job", name: "Coordinator job", schedule: "every 1h", prompt: "Work" }],
      triggerCronJob: async (profile: string) => {
        triggerCalls.push(profile);
        return { job_id: "coordinator-job", name: "Coordinator job", schedule: "every 1h", prompt: "Work" };
      },
    };
    const bridge = await openBridge(agent.url, "", profileApi);
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");

    bridge.send({ type: "req", id: "l1", method: "cron.list", params: {} });
    const listed = await bridge.waitFor((f) => f.type === "res" && f.id === "l1", "cron.list");
    expect(listed.ok).toBe(true);

    bridge.send({
      type: "req",
      id: "rename",
      method: "agents.update",
      params: { agentId: "coordinator", name: "Dispatcher" },
    });
    const renamed = await bridge.waitFor((f) => f.type === "res" && f.id === "rename", "agents.update");
    expect(renamed.ok).toBe(true);

    bridge.send({ type: "req", id: "r1", method: "cron.run", params: { id: "coordinator-job" } });
    const ran = await bridge.waitFor((f) => f.type === "res" && f.id === "r1", "cron.run");
    expect(ran.ok).toBe(true);
    expect(triggerCalls).toEqual(["dispatcher"]);

    bridge.send({
      type: "req",
      id: "delete",
      method: "agents.delete",
      params: { agentId: "dispatcher" },
    });
    const deleted = await bridge.waitFor((f) => f.type === "res" && f.id === "delete", "agents.delete");
    expect(deleted.ok).toBe(true);

    bridge.send({ type: "req", id: "r2", method: "cron.run", params: { id: "coordinator-job" } });
    const missing = await bridge.waitFor((f) => f.type === "res" && f.id === "r2", "cron.run deleted");
    expect(at(missing, "error.code")).toBe("hermes_agent.cron_run_failed");
    expect(String(at(missing, "error.message"))).toContain('Unknown cron job "coordinator-job"');
    expect(triggerCalls).toEqual(["dispatcher"]);
  });

  it("preserves semantic backend failures for profile-scoped mutations", async () => {
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: [{ name: "coordinator", is_default: false, path: "/home/hermes/.hermes/profiles/coordinator" }] }),
    });
    const profileApi = {
      createCronJob: async () => ({ success: false, error: "backend rejected coordinator job" }),
    };
    const bridge = await openBridge(agent.url, "", profileApi);
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");
    bridge.send({
      type: "req", id: "a1", method: "cron.add",
      params: { agentId: "coordinator", name: "Job", schedule: { kind: "every", everyMs: 3_600_000 }, payload: { kind: "agentTurn", message: "Work" } },
    });
    const frame = await bridge.waitFor((f) => f.type === "res" && f.id === "a1", "cron.add");
    expect(at(frame, "error.code")).toBe("hermes_agent.cron_add_failed");
    expect(String(at(frame, "error.message"))).toContain("backend rejected coordinator job");
  });

});

describe("toHermes3dMessages", () => {
  it("renames text to content and drops non-conversational rows", () => {
    expect(
      toHermes3dMessages([
        { role: "user", text: "hi" },
        { role: "tool", name: "terminal" },
        { role: "assistant", text: "hello" },
      ]),
    ).toEqual([
      { role: "user", content: "hi" },
      { role: "assistant", content: "hello" },
    ]);
  });

  it("returns an empty list for a missing transcript", () => {
    expect(toHermes3dMessages(undefined)).toEqual([]);
  });
});

describe("hermes-agent bridge", () => {
  it("sends the token as a query param on /api/ws", async () => {
    const agent = await startFakeHermesAgent({});
    await openBridge(agent.url, "tok-123");

    expect(agent.received.find((frame) => frame.__connect)).toMatchObject({
      path: "/api/ws",
      token: "tok-123",
    });
  });

  it("answers connect with a hello-ok advertising one agent", async () => {
    const agent = await startFakeHermesAgent({});
    const bridge = await openBridge(agent.url);

    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");

    expect(res.ok).toBe(true);
    expect(at(res, "payload.type")).toBe("hello-ok");
    expect(at(res, "payload.snapshot.health.agents")).toHaveLength(1);
    expect(at(res, "payload.snapshot.health.defaultAgentId")).toBe("hermes");
  });

  it("manages hermes-agent profiles and persists virtual role files without touching default profiles", async () => {
    let profiles = [
      { name: "default", display_name: "Default", is_default: true, path: "/home/hermes/.hermes" },
      { name: "partner", display_name: "Partner", is_default: false, path: "/home/hermes/.hermes/profiles/partner" },
    ];
    const souls = new Map<string, string>([["partner", "Partner soul"]]);
    const profileApi = {
      listProfiles: async () => profiles,
      createProfile: async (displayName: string) => {
        const name = displayName.trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "-");
        const path = `/home/hermes/.hermes/profiles/${name}`;
        profiles = [...profiles, { name, display_name: displayName, is_default: false, path }];
        souls.set(name, "");
        return { name, path };
      },
      renameProfile: async (name: string, displayName: string) => {
        const nextName = displayName.trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "-");
        profiles = profiles.map((profile) =>
          profile.name === name ? { ...profile, name: nextName, display_name: displayName } : profile,
        );
        souls.set(nextName, souls.get(name) ?? "");
        souls.delete(name);
        return { name: nextName, path: `/home/hermes/.hermes/profiles/${nextName}` };
      },
      deleteProfile: async (name: string) => {
        profiles = profiles.filter((profile) => profile.name !== name);
        souls.delete(name);
        return { ok: true };
      },
      getSoul: async (name: string) => ({ content: souls.get(name) ?? "", exists: souls.has(name) }),
      setSoul: async (name: string, content: string) => {
        souls.set(name, content);
        return { ok: true };
      },
    };
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles }),
    });
    const bridge = await openBridge(agent.url, "", profileApi);

    bridge.send({ type: "req", id: "c-profile", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c-profile", "profile connect");

    bridge.send({ type: "req", id: "create-editor", method: "agents.create", params: { name: "Editor" } });
    const created = await bridge.waitFor(
      (f) => f.type === "res" && f.id === "create-editor",
      "agents.create",
    );
    expect(created.ok).toBe(true);
    expect(at(created, "payload.agentId")).toBe("editor");

    bridge.send({
      type: "req",
      id: "write-editor",
      method: "agents.files.set",
      params: { agentId: "editor", name: "AGENTS.md", content: "Editor responsibilities" },
    });
    const written = await bridge.waitFor((f) => f.type === "res" && f.id === "write-editor", "file write");
    expect(written.ok).toBe(true);
    expect(souls.get("editor")).toContain("HERMES3D_FILE:AGENTS.md:BEGIN");
    expect(souls.get("editor")).toContain("Editor responsibilities");

    bridge.send({
      type: "req",
      id: "read-editor",
      method: "agents.files.get",
      params: { agentId: "editor", name: "AGENTS.md" },
    });
    const read = await bridge.waitFor((f) => f.type === "res" && f.id === "read-editor", "file read");
    expect(at(read, "payload.file.content")).toBe("Editor responsibilities");

    bridge.send({ type: "req", id: "delete-default", method: "agents.delete", params: { agentId: "default" } });
    const protectedDefault = await bridge.waitFor(
      (f) => f.type === "res" && f.id === "delete-default",
      "default delete refusal",
    );
    expect(protectedDefault.ok).toBe(false);
    expect(at(protectedDefault, "error.code")).toBe("hermes_agent.default_profile_immutable");

    bridge.send({ type: "req", id: "delete-editor", method: "agents.delete", params: { agentId: "editor" } });
    const deleted = await bridge.waitFor((f) => f.type === "res" && f.id === "delete-editor", "agents.delete");
    expect(deleted.ok).toBe(true);
    expect(profiles.map((profile) => profile.name)).toEqual(["default", "partner"]);
    expect(souls.get("partner")).toBe("Partner soul");
  });

  it("turns chat.send into prompt.submit and streams deltas into chat events", async () => {
    const agent = await startFakeHermesAgent({
      "session.create": () => ({ session_id: "s1", stored_session_id: "stored-1" }),
      "prompt.submit": (_params, emit) => {
        setTimeout(() => {
          emit("message.start", {});
          emit("message.delta", { text: "Hel" });
          emit("message.delta", { text: "lo" });
          emit("message.complete", { text: "Hello", status: "complete" });
        }, 10);
        return { status: "streaming" };
      },
    });
    const bridge = await openBridge(agent.url);

    bridge.send({
      type: "req",
      id: "m1",
      method: "chat.send",
      params: { sessionKey: "agent:hermes:main", message: "hi", idempotencyKey: "run-1" },
    });

    const started = await bridge.waitFor((f) => f.type === "res" && f.id === "m1", "chat.send res");
    expect(started.payload).toMatchObject({ status: "started", runId: "run-1" });

    const submitted = agent.received.find((frame) => frame.method === "prompt.submit");
    expect(submitted?.params).toMatchObject({ session_id: "s1", text: "hi" });

    const final = await bridge.waitFor(
      (f) => f.event === "chat" && at(f, "payload.state") === "final",
      "final chat event",
    );
    expect(at(final, "payload.message")).toEqual({ role: "assistant", content: "Hello" });
    expect(at(final, "payload.runId")).toBe("run-1");

    // Deltas accumulate, so the browser always receives the full text so far.
    const deltas = bridge.frames.filter(
      (f) => f.event === "chat" && at(f, "payload.state") === "delta",
    );
    expect(deltas.map((frame) => at(frame, "payload.message.content"))).toEqual(["Hel", "Hello"]);
  });

  it("reports a failed prompt as an error response rather than a silent hang", async () => {
    const agent = await startFakeHermesAgent({
      "session.create": () => ({ session_id: "s1" }),
    });
    const bridge = await openBridge(agent.url);

    bridge.send({
      type: "req",
      id: "m2",
      method: "chat.send",
      params: { sessionKey: "agent:hermes:main", message: "hi" },
    });

    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "m2", "chat.send failure");
    expect(res.ok).toBe(false);
    expect(at(res, "error.code")).toBe("hermes_agent.prompt_failed");
  });

  it("maps chat.abort onto session.interrupt", async () => {
    const agent = await startFakeHermesAgent({
      "session.create": () => ({ session_id: "s1" }),
      "prompt.submit": () => ({ status: "streaming" }),
      "session.interrupt": () => ({ status: "interrupted" }),
    });
    const bridge = await openBridge(agent.url);

    bridge.send({
      type: "req",
      id: "m3",
      method: "chat.send",
      params: { sessionKey: "agent:hermes:main", message: "hi", idempotencyKey: "run-9" },
    });
    await bridge.waitFor((f) => f.type === "res" && f.id === "m3", "chat.send res");

    bridge.send({ type: "req", id: "a1", method: "chat.abort", params: { runId: "run-9" } });
    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "a1", "abort res");

    expect(res.payload).toMatchObject({ ok: true, aborted: 1 });
    expect(agent.received.some((frame) => frame.method === "session.interrupt")).toBe(true);
  });

  it("surfaces stored hermes-agent sessions alongside the main key", async () => {
    const agent = await startFakeHermesAgent({
      "session.list": () => ({
        sessions: [{ id: "20260409_abc", title: "Yesterday's chat", started_at: 1000 }],
      }),
    });
    const bridge = await openBridge(agent.url);

    bridge.send({ type: "req", id: "s1", method: "sessions.list", params: {} });
    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "s1", "sessions.list");

    const sessions = at(res, "payload.sessions") as { key: string }[];
    expect(sessions.map((session) => session.key)).toEqual(
      expect.arrayContaining(["agent:hermes:main", "agent:hermes:20260409_abc"]),
    );
  });

  it("keeps working when an optional upstream method is unavailable", async () => {
    const agent = await startFakeHermesAgent({});
    const bridge = await openBridge(agent.url);

    bridge.send({ type: "req", id: "k1", method: "models.list", params: {} });
    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "k1", "models.list");

    expect(res.ok).toBe(true);
    expect(at(res, "payload.models")).not.toHaveLength(0);
  });
});
