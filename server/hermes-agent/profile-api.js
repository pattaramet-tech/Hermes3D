"use strict";

const http = require("node:http");
const https = require("node:https");

const PROFILE_NAME_RE = /^[a-z0-9][a-z0-9_-]{0,63}$/;

const resolveHttpBaseUrl = (baseUrl) => {
  const raw = typeof baseUrl === "string" ? baseUrl.trim() : "";
  if (!raw) throw new Error("hermes-agent URL is empty.");
  const parsed = new URL(raw);
  if (parsed.protocol === "ws:") parsed.protocol = "http:";
  else if (parsed.protocol === "wss:") parsed.protocol = "https:";
  else if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error(`Unsupported scheme "${parsed.protocol}" for a hermes-agent URL.`);
  }
  parsed.pathname = "/";
  parsed.search = "";
  parsed.hash = "";
  return parsed;
};

const profileNameFromDisplay = (value) => {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64);
  if (!PROFILE_NAME_RE.test(normalized)) {
    throw new Error("Agent name did not produce a valid hermes-agent profile name.");
  }
  return normalized;
};

const encodeProfile = (name) => encodeURIComponent(String(name ?? "").trim());

const requestJson = ({ baseUrl, token, method, path, body }) => {
  const base = resolveHttpBaseUrl(baseUrl);
  const transport = base.protocol === "https:" ? https : http;
  const payload = body === undefined ? "" : JSON.stringify(body);
  const headers = {
    Accept: "application/json",
    Host: "localhost",
  };
  if (typeof token === "string" && token.trim()) {
    headers["X-Hermes-Session-Token"] = token.trim();
  }
  if (payload) {
    headers["Content-Type"] = "application/json";
    headers["Content-Length"] = Buffer.byteLength(payload);
  }

  return new Promise((resolve, reject) => {
    const req = transport.request(
      {
        hostname: base.hostname,
        port: base.port ? Number(base.port) : base.protocol === "https:" ? 443 : 80,
        method,
        path,
        headers,
      },
      (res) => {
        const chunks = [];
        res.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        res.on("end", () => {
          const raw = Buffer.concat(chunks).toString("utf8");
          let decoded = {};
          if (raw.trim()) {
            try {
              decoded = JSON.parse(raw);
            } catch {
              decoded = { detail: "Invalid JSON response from hermes-agent." };
            }
          }
          const status = Number(res.statusCode ?? 0);
          if (status >= 200 && status < 300) {
            resolve(decoded);
            return;
          }
          const detail =
            typeof decoded?.detail === "string"
              ? decoded.detail
              : typeof decoded?.message === "string"
                ? decoded.message
                : `HTTP ${status || "error"}`;
          reject(new Error(`hermes-agent ${method} ${path} failed (HTTP ${status}): ${detail}`));
        });
      },
    );
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
};

const createHermesAgentProfileApi = ({ url, token }) => ({
  async listProfiles() {
    const result = await requestJson({ baseUrl: url, token, method: "GET", path: "/api/profiles" });
    return Array.isArray(result?.profiles) ? result.profiles : [];
  },

  async createProfile(displayName) {
    const name = profileNameFromDisplay(displayName);
    const result = await requestJson({
      baseUrl: url,
      token,
      method: "POST",
      path: "/api/profiles",
      body: { name, clone_from_default: true, no_skills: false },
    });
    return { name: String(result?.name || name), path: String(result?.path || "") };
  },

  async renameProfile(name, displayName) {
    const nextName = profileNameFromDisplay(displayName);
    const result = await requestJson({
      baseUrl: url,
      token,
      method: "PATCH",
      path: `/api/profiles/${encodeProfile(name)}`,
      body: { new_name: nextName },
    });
    return { name: String(result?.name || nextName), path: String(result?.path || "") };
  },

  async deleteProfile(name) {
    return requestJson({
      baseUrl: url,
      token,
      method: "DELETE",
      path: `/api/profiles/${encodeProfile(name)}`,
    });
  },

  async getSoul(name) {
    const result = await requestJson({
      baseUrl: url,
      token,
      method: "GET",
      path: `/api/profiles/${encodeProfile(name)}/soul`,
    });
    return {
      content: typeof result?.content === "string" ? result.content : "",
      exists: result?.exists === true,
    };
  },

  async setSoul(name, content) {
    return requestJson({
      baseUrl: url,
      token,
      method: "PUT",
      path: `/api/profiles/${encodeProfile(name)}/soul`,
      body: { content: String(content ?? "") },
    });
  },
});

module.exports = {
  PROFILE_NAME_RE,
  resolveHttpBaseUrl,
  profileNameFromDisplay,
  requestJson,
  createHermesAgentProfileApi,
};
