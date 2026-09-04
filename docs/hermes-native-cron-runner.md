# Native Hermes Cron runner rollout

Status: planned only. H3D-004 does not authorize a production deploy, restart, Scheduled Task, credential change, or real Cron trigger.

## Decision

Use Hermes' installed one-shot native scheduler tick from a Coolify Scheduled Task. Do not add a standalone application poller. Start with the coordinator profile only.

The production image currently starts the Hermes web server with embedded chat and does not start the Hermes Gateway ticker. The full Gateway also starts other adapters, so it must not be enabled beside embedded chat until that combined topology is reviewed separately.

## Planned runner

Create one Coolify Scheduled Task for the Hermes3D Office backend, every minute, with this exact command:

```sh
setpriv --reuid=1000 --regid=1000 --clear-groups env HOME=/home/hermes /opt/hermes/.venv/bin/hermes --profile coordinator cron tick
```

This calls Hermes' native `cron.scheduler.tick`. Its profile lock prevents overlapping ticks. Do not run the task as the repository user: the persistent Hermes home and credentials are owned by numeric UID:GID 1000:1000.

## Ownership and persistence preflight

Before enabling the task, verify without changing data:

- `/home/hermes/.hermes` remains the mounted persistent Hermes home.
- Coordinator state remains under `/home/hermes/.hermes/profiles/coordinator`.
- Coordinator Cron state remains under `/home/hermes/.hermes/profiles/coordinator/cron`.
- The runner resolves as UID:GID 1000:1000 and can read the profile state without exposing secrets.
- Only one scheduled runner exists for the coordinator profile.

## Controlled activation

1. Deploy the reviewed Hermes3D revision through the normal production gate.
2. Verify the Office backend and all seven Agents before enabling any schedule.
3. Add the coordinator-only Scheduled Task in a disabled state if Coolify supports it; review its command and interval, then enable it.
4. Observe at least two tick intervals with a harmless pre-approved coordinator test job.
5. Add per-profile native tick tasks only after coordinator ownership and non-duplication are proven. Each task must pass its own explicit `--profile`.

## Verification

- Scheduled Task logs show successful one-shot exits and no permission errors.
- Hermes scheduler lock shows no overlapping execution.
- The coordinator job updates `last_run` and `next_run` exactly once per due interval.
- Office lists the job under Coordinator and mutations remain profile-scoped through REST.
- A read-only `ai-task` check from Coordinator succeeds without modifying task state.
- Restart persistence is verified separately: restart only after authorization, then confirm the task definition and profile Cron files remain and no duplicate run occurs.

## Rollback

Disable or remove only the Coolify Scheduled Task. Do not delete profile Cron jobs or persistent files. Confirm no tick process remains, then verify the web server and seven Agents are unchanged. If the bridge revision itself must be rolled back, use Coolify's prior known-good image after recording the failed revision and evidence.
