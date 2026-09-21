---
name: venus-wyrd-connected
description: Run WYRDHOLM connected — build the Linux server / headless-client Docker images from a SHA with a build label, keep Docker disk under control, drive the real dedicated server + headless witnesses + graphical review, run the join probe legs L1-L6, and write the connected receipt. Use for any Stage A/B connected evidence (join, reconnect, encounter, crate, route), never for a Windows server exe.
---

# Rules that already bit us

- Servers run in the Linux container, never as a Windows .exe (house rule).
- Docker's WSL disk (`%LOCALAPPDATA%\Docker\wsl\disk\docker_data.vhdx`) was
  127 GB and the daemon unresponsive on 2026-09-07. Before a build:
  `docker system df`; `docker builder prune -a -f`; remove superseded
  `venus-*` images/containers by name (never `system prune -a`, other
  projects' images live there). The vhdx only shrinks with an elevated
  `wsl --shutdown` + `diskpart compact vdisk` — Ben's call.
- Pre-pull `debian:bookworm` and build with `--pull=false`.
- A receipt names the image's build SHA; `images_pin` must tolerate
  `"Labels": null` (`((info.get("Config") or {}).get("Labels") or {})`).
- Env levers reach the container through `docker run -e` (initial process
  block), so the MSVC `getenv` trap does not apply there; the WINDOWS client
  side still needs setters or ctest ENVIRONMENT.

# Build the images from a SHA

```bash
cd C:/code/Venus && SHA=$(git rev-parse --short HEAD)
docker build --pull=false --build-arg VENUS_TARGET=wyrdholm_server \
  --label venus.build-sha=$SHA -t venus-wyrdholm-server:$SHA -t venus-wyrdholm-server:stage-a .
docker build --pull=false --build-arg VENUS_TARGET=wyrdholm_headless_client \
  --label venus.build-sha=$SHA -t venus-wyrdholm-headless-client:$SHA -t venus-wyrdholm-headless-client:stage-a .
docker image inspect --format '{{json .Config.Labels}}' venus-wyrdholm-server:stage-a
```

The root `Dockerfile` is the only Dockerfile (multi-stage, clang/LLVM only,
`VENUS_TARGET` build-arg). `tools/wyrdholm_net_docker_gate.ps1` reuses an
image when its source digest is unchanged (pass `-ForceBuild` after a
submodule bump); it SKIPs by name (exit 125) when Docker is unreachable.

# Drive a connected run

- Gate shape: `tools/wyrdholm_net_docker_gate.ps1` (server detached, UDP port
  published, persistence bind-mounted, >= 2 witness clients on the host,
  measured wall-clock >= 20 s, ONE verdict table per client + server, nonzero
  on any red row).
- Full review: `tools/run_wyrdholm_review.ps1` (Windows graphical client +
  headless peers via `tools/run_headless_multiplayer.py`, captures
  `wyrd_wyrd_review_tNN.png`, validation probe delivered, `session.json`).
  `--legs encounter,crate` once N2 lands.
- Join probes: `tools/run_wyrdholm_restore_probe.py --leg L1..L6` (fresh
  GROUND_SEATED; RESTORED EXACT; `--case buried` -> CONNECTION_REFUSED pinned;
  UNAVAILABLE for the full COLLISION_WAIT via the server lever; unclean drop
  -> engine 10 s liveness timeout -> disconnect-time save -> RESTORED;
  `VENUS_NET_SIM_LOSS=0.2` on BOTH ends, it is outbound-only). Each leg
  >= 20 s connected after admission.

# What the connected receipt must carry

SHA of both images (label read back), server log lines: join phases by name
(`COLLISION_WAIT`/`BIND_WAIT`/`ACTIVE`), per-seat phase ticks and the shutdown
tally (placement REFUSED / UNAVAILABLE-timeout / bind-timeout counts),
`venus_pworld_terrain(w) == NULL`; client verdict lines per witness (pass,
compared ACKs, corrections p50/p95/max per bucket); all exits 0; validation
probe delivered with 0 VUIDs; durations measured, not requested; captures.
Store under `build-msvc/stage-a-<date>/<run>/` and cite from the MAP.

# Evidence hygiene

One receipt per gate per SHA; a red receipt beside a later green one is
superseded explicitly (`superseded_by`), never left side by side. A client
log that prints a pre-rewrite string (e.g. the old far-tier line) is proof the
binary predates the change — check the SHA line before trusting a run.
