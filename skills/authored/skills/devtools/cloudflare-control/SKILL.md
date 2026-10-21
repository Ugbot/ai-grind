---
name: cloudflare-control
description: Coordinate installed official Cloudflare skills, MCP and CLI access across machines; diagnose authentication boundaries, select an account, manage bounded R2 workflows, and prepare secret-free S3 test configuration.
---

# Cloudflare control in ai-grind

Use the official `cloudflare` and product skills for platform instructions. This
skill coordinates existing access and test setup; load `cf-cli` or `wrangler`
for command details. Keep source skills authoritative and their generated
catalog/plugin copies unedited.

1. Identify the requested account, resource and operation. Read the project's
   working agreement and existing Cloudflare configuration. Use `cf` unless a
   Wrangler configuration selects Wrangler; follow the upstream skill when both
   configuration formats exist. Preserve unrelated resources and active edits.
2. Check the installed tool's actual version, `--help` and schema before using
   documented beta commands. Observed cf0.15.0 lacks `cf cli search`; use actual
   command help and `cf schema`, and report unsupported discovery faithfully.
   Do not install another CLI or migrate configuration to work around that gap.
3. Treat MCP, cf, Wrangler and R2 S3 authentication as separate boundaries. Use
   their official status/reauth flows. Successful Wrangler/cf login does not
   prove MCP or S3 access. Never read credential stores, export auth tokens into
   logs, create another credential store or implement OAuth refresh here.
4. Use official Cloudflare MCP at `https://mcp.cloudflare.com/mcp`, or the
   selected official CLI, for account discovery and R2 control. Read current
   schema/help, explicitly select the account, bound pagination/output, and
   record the target and read-back result. A truncated list is incomplete.
   Carry existing session authorization; resource writes require the requested
   target/action, and deletes require their explicit authorization. No retries
   or resource creation should be hidden inside status/configuration checks.
5. Prepare S3 configuration with the bundled helper only after selecting an
   account/bucket. It emits endpoint/region and environment-variable names,
   never credentials. Presence is not validity; verify permissions with the
   intended S3 client before declaring integration ready.

[Access and R2 workflow](references/access-and-r2.md) covers official setup,
independent authentication, beta discovery, and bounded resource operations.
[Helper contract](references/helper-and-publication.md) covers UV invocation,
secret-free output, platform limits and selective library publication.

The [native Codex role](agents/cloudflare-controller.toml) is a runner asset.
The authored `cloudflare-controller.md` is a Claude agent. Markdown sync does
not register native Codex roles or install a background scheduler.
