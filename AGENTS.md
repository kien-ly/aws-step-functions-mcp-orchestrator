# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project

`stepfunctions-agentcore-demo` — a reference implementation that exposes an AWS Step Functions Standard Workflow as an MCP tool through Amazon Bedrock AgentCore Gateway (`connectorId: "states"`).

Deep-dive article: `docs/article.md`.

## Layout

- `template.yaml` — AWS SAM stack: S3, Glue job, DynamoDB audit table, SNS topic, Step Functions state machine, least-privilege IAM.
- `state-machine/` — Step Functions definition (ValidateInput → StartGlueJob → RecordSuccess, with NotifyFailure → WorkflowFailed).
- `gateway/target.json` — AgentCore Gateway target config (Step Functions connector, pinned `stateMachineArn`).
- `agent/invoke_agent.py` — Python MCP client for `tools/list` and `tools/call`, with a local simulation fallback.
- `deploy.sh` / `cleanup.sh` — deploy and tear-down scripts (deploy renders the gateway target config).
- `.env.example` — copy to `.env` before deploying.
- `requirements.txt` — Python dependencies.
- `.agents/` — local agent skills and generated graph output (`.agents/graphify-out/`). Not part of the application.

## Working Rules

- Keep IAM least-privilege. Scope state-machine actions to the single state machine ARN and execution actions to `execution:${StateMachineName}:*`.
- Do not hardcode AWS account IDs, regions, or ARNs; use `.env` values and SAM parameters.
- Do not commit `.env` or any credentials.
- Changes to the state machine must keep `WorkflowFailed` as an explicit `Fail` state so `DescribeExecution` reports `FAILED` correctly.
- Prefer editing existing files over adding new top-level files.

## Commands

```bash
cp .env.example .env
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
chmod +x deploy.sh cleanup.sh
./deploy.sh
python3 agent/invoke_agent.py --prompt "Start the sales ingestion pipeline for raw/sales/2026-11-05/sales.parquet and tell me if it succeeds."
./cleanup.sh
```

## Prerequisites

AWS CLI v2.15+, AWS SAM CLI v1.115+, Python 3.10+, jq 1.6+, and a configured identity (`aws sts get-caller-identity`).

<!-- setup-py:start -->
## Python

- Use `uv` only. Do not use `pip`, `poetry`, `pipenv`, `conda`, or `python -m venv`.
- The virtual environment is `.venv/` in the project root, managed by uv.
- `pyproject.toml` is the source of truth for dependencies. Add packages with `uv add` / `uv add --dev`.
- Python version is pinned in `.python-version`.
- Run commands with `uv run ...`. Sync with `uv sync`.
- Never commit `.venv/`.
- Full rules: `.agents/rules/python.md`.
<!-- setup-py:end -->
