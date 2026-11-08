# Environment Setup Guide

This guide explains how to create the `.env` file used by `deploy.sh`, `cleanup.sh`, and `agent/invoke_agent.py`.

The `.env` file holds local configuration only. It is listed in `.gitignore`, so it is never committed. Do not put AWS access keys in it. The scripts use your normal AWS CLI credentials.

---

## 1. Create the file

From the repository root:

```bash
cp .env.example .env
```

Then open `.env` in an editor and follow the sections below.

---

## 2. Check your AWS identity first

Before filling in values, confirm the AWS CLI is configured for the account and region you want:

```bash
aws sts get-caller-identity
aws configure get region
```

The `Account` field is the account the stack will be deployed into. Everything else in `.env` should be consistent with it.

---

## 3. Variables

### Required core settings

| Variable | Example | Used by | What to set |
| --- | --- | --- | --- |
| `AWS_REGION` | `us-east-1` | all scripts, client | The region to deploy to. Must match the region where Bedrock AgentCore is available for your account. |
| `STACK_NAME` | `sfn-agentcore-sales-demo` | `deploy.sh`, `cleanup.sh`, client | Name of the CloudFormation stack. Pick a unique name if several copies of this demo run in one account. |
| `GATEWAY_NAME` | `sales-data-ops-mcp-gateway` | `deploy.sh` (SAM parameter) | Name for the AgentCore Gateway. |
| `GLUE_JOB_NAME` | `sales-daily-parquet-ingestion-job` | `deploy.sh` (SAM parameter) | Name for the AWS Glue job. |
| `ALERT_EMAIL` | `you@example.com` | `deploy.sh` (SAM parameter) | Email that receives SNS failure alerts. **Change this from the example value.** AWS sends a confirmation email to this address after deploy. You must click the link before alerts arrive. |

If a core variable is left out, `deploy.sh` falls back to the default shown in the table, except `ALERT_EMAIL`, which falls back to a placeholder address.

### Gateway settings (set after the first deploy)

| Variable | Example | Used by | What to set |
| --- | --- | --- | --- |
| `GATEWAY_ID` | `gw-xxxxxxxx` | `deploy.sh`, `cleanup.sh` | ID of an existing AgentCore Gateway. Leave empty on the first deploy. Set it to attach the Step Functions target automatically. |
| `GATEWAY_ENDPOINT_URL` | `https://...` | `invoke_agent.py` | The gateway's MCP endpoint URL. Leave empty to use the local simulation fallback. |
| `TARGET_ID` | `tgt-xxxxxxxx` | `cleanup.sh` | ID of the gateway target created by `deploy.sh`. Needed only if you want `cleanup.sh` to remove the target. |

See section 4 for how to find these values.

### Optional settings

| Variable | Example | Used by | What to set |
| --- | --- | --- | --- |
| `BEDROCK_MODEL_ID` | `openai.gpt-6-1-terra-v1:0` | not read by current code | The example value is a placeholder. The current scripts do not read this variable. You can leave it as is or remove it. |

---

## 4. Fill in the values after deploy

Some values only exist after the stack is deployed. The first run works without them.

### Step A: deploy once without gateway values

Leave `GATEWAY_ID`, `GATEWAY_ENDPOINT_URL`, and `TARGET_ID` empty, then run:

```bash
chmod +x deploy.sh cleanup.sh
./deploy.sh
```

At the end, `deploy.sh` prints the data bucket, state machine ARN, and gateway role ARN. It also writes `gateway/target.rendered.json`, and prints the CLI command to attach the target if `GATEWAY_ID` is not set.

Read the stack outputs at any time with:

```bash
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$AWS_REGION" \
  --query "Stacks[0].Outputs" \
  --output table
```

### Step B: create or find an AgentCore Gateway

The gateway is created in the Amazon Bedrock AgentCore console for your region. Copy its ID into `GATEWAY_ID`.

If you already have a gateway, use its existing ID.

Then either:

- run `./deploy.sh` again. With `GATEWAY_ID` set, the script registers the Step Functions target automatically, or
- run the `aws bedrock-agentcore create-gateway-target` command that `deploy.sh` prints, using `gateway/target.rendered.json`.

### Step C: fill in the endpoint and target IDs

- `GATEWAY_ENDPOINT_URL`: copy the MCP endpoint URL shown for the gateway in the AgentCore console.
- `TARGET_ID`: copy the ID of the Step Functions target after it is created.

Save `.env` when done.

---

## 5. Verify the setup

Run these checks from the repository root.

```bash
# 1. Required tools
aws --version && sam --version && python3 --version && jq --version

# 2. Credentials and region
aws sts get-caller-identity
echo "$AWS_REGION"    # run after: set -a; source .env; set +a

# 3. Client smoke test (uses GATEWAY_ENDPOINT_URL if set, otherwise simulation)
python3 agent/invoke_agent.py \
  --prompt "Start the sales ingestion pipeline for raw/sales/2026-11-05/sales.parquet and tell me if it succeeds."
```

To load `.env` into your current shell for manual commands:

```bash
set -a
source .env
set +a
```

The deploy and cleanup scripts do this automatically.

---

## 6. Common mistakes

- **Wrong region.** `AWS_REGION` must match the region where the stack and gateway live. A mismatch causes "stack not found" or "gateway not found" errors.
- **Stack name reused.** If `STACK_NAME` matches an existing stack, `deploy.sh` updates that stack. It does not create a new one.
- **Alert email not confirmed.** SNS sends a confirmation email to `ALERT_EMAIL`. Failure alerts are not delivered until the subscription is confirmed.
- **Gateway values filled too early.** Setting `GATEWAY_ID` before the gateway exists makes `deploy.sh` attempt to register a target and print a warning. Create the gateway first.
- **Committing `.env`.** It is already ignored by git. Confirm with `git check-ignore .env`. It should print `.env`.
- **Editing `.env.example` with real values.** Keep real values only in `.env`. Edit `.env.example` only to change the defaults for everyone.

---

## 7. Quick reference: complete example

```bash
# .env
AWS_REGION=us-east-1
STACK_NAME=sfn-agentcore-sales-demo
GATEWAY_NAME=sales-data-ops-mcp-gateway
GLUE_JOB_NAME=sales-daily-parquet-ingestion-job
ALERT_EMAIL=you@example.com

# Fill after deploy and gateway creation
GATEWAY_ID=
GATEWAY_ENDPOINT_URL=
TARGET_ID=
```
