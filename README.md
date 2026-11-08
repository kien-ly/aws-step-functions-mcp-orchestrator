# stepfunctions-agentcore-demo

A production-minded reference implementation demonstrating how to expose an **AWS Step Functions Standard Workflow** as an **MCP (Model Context Protocol) tool** using **Amazon Bedrock AgentCore Gateway** (`connectorId: "states"`).

> **Documentation:** For the comprehensive deep-dive blog article and architectural analysis, see [docs/article.md](docs/article.md).

## Architecture Summary

1. **AWS Step Functions Standard Workflow** (`SalesIngestionStateMachine`):
   * `ValidateInput`: Verifies that `$.sourceKey` is present and matches `raw/sales/*.parquet`.
   * `StartGlueJob`: Runs an AWS Glue PySpark job synchronously (`arn:aws:states:::glue:startJobRun.sync`) with exponential backoff `Retry` and error `Catch` handling.
   * `RecordSuccess`: Writes execution audit metadata to an Amazon DynamoDB table.
   * `NotifyFailure` -> `WorkflowFailed`: Publishes operational alerts to Amazon SNS and terminates in an explicit `Fail` state so `DescribeExecution` accurately reports `FAILED`.
2. **Amazon Bedrock AgentCore Gateway Target** (`gateway/target.json`):
   * Uses the built-in Step Functions connector (`"connectorId": "states"`).
   * Pins `stateMachineArn` for `StartExecution`, `ListExecutions`, and `DescribeStateMachine`.
   * Exposes execution-level tools (`DescribeExecution`, `GetExecutionHistory`, `StopExecution`).
3. **Least-Privilege IAM** (`template.yaml`):
   * Scopes state-machine-level actions to the single state machine ARN.
   * Scopes execution-level actions to `arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:execution:${StateMachineName}:*`.

---

## Prerequisites

Verify that the following tools are installed and configured before deploying:

```bash
aws --version          # AWS CLI v2.15+ (with bedrock-agentcore support)
sam --version          # AWS SAM CLI v1.115+
python3 --version      # Python 3.10+
jq --version           # jq 1.6+ for JSON rendering in deploy.sh
aws sts get-caller-identity

```

---

## Step 1: Configuration

Copy the sample environment file and customize if needed:

```bash
cp .env.example .env

```

install the Python dependencies inside a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

```

---

## Step 2: Deployment

Run the automated deployment script. It provisions the AWS SAM stack (S3 bucket, sample Glue PySpark script, Glue job, DynamoDB audit table, SNS topic, Step Functions Standard state machine, and least-privilege IAM roles), uploads a sample Parquet file to `raw/sales/2026-11-05/sales.parquet`, and renders the AgentCore Gateway target configuration:

```bash
chmod +x deploy.sh cleanup.sh
./deploy.sh

```

---
## Step 3: Invocation & Validation

Run the Python agent client to test the MCP `tools/list` and `tools/call` workflow against the AgentCore Gateway (including a local simulation fallback if your Gateway endpoint URL is pending provisioning):

```bash
python3 agent/invoke_agent.py \
  --prompt "Start the sales ingestion pipeline for raw/sales/2026-11-05/sales.parquet and tell me if it succeeds."

```

### Manual Validation Commands

You can also inspect the state machine executions and DynamoDB audit records directly via the AWS CLI:

```bash
source .env
STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" \
  --output text)

# 1. List recent executions
aws stepfunctions list-executions \
  --region "$AWS_REGION" \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --max-items 5

# 2. Test input validation guardrails (should fail immediately in ValidateInput without starting Glue)
aws stepfunctions start-execution \
  --region "$AWS_REGION" \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --input '{"sourceKey": "untrusted/bucket/hack.csv"}'

```

---

## Step 4: Cleanup

To delete the AgentCore Gateway target, empty the S3 artifacts bucket, and tear down the CloudFormation stack cleanly:

```bash
./cleanup.sh

```

---

## Troubleshooting Common Deployment Errors

### 1. `AccessDeniedException` on `states:DescribeExecution` or `states:GetExecutionHistory`

* **Symptom:** `StartExecution` succeeds and returns an `executionArn`, but subsequent status checks fail with an IAM authorization error.
* **Cause:** Execution-level operations in Step Functions evaluate against `:execution:<StateMachineName>:<ExecutionName>` rather than `:stateMachine:<StateMachineName>`.
* **Fix:** Verify in `template.yaml` that `ExecutionLevelOperations` uses `arn:aws:states:${AWS::Region}:${AWS::AccountId}:execution:${SalesIngestionStateMachine.Name}:*`.

### 2. Glue Job Fails Immediately with `S3 Object Not Found` for the PySpark Script

* **Symptom:** `StartGlueJob` transitions to `NotifyFailure` because the Glue script `s3://<bucket>/scripts/sales_etl.py` does not exist.
* **Cause:** The SAM stack creates the S3 bucket, and `deploy.sh` uploads the inline Glue script and sample Parquet file immediately after `sam deploy`.
* **Fix:** Re-run `./deploy.sh` to ensure the post-deploy S3 asset upload step completes cleanly.

### 3. `ValidationError` When Registering the AgentCore Gateway Target

* **Symptom:** Gateway target creation fails complaining about missing or invalid `stateMachineArn` in `StartExecution`.
* **Cause:** The placeholders `${STATE_MACHINE_ARN}` and `${GATEWAY_ROLE_ARN}` in `gateway/target.json` were not substituted with live CloudFormation stack outputs.
* **Fix:** Ensure `deploy.sh` generated `gateway/target.rendered.json` and pass `file://gateway/target.rendered.json` to the CLI.

### 4. `ValidateInput` State Immediately Routes to `NotifyFailure`

* **Symptom:** The workflow fails in under 100ms without invoking AWS Glue.
* **Cause:** The `sourceKey` passed in `StartExecution` did not match the required pattern `raw/sales/*.parquet`.
* **Fix:** Ensure the input JSON contains `{"sourceKey": "raw/sales/2026-11-05/sales.parquet"}`.
