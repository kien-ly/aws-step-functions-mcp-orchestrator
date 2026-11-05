# From ETL Jobs to Agentic Orchestrators: How AWS Step Functions Became an LLM Tool

Data pipelines thrive on determinism. For the past decade, data engineers have built ingestion and transformation architectures around a simple contract: given a known schedule, a validated schema, and a predictable sequence of states, a workflow engine executes the exact same Directed Acyclic Graph (DAG) every single day. Whether orchestrated via AWS Step Functions, Apache Airflow, or Amazon EventBridge schedules, traditional ETL pipelines are reliable precisely because they do not improvise.

Operational reality, however, rarely sticks to the script. Upstream partners drop late Parquet files into Amazon S3 hours after the batch window closes. Regional API endpoints throttle requests during end-of-quarter surges. Finance teams message the data engineering on-call channel asking to re-run a specific daily partition after fixing a reference table. While the pipelines themselves are automated, the operational control plane around them remains stubbornly manual. A human engineer still has to read the Slack request, verify the S3 key, open the AWS Management Console or CLI, construct a JSON payload, trigger the workflow, monitor the run, and report back.

When teams try to solve this operational bottleneck with AI agents, they frequently swing to a dangerous extreme: granting a Large Language Model (LLM) direct tool access to low-level cloud primitives. Give an agent raw permissions to invoke `glue:StartJobRun`, `redshift-data:ExecuteStatement`, `lambda:InvokeFunction`, and `s3:CopyObject`, and you effectively ask a probabilistic token generator to act as a distributed transaction coordinator. If the agent hallucinates a parameter on step three of a five-step ingestion flow, hits a context-window timeout while waiting for a Spark cluster to spin up, or retries a non-idempotent load query, your data lake ends up in a corrupted, half-written state with no centralized execution state to audit.

Recently, AWS introduced a capability that resolves this tension cleanly: [AWS Step Functions workflows are now available as AI agent tools](https://aws.amazon.com/about-aws/whats-new/2026/10/step-functions-workflows-ai-agent-tools/) through Amazon Bedrock AgentCore Gateway over the Model Context Protocol (MCP). Featured alongside the arrival of OpenAI GPT-6.1 Terra in the [AWS Weekly Roundup](https://aws.amazon.com/blogs/aws/aws-weekly-roundup-gpt-6-1-terra-on-bedrock-step-functions-as-agent-tools-and-more-november-2-2026/), this integration lets you expose battle-tested state machines directly to AI agents as structured MCP tools without writing custom wrapper Lambda functions or glue code.

The core architectural principle behind this pattern is straightforward: **the AI agent decides when to act and with what input; AWS Step Functions decides how the workflow executes.**

---

## 1. Architecture Overview: Bridging Reasoning and Determinism

To understand why this integration changes how we design data platform operations, we first need to clarify the role of the **Model Context Protocol (MCP)**. MCP is an open, standardized protocol built on JSON-RPC that defines how AI applications and agents discover (`tools/list`) and invoke (`tools/call`) external tools and context sources. Instead of writing bespoke tool schemas and API adapters for every model provider or agent framework, platform teams expose an MCP server endpoint once, and any compliant agent can interact with it through a uniform contract.

Amazon Bedrock AgentCore Gateway acts as a managed MCP server boundary for enterprise AWS environments. According to the [Amazon Bedrock AgentCore Gateway documentation](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-target-connector-step-functions.html), the Gateway now includes a built-in AWS Step Functions connector identified by `connectorId: "states"`. When you register a Step Functions target on the Gateway, it automatically generates MCP-compliant JSON schemas for core state machine operations and executes those calls using a dedicated Gateway IAM service role.

Here is how the end-to-end control flow looks in practice:

```text
+-----------------------------------------------------------------------+
|                        User / Data Ops Engineer                       |
|  "Start the sales ingestion pipeline for                              |
|   raw/sales/2026-11-05/sales.parquet and tell me if it succeeds."     |
+-----------------------------------+-----------------------------------+
                                    |
                                    v
+-----------------------------------------------------------------------+
|                AI Agent (Amazon Bedrock / GPT-6.1 Terra)              |
|  1. Parses intent and extracts sourceKey                              |
|  2. Calls MCP tool: StartExecution(input={"sourceKey": "..."})        |
|  3. Polls MCP tool: DescribeExecution / GetExecutionHistory           |
+-----------------------------------+-----------------------------------+
                                    |
                          MCP (tools/list, tools/call)
                                    |
                                    v
+-----------------------------------------------------------------------+
|                   Amazon Bedrock AgentCore Gateway                    |
|  * Built-in Connector: connectorId = "states"                         |
|  * Auto-generates MCP tool schemas                                    |
|  * Enforces pinned stateMachineArn on state-machine-level calls       |
|  * Assumes least-privilege Gateway IAM Service Role                   |
+-----------------------------------+-----------------------------------+
                                    |
                           AWS SDK / IAM Auth
                                    |
                                    v
+-----------------------------------------------------------------------+
|               AWS Step Functions (Standard State Machine)             |
|                                                                       |
|  [ValidateInput] ==> [StartGlueJob (.sync)] ==> [RecordSuccess]       |
|         |                      |                                      |
|         +=======> (Catch) <====+                                      |
|                      |                                                |
|                      v                                                |
|               [NotifyFailure] ==> [WorkflowFailed]                    |
+------+------------------------+-------------------------+-------------+
       |                        |                         |
       v                        v                         v
+--------------+        +---------------+         +---------------+
|  Amazon S3   |        |   AWS Glue    |         |Amazon Redshift|
| (Raw & Clean |        | (PySpark ETL  |         | / DynamoDB /  |
|  Parquet)    |        |  Job Runner)  |         |  Amazon SNS   |
+--------------+        +---------------+         +---------------+

```

Each layer in this architecture has a strictly bounded responsibility:

* **User and Bedrock Agent:** The user provides operational intent in natural language. The agent, powered by a foundation model on Amazon Bedrock such as OpenAI GPT-6.1 Terra, translates that unstructured request into structured JSON parameters, decides which MCP tool to invoke, evaluates execution status, and summarizes the outcome for the operator.
* **Amazon Bedrock AgentCore Gateway (MCP Layer):** The Gateway serves as the protocol translation and security enforcement boundary. Using the built-in `states` connector, it exposes selected Step Functions API actions as MCP tools, automatically generates their input schemas, injects pinned configuration values such as `stateMachineArn`, and authenticates requests using its own tightly scoped IAM service role.
* **AWS Step Functions (Deterministic Execution Layer):** Step Functions owns the control flow, state transitions, payload validation, error handling, exponential backoff retries, and dead-letter notifications. Because Step Functions natively integrates with more than 220 AWS services, your existing orchestration logic requires zero modification to become agent-accessible.
* **Downstream Data Services (Amazon S3, AWS Glue, Amazon Redshift, Amazon SNS):** These services perform the heavy data storage, distributed Spark processing, warehouse loading, and failure alerting. They only ever receive requests from the Step Functions execution role, remaining completely isolated from the AI agent.

### Why This Beats Direct Infrastructure Access

Compare this architecture to giving an LLM agent direct tool definitions for `s3:HeadObject`, `glue:StartJobRun`, `redshift-data:ExecuteStatement`, and `sns:Publish`. When an agent orchestrates raw services directly, the orchestration graph lives exclusively inside the model's probabilistic context window. If the agent process crashes while a 20-minute Glue job is running, nothing triggers the downstream warehouse load or failure notification. Worse, an agent with direct Glue or Redshift permissions could be tricked via prompt injection into running arbitrary scripts or querying restricted tables.

By placing AWS Step Functions between the agent and your data services, you collapse the agent's attack surface to a single workflow entry point. The agent cannot alter the sequence of ETL steps, skip input validation, disable failure notifications, or invent new IAM permissions at runtime.

---

## 2. Demo Scenario: AI-Triggered Daily Sales Ingestion Pipeline

To demonstrate how this works in a realistic data engineering environment, we will build an **AI-Triggered Daily Sales Ingestion Pipeline**.

Imagine a scenario where an upstream e-commerce platform uploads a corrected daily sales extract to S3 outside the normal midnight batch schedule. A data engineer or automated Slack ops bot sends the following prompt to the operational agent:

> *"Start the sales ingestion pipeline for `raw/sales/2026-11-05/sales.parquet` and tell me if it succeeds."*

Rather than executing shell scripts or manually clicking through the AWS console, the agent handles the request through a four-stage lifecycle governed by the [AWS Step Functions AgentCore Gateway integration](https://docs.aws.amazon.com/step-functions/latest/dg/tutorial-agentcore-gateway.html):

1. **Tool Discovery and Parameter Extraction:** The agent inspects the MCP tools exposed by the AgentCore Gateway target. It identifies the `StartExecution` tool and extracts `raw/sales/2026-11-05/sales.parquet` as the `sourceKey` attribute inside the workflow input payload. Crucially, the agent does not need to know or supply the `stateMachineArn` because that parameter is pinned inside the Gateway target configuration.
2. **Deterministic Workflow Execution:** The Gateway invokes `states:StartExecution` on the pinned **Standard Step Functions workflow**. Step Functions initializes a new execution and runs through four states:
* `ValidateInput`: A `Choice` state that verifies `sourceKey` exists in the payload and matches the expected `raw/sales/*.parquet` prefix pattern before provisioning any compute.
* `StartGlueJob`: A synchronous `Task` state (`arn:aws:states:::glue:startJobRun.sync`) that triggers the AWS Glue PySpark job with `--source_key` and waits for completion, applying built-in exponential backoff retries for transient resource exceptions.
* `RecordSuccess`: A `Task` state that writes audit metadata (execution ARN, source key, completion timestamp, and status) to an Amazon DynamoDB audit table (or triggers a Redshift `COPY` metadata update).
* `NotifyFailure` and `WorkflowFailed`: If either input validation or the Glue job fails after exhausting retries, a `Catch` block routes execution to an Amazon SNS notification task (`NotifyFailure`) and terminates in an explicit `Fail` state (`WorkflowFailed`).


3. **Execution Monitoring and Inspection:** When `StartExecution` returns an `executionArn`, the agent calls `DescribeExecution` over MCP to poll the workflow status until it transitions from `RUNNING` to `SUCCEEDED` (or `FAILED`). If the workflow fails, or if the operator asks for step-by-step diagnostics, the agent invokes `GetExecutionHistory` to inspect the exact state transition event where the error occurred.
4. **Operational Summary:** Once the workflow reaches a terminal state, the agent synthesizes the execution metadata, elapsed duration, and audit record into a concise response for the engineer.

### Why Standard Workflows Are Mandatory Here

Step Functions offers two workflow types: Standard and Express. For agent-orchestrated data pipelines, **Standard Workflows** are the right architectural choice for two reasons:

* **Execution Duration:** Data ingestion jobs on AWS Glue or Amazon Redshift routinely run longer than the five-minute maximum duration of Express Workflows. Standard Workflows support executions lasting up to one year.
* **Auditability and History API Support:** The AgentCore Gateway `states` connector exposes `DescribeExecution` and `GetExecutionHistory` so the agent can track progress and diagnose failures. Standard Workflows persist full execution history queryable via those synchronous APIs, whereas Express Workflows push logs asynchronously to CloudWatch Logs and do not support `DescribeExecution` or `GetExecutionHistory` in the same manner.

---

## 3. Implementation Deep Dive

Let's examine the core engineering artifacts required to deploy this pattern using AWS SAM (Serverless Application Model). The complete, copy-pasteable repository is provided in the **Demo Repository Files** section at the end of this post, but understanding four specific design choices will save you hours of debugging.

### 1. The State Machine Definition (ASL)

In our Amazon States Language (`state-machine/data-pipeline.asl.json`) definition, the first defense against LLM hallucinations is an explicit `ValidateInput` state. Even though the Gateway generates an MCP tool schema, `StartExecution` accepts a serialized JSON string in its `input` field. A `Choice` state at the entry point guarantees that malformed inputs fail immediately at near-zero cost before spinning up an AWS Glue DPU cluster:

```json
"ValidateInput": {
  "Type": "Choice",
  "Comment": "Ensure sourceKey is provided and targets the raw/sales/ prefix with a .parquet extension",
  "Choices": [
    {
      "And": [
        {
          "Variable": "$.sourceKey",
          "IsPresent": true
        },
        {
          "Variable": "$.sourceKey",
          "IsString": true
        },
        {
          "Variable": "$.sourceKey",
          "StringMatches": "raw/sales/*.parquet"
        }
      ],
      "Next": "StartGlueJob"
    }
  ],
  "Default": "FormatValidationFailure"
}

```

Next, `StartGlueJob` uses the `.sync` service integration pattern (`arn:aws:states:::glue:startJobRun.sync`). This instructs Step Functions to start the Glue job, pause the state machine execution, poll Glue behind the scenes, and only advance to `RecordSuccess` once the Spark job finishes.

Notice also how failure handling is structured: when `StartGlueJob` catches an error, it transitions to `NotifyFailure` (which publishes an alert to Amazon SNS) and then immediately transitions to `"Next": "WorkflowFailed"`, which is a `"Type": "Fail"` state. A common mistake in Step Functions design is ending an error-notification state with `"End": true`. If you do that, Step Functions considers the error handled and marks the overall execution status as `SUCCEEDED`, causing the AI agent calling `DescribeExecution` to report a false positive to the user.

### 2. The AgentCore Gateway Target Configuration and ARN Pinning

According to the [Amazon Bedrock AgentCore Gateway Step Functions connector guide](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-target-connector-step-functions.html), configuring the target requires specifying `"connectorId": "states"` and declaring which Step Functions API actions to expose as MCP tools:

* **State-machine-level operations:** `StartExecution`, `ListExecutions`, and `DescribeStateMachine` operate on a `stateMachineArn`. In our `gateway/target.json`, we **pin** `stateMachineArn` to our specific sales ingestion workflow ARN. When a parameter is pinned at the Gateway target layer, the Gateway strips that parameter from the tool schema exposed to the agent and injects the hardcoded ARN automatically at invocation time. The agent literally has no mechanism to override which state machine it triggers or inspects.
* **Execution-level operations:** `DescribeExecution`, `GetExecutionHistory`, and `StopExecution` operate on a dynamic `executionArn` generated when a workflow starts. Because every run produces a unique execution ARN, this parameter cannot be pinned statically; instead, the agent passes the `executionArn` returned by `StartExecution` (or `ListExecutions`), and security is enforced via IAM resource scoping.

### 3. Two-Tier Least-Privilege IAM Design

A subtle nuance of AWS Step Functions IAM authorization is that state-machine-level operations and execution-level operations evaluate permissions against **two distinct IAM resource ARN formats**:

```text
State Machine ARN format:
arn:aws:states:<region>:<account-id>:stateMachine:<state-machine-name>

Execution ARN format:
arn:aws:states:<region>:<account-id>:execution:<state-machine-name>:<execution-name>

```

In our SAM `template.yaml`, the `AgentCoreGatewayServiceRole` separates these permissions into two tightly scoped statements:

```yaml
Policies:
  - PolicyName: StepFunctionsMCPConnectorLeastPrivilege
    PolicyDocument:
      Version: "2012-10-17"
      Statement:
        - Sid: StateMachineLevelOperations
          Effect: Allow
          Action:
            - states:StartExecution
            - states:ListExecutions
            - states:DescribeStateMachine
          Resource: !GetAtt SalesIngestionStateMachine.Arn
        - Sid: ExecutionLevelOperations
          Effect: Allow
          Action:
            - states:DescribeExecution
            - states:GetExecutionHistory
            - states:StopExecution
          Resource: !Sub "arn:aws:states:${AWS::Region}:${AWS::AccountId}:execution:${SalesIngestionStateMachine.Name}:*"

```

Because the execution ARN pattern includes the state machine name (`${SalesIngestionStateMachine.Name}:*`), the Gateway role can only inspect or stop executions belonging to the sales ingestion state machine. Even if an attacker tricks the agent into calling `StopExecution` with an execution ARN belonging to a financial payroll workflow in the same AWS account, AWS IAM denies the request at the API boundary.

### 4. Invoking the Gateway via MCP `tools/call` in Python

From the client side (`agent/invoke_agent.py`), interacting with the AgentCore Gateway follows standard MCP JSON-RPC semantics over AWS SigV4-authenticated HTTPS requests (or via the `bedrock-agentcore` runtime client). The script first sends a `tools/list` request to verify the auto-generated tool schemas, then invokes `StartExecution` via `tools/call`:

```python
mcp_start_request = {
    "jsonrpc": "2.0",
    "id": "req-start-1",
    "method": "tools/call",
    "params": {
        "name": "states_StartExecution",
        "arguments": {
            "name": f"sales-ingest-{uuid.uuid4().hex[:8]}",
            "input": json.dumps({"sourceKey": source_key})
        }
    }
}

```

Once the Gateway returns the `executionArn`, the Python client loops with backoff using `tools/call` on `states_DescribeExecution` until `status` is no longer `RUNNING`, followed by `states_GetExecutionHistory` to extract step-level timing and audit details.

---

## 4. Security, Guardrails, and Common Mistakes

When giving AI agents operational access to production data platforms, **IAM policies and deterministic workflow boundaries matter infinitely more than system prompt instructions.** Telling an LLM in its system prompt *"Never run any workflow other than the sales pipeline and never pass invalid S3 keys"* is an aspiration, not a security control. Prompt injection, context confusion, or model updates can bypass prompt rules at any time. Hardware-enforced IAM policies, Gateway parameter pinning, and ASL input validation cannot be sweet-talked by a clever prompt.

### Risk and Mitigation Matrix

| Operational Risk | How It Manifests in Agentic Data Ops | Deterministic Mitigation |
| --- | --- | --- |
| **Prompt Injection** | A malicious string embedded in an incident ticket or filename instructs the agent to trigger a destructive cleanup state machine or pass malicious arguments to Glue. | Pin `stateMachineArn` in the AgentCore Gateway target config so the agent cannot select another state machine. Enforce strict prefix pattern matching (`raw/sales/*.parquet`) in the `ValidateInput` ASL Choice state. |
| **Unauthorized Workflow Execution** | The agent attempts to discover and invoke sensitive financial, HR, or infrastructure state machines residing in the same AWS account. | Combine Gateway target ARN pinning with an IAM role statement that scopes `states:StartExecution`, `states:ListExecutions`, and `states:DescribeStateMachine` exclusively to a single state machine ARN. Never grant `states:*` or `Resource: "*"`. |
| **Agent Stopping Unrelated Executions** | An agent instructed to cancel a stuck ingestion job guesses or receives an `executionArn` belonging to a critical production workflow and invokes `StopExecution`. | Scope `states:StopExecution`, `states:DescribeExecution`, and `states:GetExecutionHistory` in IAM to `arn:aws:states:region:account:execution:PinnedStateMachineName:*`. IAM blocks cross-workflow execution interference. |
| **Excessive Retries** | When a downstream Glue job fails due to a schema error, an autonomous agent enters a runaway loop calling `StartExecution` repeatedly, burning DPU budget. | Keep retry logic inside the Step Functions `StartGlueJob` task with strict `MaxAttempts` and `BackoffRate`. Enforce deterministic idempotency keys (`name` parameter on `StartExecution`) and cap agent tool-call iterations per session. |
| **Missing Auditability** | A pipeline runs outside normal batch hours and modifies warehouse tables, but platform engineers cannot tell whether a human, a schedule, or an agent triggered it. | Use **Standard Workflows** so every transition is recorded in `GetExecutionHistory`. Enable AWS CloudTrail for Step Functions and AgentCore Gateway API calls, and tag/prefix agent-initiated execution names (e.g., `agent-run-*`). |
| **Long-Running Workflows** | An ETL job takes 45 minutes, exceeding the HTTP timeout or conversational turn window of the synchronous LLM agent session. | Decouple execution initiation from completion. Have the agent call `StartExecution` and return the `executionArn` immediately, then rely on asynchronous polling (`DescribeExecution` / `ListExecutions`) in subsequent turns or webhook callbacks. |

### Common Mistakes to Avoid

When teams implement the Step Functions AgentCore Gateway connector for the first time, four pitfalls come up repeatedly:

1. **Using identical IAM resource ARNs for state machine and execution actions:** Engineers frequently copy the state machine ARN (`...:stateMachine:MyWorkflow`) into the `Resource` block for `DescribeExecution`, `GetExecutionHistory`, and `StopExecution`. The deployment succeeds, `StartExecution` works, and then the agent immediately fails with an `AccessDeniedException` when checking status because execution-level APIs require the `:execution:MyWorkflow:*` ARN syntax.
2. **Leaving `stateMachineArn` unpinned during testing:** If you omit the pinned `stateMachineArn` configuration in `gateway/target.json`, the Gateway exposes `stateMachineArn` as a required input argument in the MCP tool schema. Not only does this force you to hardcode AWS account numbers and region strings into the agent's prompt context, it also opens the door for the model to hallucinate ARNs.
3. **Deploying Express Workflows for agentic monitoring:** If you configure the state machine with `Type: EXPRESS`, calling `DescribeExecution` or `GetExecutionHistory` through the MCP tool will fail because those APIs are designed for Standard Workflows (or synchronous Express edge cases that do not match long-running asynchronous ETL polling).
4. **Swallowing errors inside the notification step:** As noted in our ASL walkthrough, if your `Catch` block routes failures to an SNS `Publish` task that terminates with `"End": true` instead of transitioning to a `"Type": "Fail"` state, the Step Functions execution finishes with `status: "SUCCEEDED"`. Your agent will cheerfully tell the user that the pipeline succeeded right as the on-call pager goes off.

---

## 5. Where OpenAI GPT-6.1 Terra on Bedrock Fits

An MCP tool gateway is only half of an agentic architecture; the other half is the reasoning model that interprets user requests, plans tool invocations, and synthesizes execution logs. Alongside the Step Functions connector announcement, AWS announced in the [AWS Weekly Roundup](https://aws.amazon.com/blogs/aws/aws-weekly-roundup-gpt-6-1-terra-on-bedrock-step-functions-as-agent-tools-and-more-november-2-2026/) that **OpenAI GPT-6.1 Terra** is generally available on Amazon Bedrock.

AWS describes GPT-6.1 Terra as a fast, cost-efficient model built for everyday enterprise workloads. *(Note on performance claims: any throughput, latency, or price-performance figures published by AWS or OpenAI are vendor-reported metrics rather than independent benchmarks. Always validate token latency, structured tool-call accuracy, and cost per run against your own payload shapes before committing to production SLAs.)*

In practical data engineering and LLMOps workflows, pairing GPT-6.1 Terra on Amazon Bedrock with the Step Functions AgentCore Gateway connector unlocks four strong operational patterns:

* **Multi-Turn Agent Tool Loops:** When an agent triggers a pipeline (`StartExecution`), checks status (`DescribeExecution`), and inspects event logs (`GetExecutionHistory`), it executes three to five sequential MCP tool calls per user request. Using a fast, cost-efficient reasoning model keeps the cumulative round-trip overhead low, making interactive ops bots feel responsive rather than sluggish.
* **RAG-Assisted Incident Triage:** When a pipeline fails, `GetExecutionHistory` returns a detailed JSON array of state transitions and exception stack traces (for example, a `GlueJobRunException` caused by a Parquet column type mismatch). An agent powered by GPT-6.1 Terra can take that raw error event, query an internal Bedrock Knowledge Base containing team runbooks and dbt/Glue table contracts, and immediately reply with both the failure status and the exact remediation steps.
* **Self-Service Operational Assistants:** Data analysts and product managers often need to check whether today's regional sales partition has finished loading before sending out executive dashboards. By exposing only `ListExecutions`, `DescribeExecution`, and `GetExecutionHistory` (while omitting `StartExecution` and `StopExecution`) on a read-only Gateway target, GPT-6.1 Terra can answer natural-language status inquiries in Slack without giving non-engineers console access.
* **Intelligent Batch Backfill Coordination:** During a multi-day backfill, an operational agent can iterate over a list of verified S3 date partitions, trigger sequential or bounded-concurrency executions through the Gateway, verify each partition's completion status, and halt immediately if an upstream data quality check trips.

### When Not to Use This Pattern

Just because you *can* trigger a Step Functions workflow via an LLM agent does not mean every workflow belongs behind an MCP gateway. Avoid this pattern in three scenarios:

1. **Routine Time-Driven or Event-Driven Cron Jobs:** If your sales pipeline runs every night at 02:00 UTC or triggers automatically whenever an `s3:ObjectCreated:*` event lands in a bucket, use Amazon EventBridge directly with Step Functions. Putting an LLM in the loop for deterministic, unvarying triggers adds token cost, latency, and an unnecessary failure point for zero cognitive value.
2. **High-Throughput, Low-Latency Streaming Micro-Batches:** If you process thousands of events per second with sub-second latency targets, keep the path strictly within Amazon Kinesis, Apache Kafka, AWS Lambda, or Step Functions Express Workflows triggered by deterministic routers. Agentic tool-calling over MCP is designed for control-plane orchestration and human-speed operational decisions, not data-plane packet routing.
3. **Pure Read-Only Analytical Queries:** If a user asks *"What were total sales in the Northeast region yesterday?"*, triggering an orchestration state machine is overkill. Route read-only analytical questions to a dedicated Text-to-SQL tool backed by Amazon Athena, Amazon Redshift, or a semantic layer rather than a workflow engine.

---

## 6. Production Readiness Checklist

Before moving an AgentCore Gateway and Step Functions integration from a sandbox proof of concept into your production AWS environment, verify these ten architectural controls:

* [ ] **Use Standard Workflows for auditable operations:** Ensure any workflow exposed to an agent is configured as a `STANDARD` state machine so `DescribeExecution` and `GetExecutionHistory` return full, durable state transition histories.
* [ ] **Pin `stateMachineArn` in the Gateway target:** Always hardcode `stateMachineArn` in the AgentCore Gateway target configuration for `StartExecution`, `ListExecutions`, and `DescribeStateMachine` so the agent cannot target arbitrary workflows.
* [ ] **Enforce two-tier least-privilege IAM:** Scope state-machine-level actions to the exact state machine ARN (`...:stateMachine:Name`) and execution-level actions to the scoped wildcard execution ARN (`...:execution:Name:*`). Never grant wildcard `states:*` permissions.
* [ ] **Add explicit retries, timeouts, and catch states in ASL:** Configure `TimeoutSeconds`, `Retry` blocks with exponential backoff for transient service errors, and `Catch` blocks that alert operators and terminate in an explicit `Fail` state.
* [ ] **Validate inputs inside the state machine:** Place a `Choice` state (or lightweight validation task) at the very beginning of your state machine to verify required keys, data types, and S3 prefix patterns before invoking expensive compute jobs.
* [ ] **Log all MCP tool invocations:** Enable detailed invocation logging on Amazon Bedrock AgentCore Gateway so every `tools/call` request, caller identity, and parameter payload is recorded.
* [ ] **Enable AWS CloudTrail and Amazon CloudWatch Logs:** Turn on CloudTrail data/management events for Step Functions and configure execution logging to CloudWatch Logs for deep post-incident forensics.
* [ ] **Separate dev, staging, and production state machines:** Deploy isolated AgentCore Gateway targets and IAM roles per environment so a staging agent can never trigger or stop a production state machine.
* [ ] **Rate-limit agent-triggered executions:** Protect downstream Glue DPUs and Redshift clusters from runaway agent loops by enforcing concurrency checks, execution naming idempotency, or API throttling quotas at the Gateway layer.
* [ ] **Add human approval for destructive or high-cost workflows:** If a workflow performs table truncations, full historical reloads, or expensive cluster provisioning, insert a Step Functions callback task (`waitForTaskToken`) that pauses execution until a human engineer approves the action in Slack or via a signed approval link.

---

## 7. Blog Angles and LinkedIn Takeaways

If you are sharing this architectural pattern with your engineering organization or posting about it on LinkedIn, here are three hooks and key takeaways tailored for technical audiences:

### 3 LinkedIn-Ready Hooks

1. **The Architecture Angle:** *"Giving an LLM direct IAM permissions to AWS Glue, Redshift, and S3 is the fastest way to turn a minor prompt hallucination into a corrupted data lake. Here is how exposing AWS Step Functions as an MCP tool through Amazon Bedrock AgentCore Gateway keeps your pipelines deterministic while making operations agentic."*
2. **The Security Angle:** *"Prompt instructions are not a security boundary. When connecting AI agents to production data pipelines over MCP, two controls actually matter: pinning `stateMachineArn` at the AgentCore Gateway layer and scoping IAM across separate `stateMachine` and `execution` ARN resource types."*
3. **The Pragmatic LLMOps Angle:** *"You don't need to rewrite your battle-tested ETL pipelines in an agent framework to make them AI-ready. With the built-in `states` connector in Amazon Bedrock AgentCore Gateway, your existing AWS Step Functions workflows become structured MCP tools in minutes."*

### 3 Concise Takeaways

* **Decouple decision from execution:** Let the AI agent decide *when* to trigger a workflow and extract the runtime parameters, while AWS Step Functions deterministically controls *how* the steps, retries, and failure alerts execute.
* **Lock down the MCP boundary:** Use AgentCore Gateway's built-in `states` connector with pinned `stateMachineArn` parameters and two-tier IAM policies (`stateMachine` vs. `execution` ARNs) so the agent cannot access or stop unauthorized workflows.
* **Design workflows for observability:** Use Standard Step Functions workflows with explicit `ValidateInput` choice states and terminal `Fail` states so agents calling `DescribeExecution` and `GetExecutionHistory` always receive accurate, auditable status data.

In the age of AI agents, data engineers do not disappear. We design the safe boundaries within which agents are allowed to operate our data platforms.

---

## Demo Repository Files

Below is the complete, deployable AWS SAM repository named `stepfunctions-agentcore-demo/`.

### Repository Structure

```text
stepfunctions-agentcore-demo/
├── README.md
├── .env.example
├── requirements.txt
├── template.yaml
├── deploy.sh
├── cleanup.sh
├── state-machine/
│   └── data-pipeline.asl.json
├── gateway/
│   └── target.json
└── agent/
    └── invoke_agent.py

```

---

### 1. `stepfunctions-agentcore-demo/README.md`

```markdown
# stepfunctions-agentcore-demo

A production-minded reference implementation demonstrating how to expose an **AWS Step Functions Standard Workflow** as an **MCP (Model Context Protocol) tool** using **Amazon Bedrock AgentCore Gateway** (`connectorId: "states"`).

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

```

---

### 2. `stepfunctions-agentcore-demo/.env.example`

```ini
# Core AWS and Stack Configuration
AWS_REGION=us-east-1
STACK_NAME=sfn-agentcore-sales-demo
GATEWAY_NAME=sales-data-ops-mcp-gateway
GLUE_JOB_NAME=sales-daily-parquet-ingestion-job

# Optional: Existing AgentCore Gateway Identifier (populated or created by deploy.sh)
GATEWAY_ID=
GATEWAY_ENDPOINT_URL=

# Optional: Bedrock Model ID for natural-language synthesis in invoke_agent.py
BEDROCK_MODEL_ID=openai.gpt-6-1-terra-v1:0
ALERT_EMAIL=dataops-alerts@example.com

```

---

### 3. `stepfunctions-agentcore-demo/requirements.txt`

```text
boto3>=1.35.0
botocore>=1.35.0
python-dotenv>=1.0.1
requests>=2.32.0

```

---

### 4. `stepfunctions-agentcore-demo/state-machine/data-pipeline.asl.json`

```json
{
  "Comment": "Standard Step Functions workflow exposed as an MCP tool via Amazon Bedrock AgentCore Gateway (connectorId: states)",
  "StartAt": "ValidateInput",
  "TimeoutSeconds": 7200,
  "States": {
    "ValidateInput": {
      "Type": "Choice",
      "Comment": "Validate that sourceKey is present, is a string, and adheres to raw/sales/*.parquet",
      "Choices": [
        {
          "And": [
            {
              "Variable": "$.sourceKey",
              "IsPresent": true
            },
            {
              "Variable": "$.sourceKey",
              "IsString": true
            },
            {
              "Variable": "$.sourceKey",
              "StringMatches": "raw/sales/*.parquet"
            }
          ],
          "Next": "StartGlueJob"
        }
      ],
      "Default": "FormatValidationFailure"
    },
    "FormatValidationFailure": {
      "Type": "Pass",
      "Comment": "Construct structured error context when input validation fails",
      "Parameters": {
        "sourceKey.$": "$.sourceKey",
        "errorInfo": {
          "Error": "InvalidInputParameterException",
          "Cause": "Input payload must include a valid sourceKey matching raw/sales/*.parquet"
        }
      },
      "Next": "NotifyFailure"
    },
    "StartGlueJob": {
      "Type": "Task",
      "Comment": "Run the AWS Glue PySpark ingestion job synchronously (.sync) with retry and catch boundaries",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "TimeoutSeconds": 3600,
      "Parameters": {
        "JobName": "${GlueJobName}",
        "Arguments": {
          "--source_key.$": "$.sourceKey",
          "--data_bucket": "${DataBucketName}",
          "--execution_id.$": "$$.Execution.Id"
        }
      },
      "ResultPath": "$.glueResult",
      "Retry": [
        {
          "ErrorEquals": [
            "Glue.ConcurrentRunsExceededException",
            "Glue.ResourceNumberLimitExceededException",
            "Glue.InternalServiceException",
            "States.TaskFailed"
          ],
          "IntervalSeconds": 15,
          "MaxAttempts": 2,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.errorInfo",
          "Next": "NotifyFailure"
        }
      ],
      "Next": "RecordSuccess"
    },
    "RecordSuccess": {
      "Type": "Task",
      "Comment": "Record successful ingestion run in the DynamoDB pipeline audit table",
      "Resource": "arn:aws:states:::dynamodb:putItem",
      "Parameters": {
        "TableName": "${AuditTableName}",
        "Item": {
          "ExecutionArn": {
            "S.$": "$$.Execution.Id"
          },
          "SourceKey": {
            "S.$": "$.sourceKey"
          },
          "GlueJobRunId": {
            "S.$": "$.glueResult.Id"
          },
          "JobStatus": {
            "S": "SUCCEEDED"
          },
          "CompletedAt": {
            "S.$": "$$.State.EnteredTime"
          }
        }
      },
      "ResultPath": "$.auditResult",
      "OutputPath": "$",
      "End": true
    },
    "NotifyFailure": {
      "Type": "Task",
      "Comment": "Publish operational alert to Amazon SNS when validation or Glue ETL fails",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${AlertTopicArn}",
        "Subject": "ALERT: AI-Triggered Sales Ingestion Pipeline Failed",
        "Message": {
          "executionArn.$": "$$.Execution.Id",
          "stateMachineArn.$": "$$.StateMachine.Id",
          "failedAt.$": "$$.State.EnteredTime",
          "errorDetails.$": "$.errorInfo"
        }
      },
      "ResultPath": "$.snsResult",
      "Next": "WorkflowFailed"
    },
    "WorkflowFailed": {
      "Type": "Fail",
      "Comment": "Ensure the Step Functions execution terminates with FAILED status for accurate MCP DescribeExecution reporting",
      "Error": "SalesIngestionPipelineFailed",
      "Cause": "The sales ingestion pipeline failed during input validation or AWS Glue job execution. Inspect GetExecutionHistory for details."
    }
  }
}

```

---

### 5. `stepfunctions-agentcore-demo/gateway/target.json`

```json
{
  "name": "sales-ingestion-sfn-target",
  "description": "Exposes the Sales Ingestion Standard Step Functions workflow as MCP tools via the built-in states connector.",
  "connectorId": "states",
  "credentials": {
    "roleArn": "${GATEWAY_ROLE_ARN}"
  },
  "configuration": {
    "operations": [
      {
        "name": "StartExecution",
        "description": "Start a new execution of the pinned daily sales ingestion pipeline. Pass JSON with sourceKey inside input.",
        "pinnedParameters": {
          "stateMachineArn": "${STATE_MACHINE_ARN}"
        }
      },
      {
        "name": "ListExecutions",
        "description": "List recent executions of the pinned daily sales ingestion pipeline, optionally filtered by statusFilter.",
        "pinnedParameters": {
          "stateMachineArn": "${STATE_MACHINE_ARN}"
        }
      },
      {
        "name": "DescribeStateMachine",
        "description": "Retrieve metadata and ASL definition details for the pinned daily sales ingestion state machine.",
        "pinnedParameters": {
          "stateMachineArn": "${STATE_MACHINE_ARN}"
        }
      },
      {
        "name": "DescribeExecution",
        "description": "Check the current status, start/stop timestamps, and output of a specific sales pipeline executionArn."
      },
      {
        "name": "GetExecutionHistory",
        "description": "Retrieve the detailed step-by-step state transition history and error causes for a specific executionArn."
      },
      {
        "name": "StopExecution",
        "description": "Stop an actively running executionArn belonging to the sales ingestion state machine."
      }
    ]
  }
}

```

---

### 6. `stepfunctions-agentcore-demo/template.yaml`

```yaml
AWSTemplateFormatVersion: "2010-09-09"
Transform: AWS::Serverless-2016-10-31
Description: >
  stepfunctions-agentcore-demo: Standard AWS Step Functions ETL pipeline
  and least-privilege IAM roles for Amazon Bedrock AgentCore Gateway (MCP connectorId: states).

Parameters:
  GatewayName:
    Type: String
    Default: sales-data-ops-mcp-gateway
    Description: Logical name of the Amazon Bedrock AgentCore Gateway
  GlueJobName:
    Type: String
    Default: sales-daily-parquet-ingestion-job
    Description: Name of the AWS Glue PySpark job orchestrated by Step Functions
  AlertEmail:
    Type: String
    Default: dataops-alerts@example.com
    Description: Email address for SNS pipeline failure notifications

Resources:
  # ---------------------------------------------------------------------------
  # 1. Amazon S3 Data Lake Bucket (Raw Parquet, Curated Output, Glue Scripts)
  # ---------------------------------------------------------------------------
  SalesDataLakeBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-data-${AWS::AccountId}-${AWS::Region}"
      BucketEncryption:
        ServerSideEncryptionConfiguration:
          - ServerSideEncryptionByDefault:
              SSEAlgorithm: AES256
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true

  # ---------------------------------------------------------------------------
  # 2. Amazon DynamoDB Pipeline Audit Table
  # ---------------------------------------------------------------------------
  PipelineAuditTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Sub "${AWS::StackName}-audit"
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: ExecutionArn
          AttributeType: S
      KeySchema:
        - AttributeName: ExecutionArn
          KeyType: HASH
      SSESpecification:
        SSEEnabled: true

  # ---------------------------------------------------------------------------
  # 3. Amazon SNS Operational Alert Topic
  # ---------------------------------------------------------------------------
  PipelineAlertTopic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: !Sub "${AWS::StackName}-alerts"

  PipelineAlertSubscription:
    Type: AWS::SNS::Subscription
    Properties:
      TopicArn: !Ref PipelineAlertTopic
      Protocol: email
      Endpoint: !Ref AlertEmail

  # ---------------------------------------------------------------------------
  # 4. AWS Glue Job & Execution Role
  # ---------------------------------------------------------------------------
  GlueExecutionRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${AWS::StackName}-glue-role"
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: glue.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
      Policies:
        - PolicyName: GlueS3DataLakeAccess
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Effect: Allow
                Action:
                  - s3:GetObject
                  - s3:PutObject
                  - s3:ListBucket
                Resource:
                  - !GetAtt SalesDataLakeBucket.Arn
                  - !Sub "${SalesDataLakeBucket.Arn}/*"

  SalesIngestionGlueJob:
    Type: AWS::Glue::Job
    Properties:
      Name: !Ref GlueJobName
      Role: !GetAtt GlueExecutionRole.Arn
      GlueVersion: "4.0"
      WorkerType: G.1X
      NumberOfWorkers: 2
      Timeout: 60
      ExecutionProperty:
        MaxConcurrentRuns: 3
      Command:
        Name: glueetl
        PythonVersion: "3"
        ScriptLocation: !Sub "s3://${SalesDataLakeBucket}/scripts/sales_etl.py"
      DefaultArguments:
        "--job-language": "python"
        "--enable-metrics": "true"
        "--enable-continuous-cloudwatch-log": "true"
        "--TempDir": !Sub "s3://${SalesDataLakeBucket}/temp/"

  # ---------------------------------------------------------------------------
  # 5. AWS Step Functions Standard Workflow & Execution Role
  # ---------------------------------------------------------------------------
  StepFunctionsExecutionRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${AWS::StackName}-sfn-exec-role"
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: states.amazonaws.com
            Action: sts:AssumeRole
      Policies:
        - PolicyName: StepFunctionsOrchestrationPolicy
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Sid: AllowGlueSyncJobControl
                Effect: Allow
                Action:
                  - glue:StartJobRun
                  - glue:GetJobRun
                  - glue:GetJobRuns
                  - glue:BatchStopJobRun
                Resource: !Sub "arn:aws:glue:${AWS::Region}:${AWS::AccountId}:job/${GlueJobName}"
              - Sid: AllowDynamoDBAuditWrite
                Effect: Allow
                Action:
                  - dynamodb:PutItem
                Resource: !GetAtt PipelineAuditTable.Arn
              - Sid: AllowSNSFailurePublish
                Effect: Allow
                Action:
                  - sns:Publish
                Resource: !Ref PipelineAlertTopic
              - Sid: AllowCloudWatchLogging
                Effect: Allow
                Action:
                  - logs:CreateLogDelivery
                  - logs:GetLogDelivery
                  - logs:UpdateLogDelivery
                  - logs:DeleteLogDelivery
                  - logs:ListLogDeliveries
                  - logs:PutResourcePolicy
                  - logs:DescribeResourcePolicies
                  - logs:DescribeLogGroups
                Resource: "*"

  SalesIngestionLogGroup:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: !Sub "/aws/vendedlogs/states/${AWS::StackName}-sales-ingestion"
      RetentionInDays: 30

  SalesIngestionStateMachine:
    Type: AWS::Serverless::StateMachine
    Properties:
      Name: !Sub "${AWS::StackName}-sales-ingestion"
      Type: STANDARD
      Role: !GetAtt StepFunctionsExecutionRole.Arn
      DefinitionUri: state-machine/data-pipeline.asl.json
      DefinitionSubstitutions:
        GlueJobName: !Ref SalesIngestionGlueJob
        DataBucketName: !Ref SalesDataLakeBucket
        AuditTableName: !Ref PipelineAuditTable
        AlertTopicArn: !Ref PipelineAlertTopic
      Logging:
        Level: ALL
        IncludeExecutionData: true
        Destinations:
          - CloudWatchLogsLogGroup:
              LogGroupArn: !GetAtt SalesIngestionLogGroup.Arn

  # ---------------------------------------------------------------------------
  # 6. Amazon Bedrock AgentCore Gateway IAM Role (Two-Tier Resource Scoping)
  # ---------------------------------------------------------------------------
  AgentCoreGatewayServiceRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${AWS::StackName}-agentcore-gw-role"
      Description: Least-privilege service role assumed by Amazon Bedrock AgentCore Gateway to invoke pinned Step Functions tools
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Sid: AllowBedrockAgentCoreGatewayAssumeRole
            Effect: Allow
            Principal:
              Service: bedrock-agentcore.amazonaws.com
            Action: sts:AssumeRole
            Condition:
              StringEquals:
                aws:SourceAccount: !Ref AWS::AccountId
      Policies:
        - PolicyName: StepFunctionsMCPConnectorLeastPrivilege
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Sid: StateMachineScopedOperations
                Effect: Allow
                Action:
                  - states:StartExecution
                  - states:ListExecutions
                  - states:DescribeStateMachine
                Resource: !GetAtt SalesIngestionStateMachine.Arn
              - Sid: ExecutionScopedOperations
                Effect: Allow
                Action:
                  - states:DescribeExecution
                  - states:GetExecutionHistory
                  - states:StopExecution
                Resource: !Sub "arn:aws:states:${AWS::Region}:${AWS::AccountId}:execution:${SalesIngestionStateMachine.Name}:*"

Outputs:
  DataBucketName:
    Description: S3 Bucket for raw/sales Parquet files and Glue ETL scripts
    Value: !Ref SalesDataLakeBucket
  GlueJobNameOutput:
    Description: AWS Glue Job orchestrated by Step Functions
    Value: !Ref SalesIngestionGlueJob
  AuditTableName:
    Description: DynamoDB table storing successful pipeline execution records
    Value: !Ref PipelineAuditTable
  AlertTopicArn:
    Description: SNS topic for failed pipeline alerts
    Value: !Ref PipelineAlertTopic
  StateMachineArn:
    Description: Pinned Standard Step Functions State Machine ARN
    Value: !GetAtt SalesIngestionStateMachine.Arn
  StateMachineName:
    Description: Standard Step Functions State Machine Name
    Value: !GetAtt SalesIngestionStateMachine.Name
  AgentCoreGatewayRoleArn:
    Description: Least-privilege IAM Role ARN for the AgentCore Gateway states target
    Value: !GetAtt AgentCoreGatewayServiceRole.Arn

```

---

### 7. `stepfunctions-agentcore-demo/deploy.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# deploy.sh
# Deploys the AWS SAM stack, seeds the Glue PySpark script & sample Parquet
# file in S3, renders gateway/target.rendered.json, and registers the
# Step Functions connector target with Amazon Bedrock AgentCore Gateway.
# -----------------------------------------------------------------------------

if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-sfn-agentcore-sales-demo}"
GATEWAY_NAME="${GATEWAY_NAME:-sales-data-ops-mcp-gateway}"
GLUE_JOB_NAME="${GLUE_JOB_NAME:-sales-daily-parquet-ingestion-job}"
ALERT_EMAIL="${ALERT_EMAIL:-dataops-alerts@example.com}"

echo "=================================================================="
echo " Deploying stack: ${STACK_NAME} in region: ${AWS_REGION}"
echo "=================================================================="

# 1. Validate required CLI tools
for cmd in aws sam jq python3; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: Required command '$cmd' is not installed." >&2
    exit 1
  fi
done

# 2. Build and deploy the SAM template
sam build --template-file template.yaml

sam deploy \
  --template-file .aws-sam/build/template.yaml \
  --stack-name "${STACK_NAME}" \
  --region "${AWS_REGION}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --resolve-s3 \
  --no-fail-on-empty-changeset \
  --parameter-overrides \
    GatewayName="${GATEWAY_NAME}" \
    GlueJobName="${GLUE_JOB_NAME}" \
    AlertEmail="${ALERT_EMAIL}"

# 3. Fetch CloudFormation outputs
STACK_OUTPUTS=$(aws cloudformation describe-stacks \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs" \
  --output json)

DATA_BUCKET=$(echo "${STACK_OUTPUTS}" | jq -r '.[] | select(.OutputKey=="DataBucketName") | .OutputValue')
STATE_MACHINE_ARN=$(echo "${STACK_OUTPUTS}" | jq -r '.[] | select(.OutputKey=="StateMachineArn") | .OutputValue')
GATEWAY_ROLE_ARN=$(echo "${STACK_OUTPUTS}" | jq -r '.[] | select(.OutputKey=="AgentCoreGatewayRoleArn") | .OutputValue')

echo "------------------------------------------------------------------"
echo " Data Bucket       : ${DATA_BUCKET}"
echo " State Machine ARN : ${STATE_MACHINE_ARN}"
echo " Gateway Role ARN  : ${GATEWAY_ROLE_ARN}"
echo "------------------------------------------------------------------"

# 4. Generate and upload the AWS Glue PySpark script + sample sales Parquet file
mkdir -p .build_artifacts

cat << 'EOF' > .build_artifacts/sales_etl.py
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_key", "data_bucket", "execution_id"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_uri = f"s3://{args['data_bucket']}/{args['source_key']}"
target_uri = f"s3://{args['data_bucket']}/curated/sales/execution_id={args['execution_id']}/"

print(f"Reading sales extract from: {source_uri}")
df = spark.read.parquet(source_uri)
row_count = df.count()
print(f"Validated {row_count} records. Writing curated output to: {target_uri}")

df.write.mode("overwrite").parquet(target_uri)
job.commit()
EOF

# Create a minimal valid dummy Parquet file for demo testing if pyarrow isn't installed
python3 - << 'EOF'
import struct

# Minimal valid Parquet file header/footer magic bytes ("PAR1") for S3 object existence checks,
# or generate a real tiny Parquet file if pyarrow is available.
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.table({
        "order_id": ["ORD-1001", "ORD-1002", "ORD-1003"],
        "sale_date": ["2026-11-05", "2026-11-05", "2026-11-05"],
        "amount_usd": [149.99, 89.50, 320.00]
    })
    pq.write_table(table, ".build_artifacts/sales.parquet")
except ImportError:
    with open(".build_artifacts/sales.parquet", "wb") as f:
        f.write(b"PAR1" + b"\x00" * 16 + b"PAR1")
EOF

echo "Uploading Glue ETL script and sample dataset to s3://${DATA_BUCKET} ..."
aws s3 cp .build_artifacts/sales_etl.py "s3://${DATA_BUCKET}/scripts/sales_etl.py" --region "${AWS_REGION}"
aws s3 cp .build_artifacts/sales.parquet "s3://${DATA_BUCKET}/raw/sales/2026-11-05/sales.parquet" --region "${AWS_REGION}"

# 5. Render gateway/target.rendered.json with live ARNs
sed \
  -e "s|\${STATE_MACHINE_ARN}|${STATE_MACHINE_ARN}|g" \
  -e "s|\${GATEWAY_ROLE_ARN}|${GATEWAY_ROLE_ARN}|g" \
  gateway/target.json > gateway/target.rendered.json

echo "Rendered Gateway target specification at: gateway/target.rendered.json"

# 6. Register or update the AgentCore Gateway target if GATEWAY_ID is configured
if [ -n "${GATEWAY_ID:-}" ]; then
  echo "Registering Step Functions target on AgentCore Gateway: ${GATEWAY_ID} ..."
  aws bedrock-agentcore create-gateway-target \
    --region "${AWS_REGION}" \
    --gateway-identifier "${GATEWAY_ID}" \
    --cli-input-json file://gateway/target.rendered.json || \
  echo "NOTE: Target may already exist or require manual update via bedrock-agentcore update-gateway-target."
else
  echo "------------------------------------------------------------------"
  echo "INFO: GATEWAY_ID not set in .env."
  echo "To attach the target to an existing Amazon Bedrock AgentCore Gateway, run:"
  echo "  aws bedrock-agentcore create-gateway-target \\"
  echo "    --region ${AWS_REGION} \\"
  echo "    --gateway-identifier <YOUR_GATEWAY_ID> \\"
  echo "    --cli-input-json file://gateway/target.rendered.json"
  echo "------------------------------------------------------------------"
fi

echo "Deployment complete."

```

---

### 8. `stepfunctions-agentcore-demo/cleanup.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# cleanup.sh
# Removes rendered build artifacts, empties the demo S3 bucket, deletes the
# AgentCore Gateway target (if configured), and deletes the SAM stack.
# -----------------------------------------------------------------------------

if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-sfn-agentcore-sales-demo}"

echo "=================================================================="
echo " Cleaning up stack: ${STACK_NAME} in region: ${AWS_REGION}"
echo "=================================================================="

# 1. Remove AgentCore Gateway target if GATEWAY_ID and TARGET_ID are set
if [ -n "${GATEWAY_ID:-}" ] && [ -n "${TARGET_ID:-}" ]; then
  echo "Deleting AgentCore Gateway target ${TARGET_ID} from gateway ${GATEWAY_ID}..."
  aws bedrock-agentcore delete-gateway-target \
    --region "${AWS_REGION}" \
    --gateway-identifier "${GATEWAY_ID}" \
    --target-identifier "${TARGET_ID}" || true
fi

# 2. Empty the S3 bucket before stack deletion
DATA_BUCKET=$(aws cloudformation describe-stacks \
  --region "${AWS_REGION}" \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" \
  --output text 2>/dev/null || true)

if [ -n "${DATA_BUCKET}" ] && [ "${DATA_BUCKET}" != "None" ]; then
  echo "Emptying S3 bucket: s3://${DATA_BUCKET} ..."
  aws s3 rm "s3://${DATA_BUCKET}" --recursive --region "${AWS_REGION}" || true
fi

# 3. Delete the CloudFormation stack
echo "Deleting CloudFormation stack: ${STACK_NAME} ..."
sam delete \
  --stack-name "${STACK_NAME}" \
  --region "${AWS_REGION}" \
  --no-prompts

# 4. Clean local temporary files
rm -rf .build_artifacts gateway/target.rendered.json

echo "Cleanup complete."

```

---

### 9. `stepfunctions-agentcore-demo/agent/invoke_agent.py`

```python
#!/usr/bin/env python3
"""
invoke_agent.py

Demonstrates invoking an AWS Step Functions Standard Workflow exposed as an
MCP tool through Amazon Bedrock AgentCore Gateway (connectorId: "states").

Lifecycle:
1. Parses a natural-language operator prompt to extract the S3 sourceKey.
2. Calls MCP `tools/list` on the AgentCore Gateway to inspect exposed tools.
3. Calls MCP `tools/call` -> `states_StartExecution` with `{"sourceKey": ...}`
   (Notice `stateMachineArn` is omitted by the agent because it is pinned
   at the Gateway target layer).
4. Polls MCP `tools/call` -> `states_DescribeExecution` until completion.
5. Calls MCP `tools/call` -> `states_GetExecutionHistory` to summarize step transitions.
"""

import argparse
import json
import os
import re
import time
import uuid
from typing import Any, Dict, Optional

import boto3
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from dotenv import load_dotenv


class AgentCoreMCPClient:
    """
    Client for invoking Amazon Bedrock AgentCore Gateway over MCP JSON-RPC
    (`tools/list` and `tools/call`) signed with AWS SigV4.
    Includes a transparent local Gateway emulator backed by boto3 Step Functions
    using the exact pinned target configuration (`gateway/target.rendered.json`)
    when GATEWAY_ENDPOINT_URL is not yet configured.
    """

    def __init__(self, region: str, stack_name: str, gateway_endpoint_url: Optional[str] = None):
        self.region = region
        self.stack_name = stack_name
        self.gateway_endpoint_url = gateway_endpoint_url
        self.session = boto3.Session(region_name=region)
        self.sfn_client = self.session.client("stepfunctions")
        self.pinned_state_machine_arn = self._load_pinned_state_machine_arn()

    def _load_pinned_state_machine_arn(self) -> str:
        """Load the pinned stateMachineArn from gateway/target.rendered.json or CloudFormation."""
        rendered_path = os.path.join(os.path.dirname(__file__), "..", "gateway", "target.rendered.json")
        if os.path.exists(rendered_path):
            with open(rendered_path, "r", encoding="utf-8") as f:
                target_cfg = json.load(f)
            ops = target_cfg.get("configuration", {}).get("operations", [])
            for op in ops:
                if op.get("name") == "StartExecution":
                    pinned = op.get("pinnedParameters", {}).get("stateMachineArn")
                    if pinned and not pinned.startswith("${"):
                        return pinned

        cfn = self.session.client("cloudformation")
        resp = cfn.describe_stacks(StackName=self.stack_name)
        for output in resp["Stacks"][0].get("Outputs", []):
            if output["OutputKey"] == "StateMachineArn":
                return output["OutputValue"]
        raise RuntimeError(f"Could not resolve StateMachineArn from stack {self.stack_name}")

    def _send_signed_mcp_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Send a SigV4-signed JSON-RPC request to the remote AgentCore Gateway MCP endpoint."""
        body = json.dumps(payload)
        credentials = self.session.get_credentials().get_frozen_credentials()
        aws_request = AWSRequest(
            method="POST",
            url=self.gateway_endpoint_url,
            data=body,
            headers={"Content-Type": "application/json"},
        )
        SigV4Auth(credentials, "bedrock-agentcore", self.region).add_auth(aws_request)
        response = requests.post(
            self.gateway_endpoint_url,
            headers=dict(aws_request.headers),
            data=body,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def rpc(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute an MCP JSON-RPC request (`tools/list` or `tools/call`)."""
        request_payload = {
            "jsonrpc": "2.0",
            "id": f"mcp-{uuid.uuid4().hex[:8]}",
            "method": method,
            "params": params or {},
        }

        if self.gateway_endpoint_url:
            return self._send_signed_mcp_request(request_payload)

        # Local Gateway Target enforcement fallback (enforces pinned stateMachineArn)
        return self._emulate_gateway_connector(request_payload)

    def _emulate_gateway_connector(self, request_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mirrors the exact behavior of the AgentCore Gateway `states` connector
        by injecting the pinned `stateMachineArn` for state-machine-level calls.
        """
        method = request_payload["method"]
        req_id = request_payload["id"]

        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "tools": [
                        {
                            "name": "states_StartExecution",
                            "description": "Start execution on the pinned state machine (stateMachineArn is pinned).",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "input": {"type": "string"},
                                },
                                "required": ["input"],
                            },
                        },
                        {
                            "name": "states_DescribeExecution",
                            "description": "Describe an execution by executionArn.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {"executionArn": {"type": "string"}},
                                "required": ["executionArn"],
                            },
                        },
                        {
                            "name": "states_GetExecutionHistory",
                            "description": "Fetch step-by-step history events for an executionArn.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {"executionArn": {"type": "string"}},
                                "required": ["executionArn"],
                            },
                        },
                        {
                            "name": "states_ListExecutions",
                            "description": "List executions for the pinned state machine.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {"statusFilter": {"type": "string"}},
                            },
                        },
                        {
                            "name": "states_DescribeStateMachine",
                            "description": "Describe the pinned state machine.",
                            "inputSchema": {"type": "object", "properties": {}},
                        },
                        {
                            "name": "states_StopExecution",
                            "description": "Stop a running execution by executionArn.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "executionArn": {"type": "string"},
                                    "cause": {"type": "string"},
                                },
                                "required": ["executionArn"],
                            },
                        },
                    ]
                },
            }

        if method == "tools/call":
            params = request_payload["params"]
            tool_name = params["name"]
            args = dict(params.get("arguments", {}))

            # Security enforcement: strip any agent-supplied stateMachineArn and inject pinned ARN
            args.pop("stateMachineArn", None)

            if tool_name == "states_StartExecution":
                resp = self.sfn_client.start_execution(
                    stateMachineArn=self.pinned_state_machine_arn,
                    name=args.get("name", f"agent-run-{uuid.uuid4().hex[:8]}"),
                    input=args["input"],
                )
            elif tool_name == "states_ListExecutions":
                call_kwargs = {"stateMachineArn": self.pinned_state_machine_arn, "maxResults": 10}
                if "statusFilter" in args:
                    call_kwargs["statusFilter"] = args["statusFilter"]
                resp = self.sfn_client.list_executions(**call_kwargs)
            elif tool_name == "states_DescribeStateMachine":
                resp = self.sfn_client.describe_state_machine(
                    stateMachineArn=self.pinned_state_machine_arn
                )
            elif tool_name == "states_DescribeExecution":
                resp = self.sfn_client.describe_execution(executionArn=args["executionArn"])
            elif tool_name == "states_GetExecutionHistory":
                resp = self.sfn_client.get_execution_history(
                    executionArn=args["executionArn"],
                    maxResults=50,
                    includeExecutionData=True,
                )
            elif tool_name == "states_StopExecution":
                resp = self.sfn_client.stop_execution(
                    executionArn=args["executionArn"],
                    cause=args.get("cause", "Stopped by AI operator request"),
                )
            else:
                raise ValueError(f"Unsupported MCP tool: {tool_name}")

            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(resp, default=str),
                        }
                    ]
                },
            }

        raise ValueError(f"Unsupported JSON-RPC method: {method}")


def extract_source_key_from_prompt(prompt: str) -> str:
    """Extract the target S3 key (e.g. raw/sales/2026-11-05/sales.parquet) from natural language."""
    match = re.search(r"([a-zA-Z0-9_\-/]+\.parquet)", prompt)
    if not match:
        raise ValueError(f"Could not extract a .parquet S3 key from prompt: {prompt}")
    return match.group(1)


def parse_mcp_tool_json(mcp_response: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap the serialized JSON payload from an MCP `tools/call` response."""
    text_block = mcp_response["result"]["content"][0]["text"]
    return json.loads(text_block)


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Invoke Step Functions via AgentCore Gateway MCP")
    parser.add_argument(
        "--prompt",
        type=str,
        default="Start the sales ingestion pipeline for raw/sales/2026-11-05/sales.parquet and tell me if it succeeds.",
        help="Natural language instruction for the data ops agent",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Seconds between DescribeExecution status checks",
    )
    args = parser.parse_args()

    region = os.getenv("AWS_REGION", "us-east-1")
    stack_name = os.getenv("STACK_NAME", "sfn-agentcore-sales-demo")
    gateway_url = os.getenv("GATEWAY_ENDPOINT_URL") or None

    client = AgentCoreMCPClient(
        region=region,
        stack_name=stack_name,
        gateway_endpoint_url=gateway_url,
    )

    print("=" * 72)
    print(f"Operator Prompt: {args.prompt}")
    print("=" * 72)

    # 1. Discover available MCP tools on the Gateway
    tools_response = client.rpc("tools/list")
    tool_names = [t["name"] for t in tools_response["result"]["tools"]]
    print(f"[1/4] Discovered MCP Tools on Gateway: {', '.join(tool_names)}")

    # 2. Extract sourceKey and invoke states_StartExecution via MCP tools/call
    source_key = extract_source_key_from_prompt(args.prompt)
    execution_name = f"agent-sales-{uuid.uuid4().hex[:8]}"
    workflow_input = json.dumps({"sourceKey": source_key})

    print(f"[2/4] Calling MCP tool 'states_StartExecution' with sourceKey='{source_key}'...")
    start_mcp_resp = client.rpc(
        "tools/call",
        {
            "name": "states_StartExecution",
            "arguments": {
                "name": execution_name,
                "input": workflow_input,
            },
        },
    )
    start_data = parse_mcp_tool_json(start_mcp_resp)
    execution_arn = start_data["executionArn"]
    print(f"      Started Execution ARN: {execution_arn}")

    # 3. Poll states_DescribeExecution until terminal state
    print(f"[3/4] Polling MCP tool 'states_DescribeExecution'...")
    status = "RUNNING"
    describe_data: Dict[str, Any] = {}
    while status == "RUNNING":
        time.sleep(args.poll_interval)
        desc_mcp_resp = client.rpc(
            "tools/call",
            {
                "name": "states_DescribeExecution",
                "arguments": {"executionArn": execution_arn},
            },
        )
        describe_data = parse_mcp_tool_json(desc_mcp_resp)
        status = describe_data["status"]
        print(f"      Current Workflow Status: {status}")

    # 4. Inspect states_GetExecutionHistory and print operational summary
    print(f"[4/4] Fetching execution audit trail via MCP tool 'states_GetExecutionHistory'...")
    history_mcp_resp = client.rpc(
        "tools/call",
        {
            "name": "states_GetExecutionHistory",
            "arguments": {"executionArn": execution_arn},
        },
    )
    history_data = parse_mcp_tool_json(history_mcp_resp)
    events = history_data.get("events", [])

    entered_states = [
        e["stateEnteredEventDetails"]["name"]
        for e in events
        if "stateEnteredEventDetails" in e
    ]

    print("\n" + "=" * 72)
    print("AGENT OPERATIONAL SUMMARY")
    print("=" * 72)
    print(f"Target Partition : {source_key}")
    print(f"Execution ARN    : {execution_arn}")
    print(f"Final Status     : {status}")
    print(f"Visited States   : {' -> '.join(entered_states)}")

    if status == "SUCCEEDED":
        output_payload = json.loads(describe_data.get("output", "{}"))
        glue_run_id = output_payload.get("glueResult", {}).get("Id", "N/A")
        print(f"Glue Job Run ID  : {glue_run_id}")
        print("Result           : Sales ingestion completed and recorded in DynamoDB audit table.")
    else:
        error_code = describe_data.get("error", "UnknownError")
        error_cause = describe_data.get("cause", "Inspect GetExecutionHistory events.")
        print(f"Failure Error    : {error_code}")
        print(f"Failure Cause    : {error_cause}")
    print("=" * 72)


if __name__ == "__main__":
    main()

```