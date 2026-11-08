---
title: "From ETL Jobs to Agentic Orchestrators: Exposing AWS Step Functions as MCP Tools"
date: 2026-11-05T20:30:00+07:00
draft: false
tags: ["aws", "step-functions", "bedrock", "mcp", "ai-agents", "data-engineering"]
categories: ["Data Engineering", "Artificial Intelligence"]
description: "How to safely bridge probabilistic AI reasoning with deterministic data pipelines by exposing AWS Step Functions workflows as Model Context Protocol (MCP) tools via Amazon Bedrock AgentCore Gateway."
---

I have spent most of my career on the boring, important part of data work: the pipelines that run at 2 a.m. and that nobody thinks about until they break. And I have always liked pipelines for one simple reason. They do not improvise. Give a workflow engine a schedule, a schema, and a known sequence of steps, and it will run the same plan every single day, without getting tired or distracted or "creative."

That predictability is the whole point. It is also the reason I was skeptical when people started talking about putting AI agents in charge of data operations.

This post is about where I landed. Short version: an agent is a great front door to a pipeline, but it should never be the thing that decides how the pipeline runs. The pipeline should keep that job. Here is how to set it up that way with AWS Step Functions and Amazon Bedrock AgentCore Gateway.

---

## Why the boring part is still the hard part

Pipelines are predictable on paper. In practice, the operational work around them is anything but.

Late files arrive in Amazon S3 hours after the batch window closed. An API throttles during a busy week. Someone in finance messages the on-call channel: "Can you re-run yesterday's partition? We fixed a reference table." The pipeline itself is automated, but the control plane around it is still manual. Someone reads the message, checks the S3 key, opens the console or the CLI, builds a JSON payload, starts the run, watches it, and reports back.

It is repetitive, low-risk work that eats time. So it is natural to want an AI agent to handle it.

The trap is what that usually looks like. The first instinct is to hand the agent the keys to the cloud. Give it permission to call `glue:StartJobRun`, `redshift-data:ExecuteStatement`, `lambda:InvokeFunction`, and `s3:CopyObject`, and you have quietly asked a language model to act as a distributed transaction coordinator. I would not trust a person to do that job from a chat window at midnight, and I do not think a model should either. If the agent gets one parameter wrong on step three of a five-step load, or times out while a Spark cluster is starting, or retries a load that was not idempotent, you are left with a half-written table and no single place that says what happened.

## A better split: the agent asks, the workflow does

AWS recently made [Step Functions workflows available as tools for AI agents](https://aws.amazon.com/about-aws/whats-new/2026/10/step-functions-workflows-ai-agent-tools/) through Amazon Bedrock AgentCore Gateway, using the Model Context Protocol (MCP). It was announced alongside OpenAI GPT-6.1 Terra on Bedrock, which is covered in the [AWS Weekly Roundup](https://aws.amazon.com/blogs/aws/aws-weekly-roundup-gpt-6-1-terra-on-bedrock-step-functions-as-agent-tools-and-more-november-2-2026/). The useful part for us: you can expose a workflow you already trust as a set of MCP tools, without writing a wrapper Lambda or any glue code.

The design rule I keep coming back to is this:

> **The agent decides when to act and with what input. Step Functions decides how the workflow runs.**

Everything else in this post follows from that split.

---

## 1. The architecture, in one picture

First, a quick note on MCP. It is an open protocol, built on JSON-RPC, that lets an AI application discover tools (`tools/list`) and call them (`tools/call`). The appeal is that you expose a tool server once, and any compliant agent can use it, without custom adapters for every model or framework.

[Amazon Bedrock AgentCore Gateway](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-target-connector-step-functions.html) acts as that managed MCP boundary. It ships with a built-in Step Functions connector, `connectorId: "states"`. When you register a Step Functions target, the Gateway generates MCP-compliant schemas for the state machine operations you choose, and it calls Step Functions with its own IAM service role.

Here is the whole flow:

![Step Functions as an MCP tool through Amazon Bedrock AgentCore Gateway: the operator sends a request, the agent calls the Gateway, the Gateway runs the pinned state machine, which fans out to Glue, S3, DynamoDB and SNS](https://raw.githubusercontent.com/kien-ly/aws-step-functions-mcp-orchestrator/main/docs/image/step-functions-mcp-orchestration-via-amazon-bedrock-agentcore-gateway.svg)

Each layer has a narrow job:

* **The operator and the agent.** You ask in plain language. The agent, running on a Bedrock model such as GPT-6.1 Terra, turns that into structured parameters, picks the right tool, checks the status, and tells you what happened. It is good at the translation. It is not in charge of the run.
* **AgentCore Gateway.** This is the protocol and security boundary. It exposes the Step Functions operations you selected, generates their input schemas, injects the values you pinned (such as `stateMachineArn`), and authenticates with a tightly scoped role.
* **Step Functions.** This is where the real work is decided: state transitions, payload validation, error handling, retries with backoff, and failure notifications. The workflow is the source of truth for what happens next. Step Functions also integrates with a long list of AWS services, so the orchestration logic does not have to be rewritten to become agent-accessible.
* **The data services.** S3, Glue, DynamoDB and SNS do the heavy lifting: storage, Spark processing, audit records, and alerts. They only accept requests from the Step Functions execution role. The agent never talks to them directly.

### Why this beats giving the agent direct access

Compare that with handing an agent tool definitions for `s3:HeadObject`, `glue:StartJobRun`, `redshift-data:ExecuteStatement`, and `sns:Publish`. In that setup, the plan for the pipeline lives inside the model's context window. If the agent process dies while a 20-minute Glue job is running, nothing starts the downstream load or sends the failure alert. An agent with direct Glue or warehouse permissions could also be talked into running something it should not, through prompt injection hidden in a ticket or a filename.

With Step Functions in the middle, the agent's reach shrinks to one entry point: the workflow. It cannot reorder the steps, skip validation, turn off alerts, or quietly widen its own permissions.

---

## 2. The scenario: a corrected sales extract at an odd hour

Let's make this concrete. An upstream e-commerce platform uploads a corrected daily sales file to S3, well outside the midnight batch. An engineer (or a Slack ops bot) asks the agent:

> *"Start the sales ingestion pipeline for `raw/sales/2026-11-05/sales.parquet` and tell me if it succeeds."*

No shell scripts, no console clicking. The agent works through four steps, using the [AWS Step Functions and AgentCore Gateway integration](https://docs.aws.amazon.com/step-functions/latest/dg/tutorial-agentcore-gateway.html):

1. **Discover the tools and pull out the input.** The agent lists the MCP tools on the Gateway target, finds `StartExecution`, and puts `raw/sales/2026-11-05/sales.parquet` into the `sourceKey` field. It never needs the state machine ARN, because that is pinned in the Gateway configuration.
2. **Run the workflow.** The Gateway calls `states:StartExecution` on the pinned Standard workflow. The run goes through these states:
   * `ValidateInput`: a `Choice` state that checks that `sourceKey` is present and matches `raw/sales/*.parquet` before any compute is started.
   * `StartGlueJob`: a synchronous task (`arn:aws:states:::glue:startJobRun.sync`) that starts the PySpark job with the source key and waits for it to finish. Transient failures are retried with exponential backoff.
   * `RecordSuccess`: writes an audit record (execution ARN, source key, completion time, status) to a DynamoDB table.
   * `NotifyFailure` and `WorkflowFailed`: if validation or the Glue job fails after its retries, a `Catch` routes to an SNS notification and ends in an explicit `Fail` state.
3. **Watch it.** When `StartExecution` returns an `executionArn`, the agent calls `DescribeExecution` until the status leaves `RUNNING`. If something fails, or you want the details, it calls `GetExecutionHistory` and points to the exact state where things went wrong.
4. **Summarize.** Once the run is finished, the agent turns the status, duration, and audit record into a short, readable answer.

### Why Standard workflows, not Express

Step Functions has two workflow types. For agent-driven pipelines, I would use **Standard** every time, for two reasons:

* **Runs can be long.** Glue and Redshift jobs often take longer than the five-minute limit on Express workflows. Standard workflows can run for up to a year.
* **You need the history.** Standard workflows keep a full execution history that `DescribeExecution` and `GetExecutionHistory` can query. Express workflows send logs to CloudWatch asynchronously and do not support these calls in the same way, which is a problem when the agent needs to answer "what happened?"

---

## 3. The implementation, piece by piece

The repository is deployed with AWS SAM (Serverless Application Model). The full, copy-paste version lives in the [demo repository](https://github.com/kien-ly/aws-step-functions-mcp-orchestrator). Four design choices took me the most time to get right, and they are the ones I would look at first.

### 1. The state machine: validate before you spend money

The first defense against a bad agent call is an explicit `ValidateInput` state in the Amazon States Language definition ([`state-machine/data-pipeline.asl.json`](https://github.com/kien-ly/aws-step-functions-mcp-orchestrator/blob/main/state-machine/data-pipeline.asl.json)). `StartExecution` takes its `input` as a serialized JSON string, and the Gateway's schema cannot check the contents of that string. So the workflow has to. The `Choice` state below rejects bad input right away, which costs next to nothing compared with starting a Glue job:

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

`StartGlueJob` uses the `.sync` integration pattern (`arn:aws:states:::glue:startJobRun.sync`). Step Functions starts the job, waits, and only moves to `RecordSuccess` once Spark has finished. You do not write any polling code.

One detail that has bitten people, including me in early drafts: if you route the error path to `NotifyFailure` and that state ends with `"End": true`, Step Functions considers the error handled and marks the whole execution `SUCCEEDED`. The agent then reports a success that never happened. The fix is to send the error path to a `"Type": "Fail"` state.

### 2. The Gateway target: pin what the agent should never choose

The Gateway target is defined in [`gateway/target.json`](https://github.com/kien-ly/aws-step-functions-mcp-orchestrator/blob/main/gateway/target.json). It sets `"connectorId": "states"` and lists the operations to expose. The important part is pinning:

* **State-machine operations** (`StartExecution`, `ListExecutions`, `DescribeStateMachine`) work on a `stateMachineArn`. We pin that value in the target. The Gateway removes the parameter from the schema the agent sees and fills it in at call time. The agent has no way to pick a different state machine.
* **Execution operations** (`DescribeExecution`, `GetExecutionHistory`, `StopExecution`) work on an `executionArn`, which is different for every run. It cannot be pinned, so the agent passes the ARN it got back from `StartExecution`. Access is controlled by IAM resource scoping instead.

### 3. Two tiers of IAM, because Step Functions has two ARN formats

This one trips people up. State-machine actions and execution actions are checked against different resource ARNs:

```text
State Machine ARN format:
arn:aws:states:<region>:<account-id>:stateMachine:<state-machine-name>

Execution ARN format:
arn:aws:states:<region>:<account-id>:execution:<state-machine-name>:<execution-name>
```

In `template.yaml`, the `AgentCoreGatewayServiceRole` splits these into two statements:

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

Because the execution pattern includes the state machine name, the role can only inspect or stop executions of this one pipeline. If someone tricks the agent into stopping a payroll run in the same account, IAM denies it at the API.

### 4. Calling the Gateway over MCP from Python

The client, [`agent/invoke_agent.py`](https://github.com/kien-ly/aws-step-functions-mcp-orchestrator/blob/main/agent/invoke_agent.py), speaks standard MCP JSON-RPC over SigV4-signed HTTPS. It first calls `tools/list` to check the generated schemas, then starts the run with `tools/call`:

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

After that, the client polls `states_DescribeExecution` with backoff until the status is no longer `RUNNING`, then pulls `states_GetExecutionHistory` for the step-level timing.

---

## 4. Guardrails, and the mistakes I would avoid

I want to be direct about this part. A system prompt that says *"never run any workflow except the sales pipeline, and never pass a bad S3 key"* is a wish, not a control. Prompts can be injected, context gets confused, and models change. The protections that actually hold are the ones enforced outside the model: IAM policies, Gateway pinning, and input validation in the state machine.

Here is how I think about the main risks:

| Operational risk | What it looks like in practice | Deterministic mitigation |
| --- | --- | --- |
| **Prompt injection** | A malicious string in a ticket or filename tells the agent to start a cleanup workflow or pass odd arguments to Glue. | Pin `stateMachineArn` in the Gateway target. Enforce the `raw/sales/*.parquet` pattern in the `ValidateInput` Choice state. |
| **Unauthorized execution** | The agent tries to find and start a finance or HR workflow in the same account. | Pin the ARN in the Gateway, and scope `states:StartExecution`, `ListExecutions`, and `DescribeStateMachine` in IAM to that one ARN. Never grant `states:*` or `Resource: "*"`. |
| **Stopping the wrong run** | The agent is asked to cancel a stuck job and picks up an execution ARN that belongs to something important. | Scope `StopExecution`, `DescribeExecution`, and `GetExecutionHistory` to `execution:PinnedStateMachineName:*`. |
| **Runaway retries** | A schema error makes the agent call `StartExecution` over and over, burning compute. | Keep retries inside the state machine with `MaxAttempts` and `BackoffRate`. Use the `name` parameter for idempotency, and cap tool calls per session. |
| **No audit trail** | A pipeline changed warehouse tables overnight, and nobody can tell whether a person, a schedule, or an agent started it. | Use Standard workflows, turn on CloudTrail for Step Functions and Gateway calls, and prefix agent-started execution names (for example `agent-run-*`). |
| **Long runs** | A 45-minute job outlasts the agent's conversation turn. | Start the run, return the `executionArn` straight away, and poll in later turns (or use a callback). |

### Four mistakes I have seen (and made)

1. **Using the state machine ARN for execution actions.** The deploy works, `StartExecution` works, and then the agent gets an `AccessDeniedException` when it checks status. Execution APIs need the `:execution:Name:*` form.
2. **Leaving `stateMachineArn` unpinned during testing.** The Gateway then makes it a required argument. Now you are hardcoding account IDs and regions into the prompt, and inviting the model to make up ARNs.
3. **Using an Express workflow for agent monitoring.** `DescribeExecution` and `GetExecutionHistory` are designed around Standard workflows. The tools will fail or give you nothing useful.
4. **Swallowing errors.** If the failure path ends with `"End": true`, the execution reports `SUCCEEDED`. Your agent will happily tell the user the pipeline worked just as the on-call pager goes off.

---

## 5. Where GPT-6.1 Terra fits

The Gateway is half of an agent. The other half is the model that reads the request, plans the tool calls, and writes the summary. AWS announced in the [weekly roundup](https://aws.amazon.com/blogs/aws/aws-weekly-roundup-gpt-6-1-terra-on-bedrock-step-functions-as-agent-tools-and-more-november-2-2026/) that **OpenAI GPT-6.1 Terra** is generally available on Amazon Bedrock. AWS describes it as a fast, cost-efficient model for everyday enterprise work.

One honest note: any speed, latency, or price figures from AWS or OpenAI are vendor-reported. Measure tool-call accuracy, latency, and cost per run against your own payloads before you put a number in an SLA.

With that caveat, a fast, cheap model suits this kind of work well. Here are four patterns I find useful:

* **Short tool loops.** A typical request means `StartExecution`, then `DescribeExecution`, then maybe `GetExecutionHistory`: three to five calls. A fast model keeps that feeling responsive, which is what makes an ops bot pleasant to use.
* **Triage with context.** When a run fails, `GetExecutionHistory` returns the state transitions and the error. A model can read that, check a knowledge base of runbooks and table contracts, and reply with the cause and the likely fix. A Glue error caused by a column type change is a good example.
* **Read-only assistants.** Analysts often want to know whether today's partition has loaded before a dashboard goes out. A Gateway target that exposes only `ListExecutions`, `DescribeExecution`, and `GetExecutionHistory` lets a model answer status questions in Slack, with no start or stop permissions.
* **Backfill coordination.** Over a multi-day backfill, an agent can walk through verified partitions, start runs with a concurrency limit, check each one, and stop as soon as a data-quality check fails.

### When I would not use this pattern

Not every workflow belongs behind an agent. I would skip it in three cases:

1. **Scheduled or event-driven jobs.** A nightly run at 02:00 UTC, or a run triggered by an S3 upload, should be an EventBridge rule that starts Step Functions. Adding a model there only adds cost, latency, and another way to fail.
2. **High-volume, low-latency streams.** Thousands of events per second with tight latency targets belong in Kinesis, Kafka, Lambda, or Express workflows with deterministic routing. Agent tool calls are built for control-plane decisions at human speed.
3. **Simple read-only questions.** "What were total sales in the Northeast yesterday?" is a query, not a workflow. Send it to a text-to-SQL tool on Athena, Redshift, or a semantic layer.

---

## 6. Before you go to production

Before this goes from a sandbox into a real account, I would check these:

* [ ] **Standard workflows** for anything an agent can reach, so the history is durable and queryable.
* [ ] **Pin `stateMachineArn`** in the Gateway target for every operation that takes it.
* [ ] **Two-tier IAM.** State-machine ARN for state-machine actions, the `:execution:Name:*` pattern for execution actions. No wildcard `states:*`.
* [ ] **Timeouts, retries, and catches in the ASL.** Use `TimeoutSeconds`, `Retry` with backoff for transient errors, and a `Catch` that alerts and ends in a `Fail` state.
* [ ] **Validate inputs at the start** of the state machine, before any expensive compute.
* [ ] **Log every MCP call** on the Gateway, with the caller and the payload.
* [ ] **CloudTrail and CloudWatch Logs** enabled for Step Functions, for forensics later.
* [ ] **Separate environments.** Separate Gateway targets and roles for dev, staging, and production, so a staging agent cannot touch production.
* [ ] **Rate limits.** Guard Glue and warehouse capacity against runaway loops with concurrency checks, idempotent names, or throttling.
* [ ] **Human approval for risky work.** Truncations, full reloads, and expensive cluster starts should wait on a `waitForTaskToken` callback that a person approves in Slack or through a signed link.

---

## 7. Closing thoughts

The instinct, when building agents, has been to give models direct access to cloud infrastructure. I understand the appeal. But a language model makes a poor distributed transaction coordinator. When an agent touches raw connectors, retries, idempotency, and audit trails turn into text in a prompt, and that is hard to debug at 3 a.m.

Putting Step Functions Standard workflows behind AgentCore Gateway gives you a cleaner split:

1. **The agent decides *when* and *what*.** It reads the request and fills in the parameters. Step Functions decides *how*: retries, error handling, and state history.
2. **Boundaries beat instructions.** A prompt is a request. IAM scoping, ARN pinning, and validation in the state machine are rules.
3. **You can see what happened.** Standard workflows keep a full history, so the agent can answer accurately without anyone digging through log streams.

AI agents do not remove the need for data engineering discipline. If anything, they make it more important. Our job is to design the state machines and the security boundaries that let an agent run a production platform safely, and to be honest with ourselves about where the agent should stop.

---

### Resources & Demo Repository

The complete deployable project, including the SAM template, least-privilege IAM roles, the PySpark Glue job, and the MCP client, is in this repository:

- **State machine definition:** [`state-machine/data-pipeline.asl.json`](https://github.com/kien-ly/aws-step-functions-mcp-orchestrator/blob/main/state-machine/data-pipeline.asl.json)
- **AgentCore Gateway config:** [`gateway/target.json`](https://github.com/kien-ly/aws-step-functions-mcp-orchestrator/blob/main/gateway/target.json)
- **SAM template:** [`template.yaml`](https://github.com/kien-ly/aws-step-functions-mcp-orchestrator/blob/main/template.yaml)
- **Python agent client:** [`agent/invoke_agent.py`](https://github.com/kien-ly/aws-step-functions-mcp-orchestrator/blob/main/agent/invoke_agent.py)
- **Setup guide:** [`docs/env-setup.md`](https://github.com/kien-ly/aws-step-functions-mcp-orchestrator/blob/main/docs/env-setup.md)
