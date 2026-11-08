# Graph Report - step-functions-mcp-orchestrator  (2026-11-08)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 230 nodes · 288 edges · 13 communities (11 shown, 2 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 35 edges (avg confidence: 0.83)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `8af53ae5`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- Agent Client and MCP RPC
- Glue Job Configuration
- CloudFormation Resource Types
- ETL Pipeline Architecture
- Stack Outputs
- AgentCore Gateway and IAM
- IAM Policy Documents
- SAM Template Parameters
- Operator Workflow and Client
- DynamoDB Table Schema
- S3 Bucket Security
- Cleanup Script
- Deploy Script

## God Nodes (most connected - your core abstractions)
1. `Resources` - 11 edges
2. `main()` - 10 edges
3. `Properties` - 10 edges
4. `AgentCoreMCPClient` - 8 edges
5. `Outputs` - 8 edges
6. `Properties` - 7 edges
7. `From ETL Jobs to Agentic Orchestrators (deep-dive article)` - 7 edges
8. `deploy.sh / cleanup.sh` - 7 edges
9. `Statement` - 6 edges
10. `Properties` - 6 edges

## Surprising Connections (you probably didn't know these)
- `Troubleshooting: AccessDeniedException on DescribeExecution or GetExecutionHistory` --semantically_similar_to--> `Two-tier least-privilege IAM design`  [INFERRED] [semantically similar]
  README.md → docs/article.md
- `Troubleshooting: Glue script S3 Object Not Found` --references--> `StartGlueJob state (glue startJobRun.sync)`  [INFERRED]
  README.md → docs/article.md
- `state-machine/ Step Functions definition` --references--> `NotifyFailure and WorkflowFailed Fail state`  [INFERRED]
  AGENTS.md → docs/article.md
- `state-machine/ Step Functions definition` --references--> `ValidateInput Choice state`  [INFERRED]
  AGENTS.md → docs/article.md
- `Bedrock AgentCore Gateway (connectorId: states, pinned target.json)` --implements--> `Amazon Bedrock AgentCore Gateway`  [INFERRED]
  .archify/architecture-step-functions-mcp-20261107-215000/architecture.html → docs/article.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Agent-to-workflow execution path** — _archify_architecture_step_functions_mcp_20261107_215000_architecture_agent, _archify_architecture_step_functions_mcp_20261107_215000_architecture_gateway, _archify_architecture_step_functions_mcp_20261107_215000_architecture_sfn, _archify_architecture_step_functions_mcp_20261107_215000_architecture_glue, _archify_architecture_step_functions_mcp_20261107_215000_architecture_s3_data, _archify_architecture_step_functions_mcp_20261107_215000_architecture_dynamodb, _archify_architecture_step_functions_mcp_20261107_215000_architecture_sns [EXTRACTED 1.00]
- **Input validation and failure routing path** — docs_article_validate_input_state, docs_article_start_glue_job_state, docs_article_notify_failure_fail_state, docs_article_sns_alerts [EXTRACTED 1.00]
- **Deterministic security boundary (ARN pinning, two-tier IAM)** — docs_article_arn_pinning, docs_article_two_tier_iam, agents_template_yaml, agents_gateway_target_json, _archify_architecture_step_functions_mcp_20261107_215000_architecture_iam_gateway [INFERRED 0.85]

## Communities (13 total, 2 thin omitted)

### Community 0 - "Agent Client and MCP RPC"
Cohesion: 0.10
Nodes (4): AgentCoreMCPClient, extract_source_key_from_prompt(), main(), parse_mcp_tool_json()

### Community 1 - "Glue Job Configuration"
Cohesion: 0.07
Nodes (31): Name, PythonVersion, ScriptLocation, --enable-continuous-cloudwatch-log, --enable-metrics, --job-language, --TempDir, AlertTopicArn (+23 more)

### Community 2 - "CloudFormation Resource Types"
Cohesion: 0.08
Nodes (26): Type, Type, Properties, Type, Properties, Type, Endpoint, LogGroupName (+18 more)

### Community 3 - "ETL Pipeline Architecture"
Cohesion: 0.15
Nodes (22): Orchestrated ETL Execution Domain (security group frame), DynamoDB Audit Table, AWS Glue PySpark Job (startJobRun.sync), Step Functions Workflow (Standard State Machine), Amazon SNS Alerts, stepfunctions-agentcore-demo project, state-machine/ Step Functions definition, Amazon Bedrock AgentCore Gateway (+14 more)

### Community 4 - "Stack Outputs"
Cohesion: 0.09
Nodes (22): Description, Value, Description, Value, Description, Value, Description, Value (+14 more)

### Community 5 - "AgentCore Gateway and IAM"
Cohesion: 0.16
Nodes (20): Bedrock AgentCore Gateway (connectorId: states, pinned target.json), Gateway Execution Role (least-privilege IAM), AWS Cloud Services (region frame), Amazon S3 Data Lake (raw/sales parquet and scripts), deploy.sh / cleanup.sh, gateway/target.json (AgentCore Gateway target config), Prerequisites (AWS CLI v2.15+, SAM CLI v1.115+, Python 3.10+, jq 1.6+), template.yaml (AWS SAM stack) (+12 more)

### Community 6 - "IAM Policy Documents"
Cohesion: 0.13
Nodes (19): Properties, Statement, Version, Properties, PolicyDocument, PolicyName, Statement, Version (+11 more)

### Community 7 - "SAM Template Parameters"
Cohesion: 0.12
Nodes (16): Default, Description, Type, AWSTemplateFormatVersion, Description, Default, Description, Type (+8 more)

### Community 8 - "Operator Workflow and Client"
Cohesion: 0.21
Nodes (13): AI Agent Client (invoke_agent.py, SigV4), Operator / User (natural language prompt), Commands, .env.example (copy to .env before deploying), agent/invoke_agent.py (Python MCP client), Python MCP client (tools/list and tools/call), Step 1: Configuration, Step 3: Invocation and Validation (+5 more)

### Community 9 - "DynamoDB Table Schema"
Cohesion: 0.15
Nodes (13): AttributeName, AttributeType, AttributeName, KeyType, Properties, Type, AttributeDefinitions, BillingMode (+5 more)

### Community 10 - "S3 Bucket Security"
Cohesion: 0.17
Nodes (12): ServerSideEncryptionConfiguration, BucketEncryption, BucketName, PublicAccessBlockConfiguration, BlockPublicAcls, BlockPublicPolicy, IgnorePublicAcls, RestrictPublicBuckets (+4 more)

## Ambiguous Edges - Review These
- `DynamoDB audit table (RecordSuccess)` → `Redshift COPY metadata update (alternative audit path)`  [AMBIGUOUS]
  docs/article.md · relation: conceptually_related_to

## Knowledge Gaps
- **8 isolated node(s):** `Redshift COPY metadata update (alternative audit path)`, `stepfunctions-agentcore-demo project`, `Express Workflow`, `Prerequisites (AWS CLI v2.15+, SAM CLI v1.115+, Python 3.10+, jq 1.6+)`, `Working Rules` (+3 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 113 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **2 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Resources` connect `CloudFormation Resource Types` to `DynamoDB Table Schema`, `S3 Bucket Security`, `SAM Template Parameters`?**
  _High betweenness centrality (0.300) - this node is a cross-community bridge._
- **What connects `Redshift COPY metadata update (alternative audit path)`, `stepfunctions-agentcore-demo project`, `Express Workflow` to the rest of the system?**
  _8 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Agent Client and MCP RPC` be split into smaller, more focused modules?**
  _Cohesion score 0.1010752688172043 - nodes in this community are weakly interconnected._
- **Why does `Outputs` connect `Stack Outputs` to `SAM Template Parameters`?**
  _High betweenness centrality (0.102) - this node is a cross-community bridge._
- **Should `Glue Job Configuration` be split into smaller, more focused modules?**
  _Cohesion score 0.06666666666666667 - nodes in this community are weakly interconnected._
- **Why does `Properties` connect `Glue Job Configuration` to `CloudFormation Resource Types`?**
  _High betweenness centrality (0.079) - this node is a cross-community bridge._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `DynamoDB audit table (RecordSuccess)` and `Redshift COPY metadata update (alternative audit path)`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._