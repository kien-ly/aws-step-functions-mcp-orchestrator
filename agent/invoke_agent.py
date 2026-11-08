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
