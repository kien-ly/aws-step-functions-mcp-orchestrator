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
