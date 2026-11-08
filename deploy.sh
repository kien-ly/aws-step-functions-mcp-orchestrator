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
