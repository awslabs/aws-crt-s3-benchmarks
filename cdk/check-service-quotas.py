#!/usr/bin/env python3
import argparse
import boto3  # type: ignore

import s3_benchmarks

PARSER = argparse.ArgumentParser(
    description="Check AWS Service Quotas needed for this CDK app")
PARSER.add_argument("--region", required=True,
                    help="AWS region (e.g. us-west-2)")

args = PARSER.parse_args()

# Find quotas needed to run each instance type one at a time.
# (quota value is number of running vCPUs)
quotas_needed: dict[str, int] = {}
for instance_type in s3_benchmarks.INSTANCE_TYPES.values():
    code = instance_type.quota_code
    prev_needed = quotas_needed.get(code, 0)
    quotas_needed[code] = max(instance_type.vcpu, prev_needed)

# The orchestrator is running at the same time, so add that in too.
orchestrator = s3_benchmarks.ORCHESTRATOR_INSTANCE_TYPE
quotas_needed[orchestrator.quota_code] += orchestrator.vcpu

# Check our current quota values
client = boto3.client('service-quotas', region_name=args.region)
exit_code = 0
for quota_code, value_needed in quotas_needed.items():
    response = client.get_service_quota(
        ServiceCode='ec2', QuotaCode=quota_code)
    quota = response['Quota']
    name = quota['QuotaName']
    current_value = quota['Value']
    msg = f"ec2: {name}. currently:{current_value} min-required:{value_needed}"
    if current_value < value_needed:
        print(f"❌ {msg}")
        console_url = f"https://{args.region}.console.aws.amazon.com/servicequotas/home/services/ec2/quotas/{quota_code}?region={args.region}"
        print(f"  👉 Request increase here: {console_url}")
        exit_code = 1
    else:
        print(f"✅ {msg}")
exit(exit_code)
