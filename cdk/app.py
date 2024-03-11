#!/usr/bin/env python3
from dataclasses import dataclass
import json
from typing import Optional

import aws_cdk as cdk

from s3_benchmarks.s3_benchmarks_stack import S3BenchmarksStack
from s3_benchmarks import get_bucket_storage_class, is_s3express_bucket


@dataclass
class Settings:
    account: str
    region: str
    buckets: Optional[list[str]] = None
    availability_zone: Optional[str] = None
    canary: bool = False


def load_settings(app: cdk.App) -> Settings:
    settings_path = app.node.try_get_context("settings")
    if settings_path is None:
        exit('S3BenchmarksStack requires you to to pass settings ' +
             'via: -c settings=<path>. See README.md for more details.')

    with open(settings_path) as f:
        settings_json = json.load(f)

    settings = Settings(**settings_json)

    if settings.buckets:
        # Availability Zone super matters for S3 Express One Zone
        if any([is_s3express_bucket(x) for x in settings.buckets]) and not settings.availability_zone:
            exit("availability_zone must be specified when using " +
                 "S3 Express One Zone directory buckets.")

        # If multiple buckets, they can't be the same storage class.
        # You wouldn't be able to tell whose metrics are whose, since
        # we use StorageClass as a metrics dimension.
        num_storage_classes = len({get_bucket_storage_class(x)
                                  for x in settings.buckets})
        if num_storage_classes != len(settings.buckets):
            exit("Cannot benchmark multiple buckets unless they're different storage classes " +
                 "(i.e. S3 Express, S3 Standard).")

    return settings


app = cdk.App()

# Add this tag to everything, so we can identify its costs in the AWS bill.
cdk.Tags.of(app).add("Project", "S3Benchmarks")

settings = load_settings(app)
S3BenchmarksStack(
    app, "S3BenchmarksStack",
    description="Stack for running S3 benchmarks on specific EC2 instance types",
    env=cdk.Environment(
        account=settings.account, region=settings.region),
    existing_bucket_names=settings.buckets,
    availability_zone=settings.availability_zone,
    add_canary=settings.canary,
)
app.synth()
