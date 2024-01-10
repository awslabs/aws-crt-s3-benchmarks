#!/usr/bin/env python3
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Optional

import aws_cdk as cdk

from s3_benchmarks.s3_benchmarks_stack import S3BenchmarksStack


@dataclass
class Settings:
    account: str
    region: str
    bucket: Optional[str]


def load_settings(app: cdk.App) -> Settings:
    settings_path = app.node.try_get_context("settings")
    if settings_path is None:
        exit('S3BenchmarksStack requires you to to pass settings ' +
             'via: -c settings=<path>. See README.md for more details.')

    with open(settings_path) as f:
        settings_json = json.load(f)

    settings = Settings(**settings_json)
    return settings


app = cdk.App()
settings = load_settings(app)
S3BenchmarksStack(
    app, "S3BenchmarksStack",
    description="Stack for running S3 benchmarks on specific EC2 instance types",
    env=cdk.Environment(
        account=settings.account, region=settings.region),
    existing_bucket_name=settings.bucket,
)
app.synth()
