#!/usr/bin/env python3
import os

import aws_cdk as cdk

from s3_benchmarks.s3_benchmarks_stack import S3BenchmarksStack


app = cdk.App()
S3BenchmarksStack(app, "S3BenchmarksStack")
app.synth()
