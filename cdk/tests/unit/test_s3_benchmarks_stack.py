import aws_cdk as core
import aws_cdk.assertions as assertions

from s3_benchmarks.s3_benchmarks_stack import S3BenchmarksStack

# example tests. To run these tests, uncomment this file along with the example
# resource in s3_benchmarks/s3_benchmarks_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = S3BenchmarksStack(app, "s3-benchmarks")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
