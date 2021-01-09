# s3-streamed-batch
Proof-of-concept AWS Lambda function code to process lines from a text file stored compressed in AWS S3, in fixed batches/pages, without downloading the file or decompressing it on-disk.
  
Example use-case: CloudFront or ALB log digests delivered to S3, triggering Lambda function via SNS to batch-submit log entries to a log storage/processing API.
