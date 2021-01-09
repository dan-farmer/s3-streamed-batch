#!/usr/bin/env python3

# Author: Dan Farmer
# SPDX-License-Identifier: GPL-3.0-only

"""
Proof-of-concept AWS Lambda function code to process lines from a text file stored compressed in AWS
S3, in fixed batches/pages, without downloading the file or decompressing it on-disk.

Example use-case: CloudFront or ALB log digests delivered to S3, triggering Lambda function via SNS
to batch-submit log entries to a log storage/processing API
"""

import logging
import os
import zlib
import boto3

COMPRESSED_CHUNK_SIZE_MIB=8     # Chunk size to read from S3 (MiB)

LOG = logging.getLogger(__name__)
LOG.setLevel(os.environ.get('LOG_LEVEL', 'WARNING'))
if not LOG.hasHandlers():
    # If we weren't executed by AWS Lambda (i.e. directly on a dev workstation), we need to add a
    # basic logging handler for stderr
    LOG.addHandler(logging.StreamHandler())

def lambda_handler(event, context):
    """Main lambda event handler."""
    # AWS Lambda will call us with an event and context argument, but we have no use for the context
    del context

    line_iter = get_lines(bucket=event['Records'][0]['s3']['bucket']['name'],
                          key=event['Records'][0]['s3']['object']['key'])

def get_lines(bucket, key):
    """
    Generator for lines in gzipped text streamed S3 object.

    Returns iterator of lines
    """
    s3_client = boto3.client('s3')
    file_stream = s3_client.get_object(Bucket=bucket, Key=key)['Body']

    decompressor = zlib.decompressobj(32 + zlib.MAX_WBITS)  # Magic window size for gzip streams
    leftover = b''

    # Iterate over chunks of file_stream. With typical request log data, this keeps our memory
    # usage under ~32 * COMPRESSED_CHUNK_SIZE_MIB regardless of absolute compressed or uncompressed
    # filesize. Very small chunk sizes are less optimal because of increased S3 API calls.
    for chunk in file_stream.iter_chunks(chunk_size=COMPRESSED_CHUNK_SIZE_MIB*1024**2):
        lines = (leftover + decompressor.decompress(decompressor.unconsumed_tail + chunk))\
                .splitlines()

        # If decompressor did not reach end of compressed data stream (i.e. This is not the last
        # chunk), then our last line is likely incomplete. Pop it off so we can prepend it to the
        # lines from the next chunk. This is fine even if the line was (by chance) complete.
        if not decompressor.eof:
            leftover = lines.pop(-1)

        for line in lines:
            yield line.decode('utf-8')

if __name__ == '__main__':
    # Testing entry point; Invocation by AWS Lambda will call lambda_handler directly
    dummy_event = {
                      'Records': [
                          {
                              's3': {
                                  'bucket': {
                                      'name': 'example-bucket'
                                  },
                                  'object': {
                                      'key': 'example-text-file.gz'
                                  }
                              }
                          }
                      ]
                  }
    lambda_handler(dummy_event, 'dummy_context')
