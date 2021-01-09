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
from itertools import chain, islice
#from itertools import chain, islice, zip_longest
import boto3

COMPRESSED_CHUNK_SIZE_MIB=8     # Chunk size to read from S3 (MiB)
HEADER_LINES=2                  # Number of header lines to discard (e.g. CSV header)
PAGE_SIZE=1000                  # Number of lines in page/batch

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

    # Discard header lines
    for _ in range(HEADER_LINES):
        next(line_iter)

    for page_first_line in line_iter:
        # Create an iterator for the next (n-1) lines using islice and chain this with the first
        # line to get a page of (n) lines. The underlying line_iter will be iterated as we process
        # each item in this page, so on the next pass of the outer for loop line_iter will have
        # iterated (n) items.
        for line in chain([page_first_line], islice(line_iter, PAGE_SIZE - 1)):
            # Placeholder; Do something useful with the line here
            print(line)

    # Alternative method using a classic itertools 'grouper' recipe. This works by creating
    # PAGE_SIZE repeated references to the line_iter object and 'zipping' them together (round-robin
    # between them). The underlying line_iter will be iterated every time we iterate on one of the
    # object references.
    #
    # Experimentally, this is less efficient than the chain method above with large page sizes (e.g.
    # 1000).
    #
    # Pages will be padded to PAGE_SIZE with 'None' objects, so we must filter these out. The
    # filtering makes this approximately equivalent to the following, but is a little faster as
    # filter is implemented in C and more efficient than a generator expression:
    #
    # for page in itertools.zip_longest(*[iter(line_iter)]*PAGE_SIZE):
    #     for line in (x for x in page if x):
    #         print(line)

    #for page in (filter(None, page) for page in zip_longest(*[iter(line_iter)]*PAGE_SIZE)):
    #    for line in page:
    #        # Placeholder; Do something useful with the line here
    #        print(line)

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
