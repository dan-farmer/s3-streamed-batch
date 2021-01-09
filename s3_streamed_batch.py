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
