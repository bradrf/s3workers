#!/usr/bin/env python

'''
test_s3workers
----------------------------------

Tests for `s3workers` module.
'''

import pytest

from boto import connect_s3
from boto.s3.key import Key
from moto import mock_s3
from click.testing import CliRunner

import s3workers
from s3workers.cli import main


class S3Helper(object):
    def __init__(self, bucket_name='mybucket', **objects):
        self.conn = connect_s3()
        self.bucket = self.conn.create_bucket(bucket_name)
        self.add(**objects)

    def add(self, **objects):
        for name, value in objects.items():
            k = Key(self.bucket)
            k.key = name
            k.set_contents_from_string(value)


def test_command_line_interface():
    runner = CliRunner()
    result = runner.invoke(main)
    assert result.exit_code == 2 # requires a command
    assert 'S3_URI' in result.output
    help_result = runner.invoke(main, ['--help'])
    assert help_result.exit_code == 0
    assert 'Show this message and exit.' in help_result.output

@mock_s3
def test_empty_list():
    runner = CliRunner()
    s3helper = S3Helper()
    result = runner.invoke(main, ['--concurrency', '1', 'list', 'mybucket'])
    assert 'Selected 0 of 0 keys' in result.output

@mock_s3
def test_simple_list():
    runner = CliRunner()
    s3helper = S3Helper(one='1', two='2')
    result = runner.invoke(main, ['list', 'mybucket'])
    assert 'Selected 2 of 2 keys' in result.output

@mock_s3
def test_selected_list():
    runner = CliRunner()
    s3helper = S3Helper(one='1', two='22')
    result = runner.invoke(main, ['--select', 'size == 1', 'list', 'mybucket'])
    assert 'Selected 1 of 2 keys' in result.output
