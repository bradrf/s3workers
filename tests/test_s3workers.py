#!/usr/bin/env python

'''
test_s3workers
----------------------------------

Tests for `s3workers` module.
'''

import pytest

from contextlib import contextmanager
from click.testing import CliRunner

import s3workers
from s3workers.cli import main


def test_command_line_interface():
    runner = CliRunner()
    result = runner.invoke(main)
    assert result.exit_code == 2 # requires a command
    assert 'S3_URI' in result.output
    help_result = runner.invoke(main, ['--help'])
    assert help_result.exit_code == 0
    assert 'Show this message and exit.' in help_result.output
