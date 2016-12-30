import os
import sys
import re
import logging
import click
import threading
import queue
import signal

from time import sleep
from boto import s3, connect_s3
from configstruct import ConfigStruct

from .worker import Worker
from .progress import S3KeyProgress
from .jobs import S3ListJob
from .reducer import Reducer

SHARDS = ([chr(i) for i in range(ord('a'), ord('z') + 1)] +
          [chr(i) for i in range(ord('0'), ord('9') + 1)])

DEFAULTS = {
    'concurrency': len(SHARDS),
}

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option('-c', '--config-file', type=click.Path(dir_okay=False, writable=True),
              default=os.path.join(os.path.expanduser('~'), '.s3tailrc'),
              help='Configuration file', show_default=True)
@click.option('-r', '--region', type=click.Choice(r.name for r in s3.regions()),
              help='AWS region to use when connecting')
@click.option('-l', '--log-level',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'critical']),
              help='set logging level')
@click.option('--log-file', metavar='FILENAME', help='write logs to FILENAME')
@click.option('--concurrency', type=int, default=DEFAULTS['concurrency'], show_default=True,
              help='set number of workers processing jobs simultaneously')
@click.option('--select', 'selection_string',
              help='provide comparisons against object name, size, md5, or last_modified to limit '
                   'selection')
@click.option('--reduce', 'reduction_string',
              help='provide reduction logic against the accumulator value for all selected objects')
@click.option('--accumulator', 'accumulation_string', default='0', show_default=True,
              help='provide a different initial accumulation value for the reduce option')
@click.argument('command', type=click.Choice(['list', 'delete']))
@click.argument('s3_uri')
def main(config_file, region, log_level, log_file, concurrency,
         selection_string, accumulation_string, reduction_string,
         command, s3_uri):
    '''Perform simple listing, collating, or deleting of many S3 objects at the same time.

    Examples:

      \b
      List empty objects:
          s3workers list --select 'size == 0' s3://mybucket/myprefix
      \b
      Report total of all non-empty objects:
          s3workers list --select 'size > 0' --reduce 'accumulator += size' s3://mybucket/myprefix
      \b
      Total size group by MD5:
          s3workers list --accumulator '{}' --reduce 'v=accumulator.get(md5,0)+size; accumulator[md5]=v' s3://mybucket/myprefix
    '''  # noqa: E501

    config = ConfigStruct(config_file, 'options', options=DEFAULTS)
    opts = config.options
    # let command line options have temporary precedence if provided values
    opts.might_prefer(region=region, log_level=log_level, log_file=log_file,
                      concurrency=concurrency)
    config.configure_basic_logging(__name__)
    logger = logging.getLogger(__name__)

    progress = S3KeyProgress()
    reducer = None

    if reduction_string:
        reducer = Reducer(reduction_string, accumulation_string)

        def key_dumper(key):
            accumulator = reducer.reduce(key.name, key.size, key.md5, key.last_modified)
            progress.write('%s %10d %s %s => %s',
                           key.last_modified, key.size, key.md5, key.name, accumulator)
    else:
        def key_dumper(key):
            progress.write('%s %10d %s %s', key.last_modified, key.size, key.md5, key.name)

    def key_deleter(key):
        progress.write('DELETING: %s %10d %s %s', key.last_modified, key.size, key.md5, key.name)
        key.delete()

    work = queue.Queue(opts.concurrency * 3)
    workers = []

    for i in range(opts.concurrency):
        worker = Worker(work)
        worker.start()
        workers.append(worker)

    stopped = threading.Event()

    def stop_work(*args):
        stopped.set()
        logger.info('Stopping! work_item_count=%d', work.qsize())
        for worker in workers:
            if worker.is_alive():
                logger.debug(worker)
                worker.stop()

    def s3workers_exception_handler(type, value, traceback):
        '''Ensure application does not hang waiting on the workers for unhandled exceptions.'''
        sys.__excepthook__(type, value, traceback)
        stop_work()

    sys.excepthook = s3workers_exception_handler

    signal.signal(signal.SIGINT, stop_work)
    signal.signal(signal.SIGTERM, stop_work)
    signal.signal(signal.SIGPIPE, stop_work)

    s3_uri = re.sub(r'^(s3:)?/+', '', s3_uri)
    items = s3_uri.split('/', 1)
    bucket_name = items[0]
    prefix = items[1] if len(items) > 1 else ''

    selector = compile(selection_string, '<select>', 'eval') if selection_string else None
    handler = key_deleter if command == 'delete' else key_dumper

    conn = s3.connect_to_region(opts.region) if opts.region else connect_s3()
    bucket = conn.get_bucket(bucket_name)

    logger.info('Preparing %d jobs for %d workers', len(SHARDS) * len(SHARDS), len(workers))

    # break up jobs into single char prefix jobs
    for shard in SHARDS:
        if stopped.isSet():
            break
        job = S3ListJob(bucket, prefix + shard, selector, handler, progress.report)
        logger.debug('Submitting %s', job)
        work.put(job)

    Worker.all_jobs_submitted()
    logger.debug('All jobs submitted. Waiting on %d to complete.', work.qsize())

    while threading.active_count() > 1:
        sleep(0.1)

    for worker in workers:
        worker.join(1)

    progress.finish()

    if reducer:
        click.echo('Final accumulator value: ' + str(reducer.accumulator))


if __name__ == '__main__':
    main()
