=====
Usage
=====

To use s3workers from the command line (CLI)
--------------------------------------------

.. code-block:: console

    $ s3workers --help

    Usage: s3workers [OPTIONS] COMMAND S3_URI

      Perform simple listing, collating, or deleting of many S3 objects at the
      same time.

      Examples:

          List empty objects:
              s3workers list --select 'size == 0' s3://mybucket/myprefix

          Report total of all non-empty objects:
              s3workers list --select 'size > 0' --reduce 'accumulator += size' s3://mybucket/myprefix

          Total size group by MD5:
              s3workers list --accumulator '{}' --reduce 'v=accumulator.get(md5,0)+size; accumulator[md5]=v' s3://mybucket/myprefix

    Options:
      --version                       Show the version and exit.
      -c, --config-file PATH          Configuration file  [default:
                                      /Users/brad/.s3tailrc]
      -r, --region [us-east-1|us-west-1|us-gov-west-1|ap-northeast-2|ap-northeast-1|sa-east-1|eu-central-1|ap-southeast-1|ca-central-1|ap-southeast-2|us-west-2|us-east-2|ap-south-1|cn-north-1|eu-west-1|eu-west-2]
                                      AWS region to use when connecting
      -l, --log-level [debug|info|warning|error|critical]
                                      set logging level
      --log-file FILENAME             write logs to FILENAME
      --concurrency INTEGER           set number of workers processing jobs
                                      simultaneously  [default: 36]
      --select TEXT                   provide comparisons against object name,
                                      size, md5, or last_modified to limit
                                      selection
      --reduce TEXT                   provide reduction logic against the
                                      accumulator value for all selected objects
      --accumulator TEXT              provide a different initial accumulation
                                      value for the reduce option  [default: 0]
      -h, --help                      Show this message and exit.


To use s3workers in a project
-----------------------------

.. code-block:: python

    import boto
    import s3workers

    manager = s3workers.Manager(3)

    bucket = boto.connect_s3().get_bucket('unitycloud-collab-store-development')
    progress = s3workers.S3KeyProgress()

    def key_dumper(key):
        progress.write('%s %10d %s %s', key.last_modified, key.size, key.md5, key.name)

    job = s3workers.S3ListJob(bucket, 'brad/f', None, key_dumper, progress.report)
    manager.add_work(job)

    manager.start_workers()
    manager.wait_for_workers()
