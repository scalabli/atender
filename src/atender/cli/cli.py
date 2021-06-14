# -*- coding: utf-8 -*-
"""
atender command line tool
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from functools import update_wrapper
import os
import sys

import quo
from redis.exceptions import ConnectionError

from atender import Connection, __version__ as version
from atender.cli.helpers import (read_config_file, refresh,
                            setup_loghandlers_from_args,
                            show_both, show_queues, show_workers, CliConfig)
from atender.contrib.legacy import cleanup_ghosts
from atender.defaults import (DEFAULT_CONNECTION_CLASS, DEFAULT_JOB_CLASS,
                         DEFAULT_QUEUE_CLASS, DEFAULT_WORKER_CLASS,
                         DEFAULT_RESULT_TTL, DEFAULT_WORKER_TTL,
                         DEFAULT_JOB_MONITORING_INTERVAL,
                         DEFAULT_LOGGING_FORMAT, DEFAULT_LOGGING_DATE_FORMAT,
                         DEFAULT_SERIALIZER_CLASS)
from atender.exceptions import InvalidJobOperationError
from atender.registry import FailedJobRegistry, clean_registries
from atender.utils import import_attribute
from atender.suspension import (suspend as connection_suspend,
                           resume as connection_resume, is_suspended)
from atender.worker_registration import clean_worker_registry



# Disable the warning that quo displays (as of quo version 5.0) when users
# use unicode_literals in Python 2.
# See http://quo.pocoo.org/dev/python3/#unicode-literals for more details.
quo.disable_unicode_literals_warning = True


shared_options = [
    quo.option('--url', '-u',
                 envvar='RQ_REDIS_URL',
                 help='URL describing Redis connection details.'),
    quo.option('--config', '-c',
                 envvar='RQ_CONFIG',
                 help='Module containing atender settings.'),
    quo.option('--worker-class', '-w',
                 envvar='RQ_WORKER_CLASS',
                 default=DEFAULT_WORKER_CLASS,
                 help='atender Worker class to use'),
    quo.option('--job-class', '-j',
                 envvar='RQ_JOB_CLASS',
                 default=DEFAULT_JOB_CLASS,
                 help='atender Job class to use'),
    quo.option('--queue-class',
                 envvar='RQ_QUEUE_CLASS',
                 default=DEFAULT_QUEUE_CLASS,
                 help='atender Queue class to use'),
    quo.option('--connection-class',
                 envvar='RQ_CONNECTION_CLASS',
                 default=DEFAULT_CONNECTION_CLASS,
                 help='Redis client class to use'),
    quo.option('--path', '-P',
                 default=['.'],
                 help='Specify the import path.',
                 multiple=True),
    quo.option('--serializer', '-S',
                 default=DEFAULT_SERIALIZER_CLASS,
                 help='Path to serializer, defaults to atender.serializers.DefaultSerializer')
]


def pass_cli_config(func):
    # add all the shared options to the command
    for option in shared_options:
        func = option(func)

    # pass the cli config object into the command
    def wrapper(*args, **kwargs):
        ctx = quo.currentcontext()
        cli_config = CliConfig(**kwargs)
        return ctx.invoke(func, cli_config, *args[1:], **kwargs)

    return update_wrapper(wrapper, func)


@quo.tether()
@quo.autoversion(version)
def main():
    """atender command line tool."""
    pass


@main.command()
@quo.option('--all', '-a', is_flag=True, help='Empty all queues')
@quo.argument('queues', nargs=-1)
@pass_cli_config
def empty(cli_config, all, queues, **options):
    """Empty given queues."""

    if all:
        queues = cli_config.queue_class.all(connection=cli_config.connection,
                                            job_class=cli_config.job_class)
    else:
        queues = [cli_config.queue_class(queue,
                                         connection=cli_config.connection,
                                         job_class=cli_config.job_class)
                  for queue in queues]

    if not queues:
        quo.echo('Nothing to do')
        sys.exit(0)

    for queue in queues:
        num_jobs = queue.empty()
        quo.echo('{0} jobs removed from {1} queue'.format(num_jobs, queue.name))


@main.command()
@quo.option('--all', '-a', is_flag=True, help='Requeue all failed jobs')
@quo.option('--queue', required=True, type=str)
@quo.argument('job_ids', nargs=-1)
@pass_cli_config
def requeue(cli_config, queue, all, job_class, job_ids, **options):
    """Requeue failed jobs."""

    failed_job_registry = FailedJobRegistry(queue,
                                            connection=cli_config.connection)
    if all:
        job_ids = failed_job_registry.get_job_ids()

    if not job_ids:
        quo.echo('Nothing to do')
        sys.exit(0)

    quo.echo('Requeueing {0} jobs from failed queue'.format(len(job_ids)))
    fail_count = 0
    with quo.progressbar(job_ids) as job_ids:
        for job_id in job_ids:
            try:
                failed_job_registry.requeue(job_id)
            except InvalidJobOperationError:
                fail_count += 1

    if fail_count > 0:
        quo.flair('Unable to requeue {0} jobs from failed job registry'.format(fail_count), fg='red')


@main.command()
@quo.option('--interval', '-i', type=float, help='Updates stats every N seconds (default: don\'t poll)')
@quo.option('--raw', '-r', is_flag=True, help='Print only the raw numbers, no bar charts')
@quo.option('--only-queues', '-Q', is_flag=True, help='Show only queue info')
@quo.option('--only-workers', '-W', is_flag=True, help='Show only worker info')
@quo.option('--by-queue', '-R', is_flag=True, help='Shows workers by queue')
@quo.argument('queues', nargs=-1)
@pass_cli_config
def info(cli_config, interval, raw, only_queues, only_workers, by_queue, queues,
         **options):
    """atender command-line monitor."""

    if only_queues:
        func = show_queues
    elif only_workers:
        func = show_workers
    else:
        func = show_both

    try:
        with Connection(cli_config.connection):

            if queues:
                qs = list(map(cli_config.queue_class, queues))
            else:
                qs = cli_config.queue_class.all()

            for queue in qs:
                clean_registries(queue)
                clean_worker_registry(queue)

            refresh(interval, func, qs, raw, by_queue,
                    cli_config.queue_class, cli_config.worker_class)
    except ConnectionError as e:
        quo.echo(e)
        sys.exit(1)
    except KeyboardInterrupt:
        quo.echo()
        sys.exit(0)


@main.command()
@quo.option('--burst', '-b', is_flag=True, help='Run in burst mode (quit after all work is done)')
@quo.option('--logging_level', type=str, default="INFO", help='Set logging level')
@quo.option('--log-format', type=str, default=DEFAULT_LOGGING_FORMAT, help='Set the format of the logs')
@quo.option('--date-format', type=str, default=DEFAULT_LOGGING_DATE_FORMAT, help='Set the date format of the logs')
@quo.option('--name', '-n', help='Specify a different name')
@quo.option('--results-ttl', type=int, default=DEFAULT_RESULT_TTL, help='Default results timeout to be used')
@quo.option('--worker-ttl', type=int, default=DEFAULT_WORKER_TTL, help='Default worker timeout to be used')
@quo.option('--job-monitoring-interval', type=int, default=DEFAULT_JOB_MONITORING_INTERVAL, help='Default job monitoring interval to be used')
@quo.option('--disable-job-desc-logging', is_flag=True, help='Turn off description logging.')
@quo.option('--verbose', '-v', is_flag=True, help='Show more output')
@quo.option('--quiet', '-q', is_flag=True, help='Show less output')
@quo.option('--sentry-ca-certs', envvar='RQ_SENTRY_CA_CERTS', help='Path to CRT file for Sentry DSN')
@quo.option('--sentry-debug', envvar='RQ_SENTRY_DEBUG', help='Enable debug')
@quo.option('--sentry-dsn', envvar='RQ_SENTRY_DSN', help='Report exceptions to this Sentry DSN')
@quo.option('--exception-handler', help='Exception handler(s) to use', multiple=True)
@quo.option('--pid', help='Write the process ID number to a file at the specified path')
@quo.option('--disable-default-exception-handler', '-d', is_flag=True, help='Disable atender\'s default exception handler')
@quo.option('--max-jobs', type=int, default=None, help='Maximum number of jobs to execute')
@quo.option('--with-scheduler', '-s', is_flag=True, help='Run worker with scheduler')
@quo.option('--serializer', '-S', default=None, help='Run worker with custom serializer')
@quo.argument('queues', nargs=-1)
@pass_cli_config
def worker(cli_config, burst, logging_level, name, results_ttl,
           worker_ttl, job_monitoring_interval, disable_job_desc_logging,
           verbose, quiet, sentry_ca_certs, sentry_debug, sentry_dsn,
           exception_handler, pid, disable_default_exception_handler, max_jobs,
           with_scheduler, queues, log_format, date_format, serializer, **options):
    """Starts an atender worker."""
    settings = read_config_file(cli_config.config) if cli_config.config else {}
    # Worker specific default arguments
    queues = queues or settings.get('QUEUES', ['default'])
    sentry_ca_certs = sentry_ca_certs or settings.get('SENTRY_CA_CERTS')
    sentry_debug = sentry_debug or settings.get('SENTRY_DEBUG')
    sentry_dsn = sentry_dsn or settings.get('SENTRY_DSN')
    name = name or settings.get('NAME')

    if pid:
        with open(os.path.expanduser(pid), "w") as fp:
            fp.write(str(os.getpid()))

    setup_loghandlers_from_args(verbose, quiet, date_format, log_format)

    try:
        cleanup_ghosts(cli_config.connection)
        exception_handlers = []
        for h in exception_handler:
            exception_handlers.append(import_attribute(h))

        if is_suspended(cli_config.connection):
            quo.flair('atender is currently suspended, to resume job execution run "atender resume"', fg='red')
            sys.exit(1)

        queues = [cli_config.queue_class(queue,
                                         connection=cli_config.connection,
                                         job_class=cli_config.job_class)
                  for queue in queues]
        worker = cli_config.worker_class(
            queues, name=name, connection=cli_config.connection,
            default_worker_ttl=worker_ttl, default_result_ttl=results_ttl,
            job_monitoring_interval=job_monitoring_interval,
            job_class=cli_config.job_class, queue_class=cli_config.queue_class,
            exception_handlers=exception_handlers or None,
            disable_default_exception_handler=disable_default_exception_handler,
            log_job_description=not disable_job_desc_logging,
            serializer=serializer
        )

        # Should we configure Sentry?
        if sentry_dsn:
            sentry_opts = {
                "ca_certs": sentry_ca_certs,
                "debug": sentry_debug
            }
            from atender.contrib.sentry import register_sentry
            register_sentry(sentry_dsn, **sentry_opts)

        # if --verbose or --quiet, override --logging_level
        if verbose or quiet:
            logging_level = None

        worker.work(burst=burst, logging_level=logging_level,
                    date_format=date_format, log_format=log_format,
                    max_jobs=max_jobs, with_scheduler=with_scheduler)
    except ConnectionError as e:
        print(e)
        sys.exit(1)


@main.command()
@quo.option('--duration', help='Seconds you want the workers to be suspended.  Default is forever.', type=int)
@pass_cli_config
def suspend(cli_config, duration, **options):
    """Suspends all workers, to resume run `atender resume`"""

    if duration is not None and duration < 1:
        quo.echo("Duration must be an integer greater than 1")
        sys.exit(1)

    connection_suspend(cli_config.connection, duration)

    if duration:
        msg = """Suspending workers for {0} seconds.  No new jobs will be started during that time, but then will
        automatically resume""".format(duration)
        quo.echo(msg)
    else:
        quo.echo("Suspending workers.  No new jobs will be started.  But current jobs will be completed")


@main.command()
@pass_cli_config
def resume(cli_config, **options):
    """Resumes processing of queues, that were suspended with `atender suspend`"""
    connection_resume(cli_config.connection)
    quo.echo("Resuming workers.")
