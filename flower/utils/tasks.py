from __future__ import absolute_import

import datetime
import time

from .search import satisfies_search_terms, parse_search_terms

from celery.events.state import Task


def iter_tasks(events, limit=None, type=None, worker=None, state=None,
               sort_by=None,
               sent_start=None, sent_end=None,
               received_start=None, received_end=None,
               started_start=None, started_end=None,
               succeeded_start=None, succeeded_end=None,
               failed_start=None, failed_end=None,
               search=None):
    i = 0
    tasks = events.state.tasks_by_timestamp()
    if sort_by is not None:
        tasks = sort_tasks(tasks, sort_by)
    convert = lambda x: time.mktime(
        datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').timetuple()
    )
    search_terms = parse_search_terms(search or {})

    for uuid, task in tasks:
        if type and task.name != type:
            continue
        if worker and task.worker and task.worker.hostname != worker:
            continue
        if state and task.state != state:
            continue

        should_yield = True

        if sent_start:
            if not task.sent:
                should_yield = False
            elif task.sent < convert(sent_start):
                should_yield = False

        if sent_end:
            if not task.sent:
                should_yield = False
            elif task.sent > convert(sent_end):
                should_yield = False

        if received_start:
            if not task.received:
                should_yield = False
            elif task.received < convert(received_start):
                should_yield = False

        if received_end:
            if not task.received:
                should_yield = False
            elif task.received > convert(received_end):
                should_yield = False

        if started_start:
            if not task.started:
                should_yield = False
            elif task.started < convert(started_start):
                should_yield = False

        if started_end:
            if not task.started:
                should_yield = False
            elif task.started > convert(started_end):
                should_yield = False

        if succeeded_start:
            if not task.succeeded:
                should_yield = False
            elif task.succeeded < convert(succeeded_start):
                should_yield = False

        if succeeded_end:
            if not task.succeeded:
                should_yield = False
            elif task.succeeded > convert(succeeded_end):
                should_yield = False

        if failed_start:
            if not task.failed:
                should_yield = False
            elif task.failed < convert(failed_start):
                should_yield = False

        if failed_end:
            if not task.failed:
                should_yield = False
            elif task.failed > convert(failed_end):
                should_yield = False

        if not should_yield:
            continue

        if not satisfies_search_terms(task, search_terms):
            continue

        yield uuid, task
        i += 1
        if i == limit:
            break


sort_keys = {'name': str, 'state': str, 'received': float, 'started': float}


def sort_tasks(tasks, sort_by):
    assert sort_by.lstrip('-') in sort_keys
    reverse = False
    if sort_by.startswith('-'):
        sort_by = sort_by.lstrip('-')
        reverse = True
    for task in sorted(
            tasks,
            key=lambda x: getattr(x[1], sort_by) or sort_keys[sort_by](),
            reverse=reverse):
        yield task


def get_task_by_id(events, task_id):
    if hasattr(Task, '_fields'):  # Old version
        return events.state.tasks.get(task_id)
    else:
        _fields = Task._defaults.keys()
        task = events.state.tasks.get(task_id)
        if task is not None:
            task._fields = _fields
        return task


def as_dict(task):
    # as_dict is new in Celery 3.1.7
    if hasattr(Task, 'as_dict'):
        return task.as_dict()
    # old version
    else:
        return task.info(fields=task._defaults.keys())
