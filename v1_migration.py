#!env/bin/python
# -*- coding: utf-8 -*-

import sys

if len(sys.argv) < 3:
    print("Usage: %s path/to/OSMTM.db postgresql://username:password@localhost/osmtm" % sys.argv[0])
    sys.exit(2)

path_to_sqlite_db = sys.argv[1]
db_url = sys.argv[2]

import transaction
import urllib
import json
import datetime

from sqlalchemy import (
    func,
    create_engine,
    and_,
)
from sqlalchemy.orm import sessionmaker

from sqlalchemy.schema import (
    MetaData,
)

from osmtm.utils import (
    TileBuilder,
    max
)

from osmtm.models import (
    DBSession as session_v2,
    Area,
    Project,
    Task,
    TaskState,
    TaskLock,
    TaskComment,
    License,
    User,
)

from sqlalchemy_i18n.manager import translation_manager

import shapely

from geoalchemy2 import (
    shape,
)
from geoalchemy2.functions import (
    ST_Transform,
)


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'


def header(msg):
    print
    print bcolors.HEADER + "# " + msg + bcolors.ENDC


def success(msg):
    print bcolors.OKGREEN + msg + bcolors.ENDC


def failure(msg):
    print bcolors.FAIL + msg + bcolors.ENDC

translation_manager.options.update({
    'locales': ['en'],
    'get_locale_fallback': True
})


''' V1 '''
metadata_v1 = MetaData()
engine_v1 = create_engine('sqlite:///%s' % path_to_sqlite_db)
session_v1 = sessionmaker(bind=engine_v1)()
metadata_v1.reflect(bind=engine_v1)

''' v2 '''
engine_v2 = create_engine(db_url)
session_v2.configure(bind=engine_v2)

jobs = metadata_v1.tables['jobs']
tiles = metadata_v1.tables['tiles']
tiles_history = metadata_v1.tables['tiles_history']
licenses = metadata_v1.tables['licenses']
users_table = metadata_v1.tables['users']

header("Cleaning up db")
with transaction.manager:
    # FIXME we may need to empty the V2 db first
    session_v2.query(TaskLock).delete()
    session_v2.query(TaskState).delete()
    session_v2.query(TaskComment).delete()
    session_v2.query(Task).delete()
    session_v2.query(Project).delete()
    session_v2.query(Area).delete()
    session_v2.query(License).delete()
    session_v2.query(User).delete()
    session_v2.flush()

header("Retrieving users ids")
f = open('users.list', 'r+')
users = {}
for line in f:
    user = line.split(';')
    users[user[0]] = user[1]

for k, u in enumerate(session_v1.query(users_table).all()):
    username = u.username.encode('utf-8')
    if username not in users:
        print "%s - %s" % (k, u.username)
        url = "http://whosthat.osmz.ru/whosthat.php?action=names&q=%s" % \
            username
        response = urllib.urlopen(url)
        data = json.load(response)
        f.write('%s;' % username)

        found = False
        for user in data:
            if u.username in user["names"]:
                found = True
                f.write('%s' % user['id'])
                users[username] = user['id']

        if not found:
            f.write('%s' % -1)
            users[username] = -1

        f.write(';\n')
f.close()

print "%d users found" % len(users)

header("Importing users in v2")
# inverting users mapping, key is now id
users_by_id = {v: k for k, v in users.items()}

users_count = 0
with transaction.manager:
    for id in users_by_id:
        username = users_by_id[id]
        if id != -1:
            user = User(id, username.decode('utf-8'))
            session_v2.add(user)
            users_count += 1
    session_v2.flush()

success("%d users - successfully imported" % (users_count))

header("Importing licenses")
with transaction.manager:
    query = session_v1.query(licenses).all()
    for l in query:
        license = License()
        license.id = l.id
        license.name = l.name
        license.description = l.description
        license.plain_text = l.plain_text
        session_v2.add(license)
        success("License %s - \"%s\" successfully imported" % (l.id, l.name))
    session_v2.flush()


header("Importing jobs and tasks")
for job in session_v1.query(jobs):

    with transaction.manager:

        geometry = shapely.wkt.loads(job.geometry)
        geometry = ST_Transform(shape.from_shape(geometry, 3857), 4326)
        area = Area(geometry)
        session_v2.add(area)

        project = Project(job.title)
        project.id = job.id
        project.area = area
        project.zoom = job.zoom
        project.last_update = job.last_update
        project.description = job.description
        project.short_description = job.short_description
        project.private = job.is_private
        project.instructions = job.workflow
        project.per_task_instructions = job.task_extra
        project.imagery = job.imagery if job.imagery != 'None' else None
        project.license_id = job.license_id
        project.author_id = users[job.author.encode('utf-8')] \
            if job.author else None
        project.status = job.status
        project.josm_preset = job.josm_preset

        if job.featured:
            project.priority = 1

        session_v2.add(project)
        session_v2.flush()
        project_id = project.id

        first_history = session_v1.query(tiles_history).filter(
            and_(
                tiles_history.c.job_id == job.id,
                'tiles_history."update" IS NOT NULL',
            )
        ).order_by('tiles_history."update"').limit(1)

        try:
            project.created = first_history.one().update
            session_v2.add(project)
        except:
            pass

        for tile in session_v1.query(tiles).filter(tiles.c.job_id == job.id):
            step = max / (2 ** (tile.zoom - 1))
            tb = TileBuilder(step)
            geometry = tb.create_square(tile.x, tile.y)
            geometry = ST_Transform(shape.from_shape(geometry, 3857), 4326)

            task = Task(tile.x, tile.y, tile.zoom, geometry)
            task.project_id = project_id
            session_v2.add(task)
            session_v2.flush()

            d = TaskState.__table__.delete(
                and_(
                    TaskState.project_id == job.id,
                    TaskState.task_id == task.id
                )
            )
            session_v2.execute(d)

            d = TaskLock.__table__.delete(
                and_(
                    TaskLock.project_id == job.id,
                    TaskLock.task_id == task.id
                )
            )
            session_v2.execute(d)

            # initial state
            task_state = TaskState()
            task_state.date = datetime.datetime(2010, 01, 01)
            task_state.task_id = task.id
            task_state.project_id = project_id
            session_v2.add(task_state)

            # initial state
            task_lock = TaskLock()
            task_lock.date = datetime.datetime(2010, 01, 01)
            task_lock.task_id = task.id
            task_lock.project_id = project_id
            session_v2.add(task_lock)

            prev_checkin = None
            for h in session_v1.query(tiles_history) \
                .filter(
                    tiles_history.c.job_id == job.id,
                    tiles_history.c.x == tile.x,
                    tiles_history.c.y == tile.y,
                    tiles_history.c.zoom == tile.zoom) \
                .order_by('tiles_history."update"'):  # noqa

                # we don't care about locks (checkout)
                if h.change:
                    state = None
                    if h.checkin == 1:
                        state = TaskState.state_done
                    elif h.checkin == 2:
                        state = TaskState.state_validated
                    elif prev_checkin:
                        state = TaskState.state_invalidated
                    task_state = TaskState(state=state)
                    task_state.date = h.update
                    task_state.task_id = task.id
                    task_state.project_id = project_id
                    task_state.user_id = users[h.username.encode('utf-8')] \
                        if h.username else None
                    session_v2.add(task_state)

                    prev_checkin = h.checkin

                if h.comment:
                    task_comment = TaskComment(h.comment, None)
                    task_comment.author_id = users[h.username.encode('utf-8')] \
                        if h.username else None
                    task_comment.task_id = task.id
                    task_comment.project_id = project_id
                    task_comment.date = h.update
                    session_v2.add(task_comment)

    success("Job %s - \"%s\" successfully imported" % (job.id, job.title))

with transaction.manager:
    header("Updating projects done stats")
    for project in session_v2.query(Project).all():
        project.done = project.get_done()
        project.validated = project.get_validated()
    session_v2.flush()


# FIXME reset the project id sequence
max_project_id = session_v2.query(func.max(Project.id)).scalar()
session_v2.execute('ALTER SEQUENCE project_id_seq RESTART %d' %
                   (max_project_id + 1))
