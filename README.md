osmtm_v1_migration
==================

Script to migrate HOT Tasking Manager v1 data to v2

Usage
-----

Activate the virtualenv::

    source /path/to/tasking/manager/env/bin/activate

Initialize the DB::

    initializedb

Then type the following command::

    python v1_migration.py path/to/OSMTM.db postgresql://username:password@localhost/osmtm
