Morango within Kolibri
======================

Here we give a breakdown of how Morango is integrated within Kolibri.

.. _kolibri-scope-definitions:

Scope Definitions
-----------------

.. code-block:: json

    {
        "model": "morango.scopedefinition",
        "pk": "full-facility",
        "fields": {
        "profile": "facilitydata",
        "version": 1,
        "primary_scope_param_key": "dataset_id",
        "description": "Allows full syncing for data under the Facility with FacilityDataset ID ${dataset_id}.",
        "read_filter_template": "",
        "write_filter_template": "",
        "read_write_filter_template": "${dataset_id}"
        }
    },
    {
        "model": "morango.scopedefinition",
        "pk": "single-user",
        "fields": {
        "profile": "facilitydata",
        "version": 1,
        "primary_scope_param_key": "",
        "description": "Allows syncing data for FacilityUser ${user_id} under Facility with FacilityDataset ID ${dataset_id}.",
        "read_filter_template": "${dataset_id}:allusers-ro\n${dataset_id}:user-ro:${user_id}",
        "write_filter_template": "${dataset_id}:anonymous",
        "read_write_filter_template": "${dataset_id}:user-rw:${user_id}"
        }
    }

There are two possible scope definitions within kolibri:

1. facility dataset level definition
    - (as the description says) allows for syncing all data under a facility, that includes facility, classrooms, users, logs, etc
    - ``${dataset_id}`` is the ``FacilityDataset`` id

2. User level definition
    - allows for syncing all data under a user, that includes logs
    - ``${user_id}`` is the ``FacilityUser`` id

Inheritance Model
-----------------
We define a base ``SyncableModel`` called ``FacilityDataSyncableModel``.
Both ``FacilityDataset`` and ``AbstractFacilityDataModel`` inherit from this. In turn, the kolibri models inherit from ``AbstractFacilityDataModel``.
On this base model, we also define the required ``morango_profile`` as "``facilitydata``".

.. image:: ./img/inheritance.png

Certificate Generation
----------------------
On the ``FacilityDataset`` model, we generate the certificate as a function of the ``calculate_source_id`` method.
For consistency, we have the ID of the certificate be the ID of the model. This makes it easier to identify and find
which certificates are associated with this ``FacilityDataset``, by doing tree related queries on the certificate hierarchy.

A certificate with a scope definiton of ``full-facility``, can create certificates that give other Kolibris permissions to
sync that target facility's data to other Kolibris, and so forth.

These types of certificates can also create ``single-user`` scope defined certificates, if they only want to give syncing
permissions for a single user.
**For example**, a school giving sync permissions for a user on their tablet. The student may
take the tablet home to continue their school work, and upon returning to classroom, they can sync their completed progress to the facility server.

.. _kolibri-partition:

Partition Definition
-------------------
Kolibri models, at any point, will have five possible partitions defined for them:

- ``${dataset_id}``
    - exams and lessons
- ``${dataset_id}:allusers-ro``
    - for facility dataset and collections
- ``${dataset_id}:user-ro:${user_id}``
    - for users, roles/memberships
- ``${dataset_id}:anonymous``
    - for content session logs
- ``${dataset_id}:user-rw:${user_id}``
    - for all logs

Since all these models have the root partition of ``${dataset_id}``, they will all be synced for a ``Certificate`` with ``ScopeDefinition``
of ``full-facility``.

When giving ``single-user`` permissions, we only want to allow the user to `write` content related logs. We need to allow the user
to `read` the heirarchical structures, so that Kolibri is still able to function properly with the correct related models.

Kolibri Data Synchronization
----------------------------
In order to handle facility syncing between Kolibri instances, we have created a management command called `kolibri manage sync <https://github.com/learningequality/kolibri/blob/develop/kolibri/core/auth/management/commands/sync.py>`_.
Anytime we sync with another Kolibri, we always pull and then push the data. Both Kolibris are guaranteed to be in sync, for the target facility data, at this point.
