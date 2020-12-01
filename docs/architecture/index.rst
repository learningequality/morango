Architecture
============


Syncable models
---------------

To make a Django model syncable, inherit from ``SyncableModel``.

If you create custom querysets or managers and your model inherits from ``SyncableModel``, then your custom classes should also inherit from ``SyncableModelQuerySet`` or ``SyncableModelManager`` in order to maintain syncability for these models.

Attributes that need to be defined for models that inherit from ``SyncableModel``:

- ``morango_model_name`` - allows Morango to register it as syncable under a sync profile
- ``morango_profile`` - allows Morango to register it under a sync profile

Most Django models can be serialized and synced in Morango, with some constraints. Models which you would like to make syncable must not:

- have self-referential foreign keys or dependency loops
- use relationships based on Django `generic foreign keys <https://docs.djangoproject.com/en/1.11/ref/contrib/contenttypes/#django.contrib.contenttypes.fields.GenericForeignKey>`_
- use `many-to-many <https://docs.djangoproject.com/en/1.11/topics/db/examples/many_to_many/>`_ relationships

In order to ensure that schema migrations work cleanly, always provide default values when defining model fields on syncable models.

In Kolibri, we define a base ``SyncableModel`` called ``FacilityDataSyncableModel``. Both ``FacilityDataset`` and ``AbstractFacilityDataModel`` inherit from this. In turn, the kolibri models inherit from ``AbstractFacilityDataModel`` as shown below:

.. image:: ./inheritance.png

Identifiers
-----------

Each Morango device is identified by its own unique instance ID, ``InstanceIDModel``. This ID is calculated as a function of a number of system properties, and will change when those properties change. Changes to ``InstanceIDModel`` are not fatal, but stability is preferable to avoid data bloat.


The ``DatabaseIDModel`` helps us uniquely define databases that are shared across Morango instances. If a database has been copied over or backed up, we generate a new ``DatabaseIDModel`` to be used in the calculation of the unique instance ID.

Each syncable model instance within the database is identified by a 32-digit hex UUID as its primary key. By default this unique identifier is calculated randomly, taking into account the calculated partition and Morango model name. Models can also define their own behavior by overriding ``calculate_source_id``.

Profiles
--------

You can define different profiles for sets of models that you would like to be syncable. If models do not seem in any way related or you would like to group them under a different set of syncing criteria, then you should define them under different profiles.


Partitions
----------


Partitions are the attributes associated with a row/record specifying which segment of data they’re part of.

For example, a particular record can be associated to User A under Facility B. User A and Facility B will be this record’s partitions.


Each model must have a defined partition, which must fall under one of the defined scope definitions below.


In Kolibri, models have five possible partitions defined for them:

- ``${dataset_id}`` - for exams and lessons
- ``${dataset_id}:allusers-ro`` - for facility dataset and collections
- ``${dataset_id}:user-ro:${user_id}`` - for user roles and memberships
- ``${dataset_id}:anonymous`` - for content session logs
- ``${dataset_id}:user-rw:${user_id}`` - for all logs

Since all these models have the root partition of ``${dataset_id}``, they will all be synced for a ``Certificate`` with ``ScopeDefinition`` of ``full-facility``.

When giving ``single-user`` permissions, we only want to allow the user to `write` content related logs. We need to allow the user to `read` the heirarchical structures, so that Kolibri is still able to function properly with the correct related models.



Certificate scopes
------------------

Certificates permissions for syncing, and their level of permission depends on their scope.
Scope gets specified in a certificate, granting particular permissions to holders of the private key for that certificate. Usually, these permissions are related to the data that they are allowed to sync. The permissions are defined at the read, write, and read/write level.


In order to define certain permissions when generating certificates, one must define scope definitions, which should reflect the defined partitions of your models. A *scopedefinitions.json* file should be created under a fixtures folder which can be loaded into your database. The scope definitions should define templates which specify what subset of data can be accessed when applied through a certificate. You must define templates for read, write and read/write permissions.

As of this writing, there are currently two scope definitions defined in Kolibri for the ``facilitydata`` profile:

- The ``full-facility`` scope syncs all data related to a facility. This includes the facility model itself plus associated classes, lessons, users, groups, content interaction logs, and everything else related to running a typical Kolibri classroom server.
- The ``single-user`` scope syncs data related to a user, specifically the content interaction logs. Note that this does *not* sync all data related to the user. For example, lessons that have been assigned to the user will not be automatically synced, and must be synced through another mechanism outside of Morango such as through the Kolibri API.

This is what `Kolibri's scope definition file <https://github.com/learningequality/kolibri/blob/bd3fe9a04e21e446da39fed92e83c75e11ef1714/kolibri/core/auth/fixtures/scopedefinitions.json>`__ looks like:

.. code-block:: json

    [
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
    ]


Models
------

.. automodule:: morango.models
    :noindex:
    :members: InstanceIDModel, DatabaseIDModel
