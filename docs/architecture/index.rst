Architecture
============

Profiles
--------

A *profile* is a unique, semantically meaningful name within the Kolibri ecosystem. It corresponds to a set of interrelated `syncable models <#syncable-models>`__ that "make sense" when synced together.

Currently there is just a single profile in the Kolibri ecosystem: ``facilitydata``.

Syncable models
---------------

A *syncable model* is a Django model which can be synced between devices using Morango. Every syncable model is associated with exactly one `profile <#profiles>`__, and exactly one `partition <#partitions>`__ within the profile.

To make a Django model syncable, inherit from ``SyncableModel``. All subclasses need to define:

- ``morango_profile`` - the name of the model's profile
- ``morango_model_name`` - a unique name within the profile
- ``calculate_source_id`` - a method that returns a unique ID of the record
- ``calculate_partition`` - a method that returns the `partition string <#partitions>`__ of the record

There are some constraints to Django models that are serialized and synced in Morango:

- models must not have self-referential foreign keys or dependency loops
- models must not use relationships based on Django `generic foreign keys <https://docs.djangoproject.com/en/1.11/ref/contrib/contenttypes/#django.contrib.contenttypes.fields.GenericForeignKey>`_
- models must not use `many-to-many <https://docs.djangoproject.com/en/1.11/topics/db/examples/many_to_many/>`_ relationships

In order to ensure that schema migrations work cleanly, always provide default values when defining model fields on syncable models.

If you create custom querysets or managers and your model inherits from ``SyncableModel``, then your custom classes should also inherit from ``SyncableModelQuerySet`` or ``SyncableModelManager`` in order to maintain syncability for these models.

In Kolibri, we currently define a base ``SyncableModel`` called ``FacilityDataSyncableModel``. Both ``FacilityDataset`` and ``AbstractFacilityDataModel`` inherit from this. In turn, other syncable Kolibri models inherit from ``AbstractFacilityDataModel`` as shown below:

.. image:: ./inheritance.png

Partitions
----------

A *partition* is a colon-delimited string that defines a subset of the `syncable models <#syncable-models>`__ in a `profile <#profiles>`__. Taken together, the partitions of a profile define mutually exclusive and complete segmented coverage of all syncable model records.

For example, a syncable model record like a content interaction log might be associated with a user in a facility. The combination of the user and the facility could be used to define a partition like ``${facility_id}:${user_id}`` that and other similar records.

Partition strings are constructed to be hierarchical. "Containment" of one partition in another can be checked with a simple ``startswith`` check. Here, the partition ``${facility_id}:${user_id}`` would be contained in the partition ``${facility_id}`` for user ``U1`` in facility ``F1`` because ``"F1:U1".startswith("F1")``. The leading part of a partition string its "prefix" designating the parent partition is a *partition prefix*.

Partition strings use colon characters to delimit levels of the hierarchy and `Python template strings <https://docs.python.org/3/library/string.html#template-strings>`__ to dynamically insert source IDs of models. Aside from this, Morango places no constraints on the structure of partition strings, and they can be constructed using other conventions and strategies.

In Kolibri, we currently have five mutually-exclusive partitions in the ``facilitydata`` profile, where the source ID of the facility is the ``dataset_id``:

- everyone has write-only access
    - partition string: ``${dataset_id}:anonymous``
    - used for content session logs
- all authenticated users have read-only access
    - partition string: ``${dataset_id}:allusers-ro``
    - used for facility metadata, classes, and other collections
- a learner has personalized read-only access
    - partition string: ``${dataset_id}:user-ro:${user_id}``
    - used for user roles and membership in classes and groups
- a learner has personalized read and write access
    - partition string: ``${dataset_id}:user-rw:${user_id}``
    - used for content interaction logs
- everything else
    - partition string: ``${dataset_id}``
    - used for quizzes and lessons

Note that all facility models share the prefix ``${dataset_id}``, which means that they are all "contained" in that top-level partition.


Filters and scopes
------------------

A *filter* is a list of `partition prefixes <#partitions>`__. They are represented as an end-line-delimited list of partition prefix strings.

A *scope* uses multiple filters to specify permission limits a device has for syncing data.

As of this writing, there are currently two scope definitions defined in Kolibri for the ``facilitydata`` profile:

- The ``full-facility`` scope provides full read and write access to all data related to a facility. This includes the facility model itself plus associated classes, lessons, users, groups, content interaction logs, and everything else related to running a typical Kolibri classroom server.
- The ``single-user`` scope provides some of the access needed by a single learner, specifically the content interaction logs. Note that this does *not* currently include all necessary data. For example, lessons that have been assigned to the user are not in this scope, and must currently be synced through another mechanism to-be-determined.

Scopes are generally hard-coded into the application using `Django fixtures <https://docs.djangoproject.com/en/3.1/howto/initial-data/#providing-data-with-fixtures>`__. This is what `Kolibri's scope definition fixture <https://github.com/learningequality/kolibri/blob/bd3fe9a04e21e446da39fed92e83c75e11ef1714/kolibri/core/auth/fixtures/scopedefinitions.json>`__ looks like:

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


Note that the ``single-user`` scope allows the user to write content-related logs and to read other facility data so that Kolibri is still able to function properly.


Certificates
------------

*Certificates* are hierarchical pairs of private/public keys that grant device-level permission to sync data within a `scope <#scopes>`__ of a `profile <#profiles>`__. Once a device has been granted access to a scope of a profile, that device can grant that scope or a subset of it to other devices by generating child certificate pairs.

Scope access and the chain of trust are established as follows:

- The private key associated with a parent certificate can be used to issue a child certificate to another device with at most the permission granted by the scope of the parent certificate
- The child certificate can be used by the new device to allow it to prove to other devices that it is authorized to access the scope
- The entire chain of signed certificates back to the origin must be exchanged during sync between devices, and the signatures and hierarchy must be verified

In the example below, *Instance A* is able to establish a future sync relationship with *Instance B* by providing admin credentials to *Instance B* and requesting a signed certificate:

.. image:: ./cert_exchange.png

In Kolibri, on the ``FacilityDataset`` model, we generate the certificate as a function of the ``calculate_source_id`` method. Note that we currently set the ID of the certificate to be the same as the ID of the facility model. This allows queries on the certificate hierarchy tree to find certificates that are associated with the facility.

.. warning::

    Certificates can not currently be revoked. This means that a stolen or hijacked device will have indefinite access to all data it has been granted. We would need to add a centralized (non-p2p) revocation system to support this.

