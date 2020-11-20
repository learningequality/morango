Integration into Application
=================================

Integrate Morango in your app!

Prerequisites
-------------
Models which you would like to make syncable should **NOT** have these properties:

- double self-referential foreign keys or dependency loops
- `GenericForeignKeys <https://docs.djangoproject.com/en/1.11/ref/contrib/contenttypes/#django.contrib.contenttypes.fields.GenericForeignKey>`_
- `ManyToMany relationships <https://docs.djangoproject.com/en/1.11/topics/db/examples/many_to_many/>`_

Profiles
--------
You can define different profiles for sets of models that you would like to be syncable. If models do not
seem in any way related or you would like to group them under a different set of syncing criteria,
then you should define them under different profiles.

Syncable Model
--------------
For any models that you would like to be syncable, they must inherit from ``SyncableModel``.
If you create custom querysets or managers, and your model inherits from ``SyncableModel``,
then your custom classes should also inherit from ``SyncableModelQuerySet`` or ``SyncableModelManager``
in order to maintain syncability for these models.

Attributes that need to be defined for models that inherit from ``SyncableModel``:

- ``morango_model_name`` - allows Morango to register it as syncable under a sync profile
- ``morango_profile`` - allows Morango to register it under a sync profile

Defining your partitions
------------------------
Each model must have a defined partition, which must fall under one of the defined scope definitions below.

For an example of partition definitions refer to :ref:`Kolibri Partitions<kolibri-partition>`

Certificate Scope Definitions
-----------------------------
In order to define certain permissions when generating certificates, one must define
scope definitions, which should reflect the defined partitions of your models.
A ``scopedefinitions.json`` should be created under a fixtures folder which can be loaded into your database.
The scope definitions should define templates which specify what subset of data can be accessed
when applied through a certificate.
You must define templates for read, write and read/write permissions.

For an example of scope definitions refer to :ref:`Kolibri Scope Definitions<kolibri-scope-definitions>`.

Migration Caveats
-----------------
Morango is designed to be resilient to changing fields between version changes, but there are still some gotchas:

- Always define default values when defining fields
   - when we are syncing data to another instance which may have a model field which we don't have, this constraint guarantees the application will not break

Orchestrating synchronization
-----------------------------
In order to facilitate synchronization between several instances, we recommend creating a django management command
which uses the Morango machinery to initiate syncing sessions.

Particularly of importance is the ``MorangoProfileController`` which can create a ``NetworkSyncConnection`` with another Morango instance.
Once the client establishes a network connection, both instances must exchange certificates so that each side can prove that they have the proper
permissions in order to push or pull the data. If the client side lacks the proper certificates, they should use the
network connection to do a ``certificate_signing_request``, where they enter admin credentials of the other instance to generate a certificate
with the valid permissions. Once both sides have the proper certificates, the client can initiate a sync session with ``create_sync_session``.
This creates a ``SyncClient`` that can handle either pushing or pulling data to/from the other Morango instance.
