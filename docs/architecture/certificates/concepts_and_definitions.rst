Concepts and Definitions
========================

Partitions
----------
Partitions are the attributes associated with a row/record specifying which segment of data they’re part of.

**For example**: a particular record can be associated to User A under Facility B. User A and Facility B will be this record’s partitions.


UUID Collisions
~~~~~~~~~~~~~~~
Each instance of a syncable model is identified by a 32-digit hex UUID as its primary key, to avoid collisions.
By default, within Morango, this UUID is calculated randomly (UUID4), but models can define their own behavior for calculating these UUIDs.
This is usually recommended, by overriding the ``calculate_source_id`` to return a string that is made up of unique model fields. We also
take into account the calculated partition and Morango model name, when calculating the unique identifier.

Scopes
------
Scope gets specified in a certificate, granting particular permissions to holders of the private key for that certificate.
Usually, these permissions are related to the data that they are allowed to sync. The permissions are defined at the read,
write, and read/write level.

Chain of Trust
--------------
In order for certificates, to sign and generate other certificates which give a subset of permissions, we must have a chain of trust
that can be followed all the way up to the root certificate:

- The private key associated with a certificate can be used to sign (issue) a new certificate with equivalent or a subset of the permissions
- This new certificate can then be used by another device to allow it to prove to other devices that it is authorized to access a particular set of data
- The entire certificate chain (the chain of signed certificates back to the origin) must be exchanged during sync, and the signatures as well as correctness (e.g. permissions always being a subset of the parent cert) must be checked all the way back up the chain to the Source of Authority
- The “Source of Authority” is the certificate that was created along with the top-level collection
