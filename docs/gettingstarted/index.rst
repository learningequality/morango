Getting Started
========
This document is intended to provide a high-level overview of how Morango internals work and how Kolibri interacts with it.

Syncing Process
--------

By default, Kolibri instances are listening for other Kolibri instances in the same network, while at the same time, exposing an URL to which other instances can request a connection. The connection is established via a REST call to the endpoint. For the exact request flow, see the `documentation <https://kolibri-dev.readthedocs.io/en/develop/dataflow/index.html#data-flow>`_.
After a connection request the two instances exchange certificates, which are used to authenticate the other instance. If the certificates are valid, the sync session is started. One instance is the **client** (i.e. Student) and the other is the **server** (i.
e. Teacher). The server instance uses Morango to verify that the client has the proper permissions to sync with it. Then the client and server exchange exactly the data, for which the client has the permissions to sync. The certificate verification takes place in `morango/api/permissions.py <https://github.com/learningequality/morango/blob/release-v0.6.x/morango/api/permissions.py>`_.


Syncable Models

Actions

Hooks
