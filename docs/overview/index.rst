Overview
========

Morango is a pure-Python database replication engine for Django that supports peer-to-peer syncing of data. It is structured as a Django app that can be included in projects to make specific application models syncable.

Developed in support of the `Kolibri <https://github.com/learningequality/kolibri/>`__ product ecosystem, Morango includes some important features including:

- A certificate-based authentication system to protect privacy and integrity of data
- A change-tracking system to support calculation of differences between databases across low-bandwidth connections
- A set of constructs to support data partitioning


Motivating user story
---------------------

Imagine a scenario where we have four instances of Kolibri:

- *Home* is a tablet used at home by a learner with no internet access
- *Facility* is a laptop at a nearby school, also with no internet access
- *City* is a laptop in a nearby city
- *Cloud* is a server online in the cloud

On *Facility*, a coach assigns resources to a learner's user account. The learner brings *Home* to the school and syncs with *Facility*, getting only their assignments and no private data about other learners.

The learner uses *Home* for a week, engaging with the assigned resources. They (and other learners) bring their tablets back to school and sync again with *Facility*. The coach can now see the recent user engagement data for their class.

An admin user wants to get the recent user engagement data from the *Facility* device onto their *City* device. In order to achieve this, the admin may bring *City* to the remote area. Once *City* arrives in the remote area, *Facility* and *City* can sync over the school's local network.

Finally, the admin brings *City* back to the city and syncs with *Cloud* over the internet. At this point, *Facility*, *City*, and *Cloud* all have the same data. Now, imagine a second admin in another city syncs their own laptop (*City 2*) with *Cloud*. Now they, too would have the recent data from *Facility*.


Objectives
----------

- **User experience:** Streamline the end-user syncing process as much as possible
- **Privacy:** Only sync data to devices and users authorized to access that data
- **Flexibility:** Afford the ability to sync only a subset of the data
- **Efficiency:** Minimize storage, bandwidth, and processing power
- **Integrity:** Protect data from accidental and malicious data corruption
- **Peer-to-peer:** Devices should be able to communicate without a central server
- **Eventual consistency:**  Eventually all devices will converge to the same data
