# Object Storage Connectors for Alfresco

This project provides connectors to use Alfresco with Content Store based on Software Defined Storage.

Currently 3 connectors for :

* [Red Hat Ceph Storage](https://www.redhat.com/fr/technologies/storage/ceph) using [rados](http://docs.ceph.com/docs/master/rados/api/librados-intro/)
* [OpenIO](http://openio.io/)
* [Openstack Swift](http://docs.openstack.org/developer/swift/)

[![Build Status](https://travis-ci.org/jeci-sarl/alfresco-object-storage-connectors.svg?branch=master)](https://travis-ci.org/jeci-sarl/alfresco-object-storage-connectors)


## Installation

All three connectors are available as Alfresco modules AMP, and could be install using the [default procedure](http://docs.alfresco.com/5.2/tasks/amp-install.html).

Then you have to edit `alfresco-global.properties` for supply configurations and credentials.

Target properties are different for each connectors

### Ceph Rados Configuration

``` ini
ceph.configFile=/etc/ceph/ceph.conf
ceph.id=client.admin
ceph.pool=alfresco
ceph.clusterName=ceph
```


### OpenIO Configuration

``` ini
openio.target=http://127.0.0.1:6006
openio.namespace=OPENIO
openio.account=alfresco
openio.container=alfresco
```


### OpenStack Swift Configuration

``` ini
swift.username=user:swift
swift.password=abcdefghijklmnop
swift.url=https://s3.example.org/auth/1.0
swift.tenantId=
swift.tenantName=
swift.containerName=alfresco
```


## License

This project is released under the [LGPL, Version 3](https://www.gnu.org/licenses/lgpl.html).


## Reporting Issues and Community Support

Report issues (and contribute!) [here](https://github.com/jeci-sarl/alfresco-object-storage-connectors/issues?milestone=1&state=open).

## Engineering support

[Jeci SARL](https://jeci.fr/about.html) offers annually contracted Level 3 support.

- Diagnose and correcting bugs
- Port / adapt code to any Alfresco version (Community or Enterprise)
- Port / adapt code to any SDS version

## Authors

- Jérémie Lesage - [Jeci](https://jeci.fr)
- Florent Manens - [Beezim](https://beezim.fr)


## Alternatives

* https://github.com/rmberg/alfresco-s3-adapter
* https://github.com/douglascrp/alfresco-cloud-store
* https://github.com/EisenVault/ev-alfresco-azure-adapter
