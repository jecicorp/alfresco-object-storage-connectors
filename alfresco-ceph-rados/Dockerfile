FROM jeci/alfresco-platform:5.2.f
MAINTAINER Jeremie Lesage <jeremie.lesage@gmail.com>


ENV ceph_stable_release=jewel

RUN apt-get update \
    && apt-get install -y --no-install-recommends apt-transport-https \
    && wget -q -O- 'https://download.ceph.com/keys/release.asc' | apt-key add - \
    && echo deb https://download.ceph.com/debian-${ceph_stable_release}/ jessie main | tee /etc/apt/sources.list.d/ceph.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
      libcephfs-java \
      librados2 \
      libjna-java \
    && rm -r /var/lib/apt/lists/*

COPY target/alfresco-ceph-*.amp /root/amp/

RUN java -jar /root/alfresco-mmt.jar install /root/amp/ /usr/local/tomcat/webapps/alfresco -nobackup -directory

COPY assets/alfresco-global.properties /usr/local/tomcat/shared/classes/alfresco-global.properties
