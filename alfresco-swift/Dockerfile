FROM jeci/alfresco-platform:5.2.f
MAINTAINER Jeremie Lesage <jeremie.lesage@gmail.com>

COPY target/alfresco-swift-*.amp /root/amp/

RUN java -jar /root/alfresco-mmt.jar install /root/amp/ /usr/local/tomcat/webapps/alfresco -nobackup -directory -force \
	&& rm -r /usr/local/tomcat/webapps/alfresco/WEB-INF/classes/alfresco/module/backup

COPY assets/alfresco-global.properties /usr/local/tomcat/shared/classes/alfresco-global.properties
