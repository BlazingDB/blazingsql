#Source: https://github.com/sequenceiq/docker-kerberos

FROM centos:6.6
LABEL Description="blazingsql/kerberos-testing"

# EPEL
RUN rpm -Uvh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm

RUN yum clean all 

# kerberos
RUN yum --disablerepo="epel" install -y krb5-server krb5-libs krb5-auth-dialog krb5-workstation 

EXPOSE 88 749

ADD config.sh /config.sh

ENTRYPOINT ["/config.sh"]
