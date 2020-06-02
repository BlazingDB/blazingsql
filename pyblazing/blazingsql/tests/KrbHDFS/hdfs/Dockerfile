ARG UBUNTU_VERSION=16.04

FROM ubuntu:${UBUNTU_VERSION}
LABEL Description="blazingsql/hdfs-testing"

SHELL ["/bin/bash", "-c"]

RUN cat /etc/os-release 

RUN apt-get update
RUN apt-get install -y openssh-server openssh-client rsync vim rsyslog unzip libselinux1

RUN echo 'alias ll="ls -alFh"' >> /root/.bashrc

# passwordless ssh
# # RUN ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key
# # RUN ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key
RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

# Kerberos client
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y krb5-user libpam-krb5 libpam-ccreds auth-client-config

RUN mkdir -p /var/log/kerberos
RUN touch /var/log/kerberos/kadmind.log

# hadoop
# download/copy hadoop. Choose one of these options
ENV HADOOP_PREFIX /usr/local/hadoop
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.3/hadoop-2.7.3.tar.gz
RUN tar -xf hadoop-2.7.3.tar.gz -C /usr/local/
RUN cd /usr/local \
    && ln -s ./hadoop-2.7.3 hadoop \
    && chown root:root -R hadoop/

ENV HADOOP_COMMON_HOME $HADOOP_PREFIX
ENV HADOOP_HDFS_HOME $HADOOP_PREFIX
ENV HADOOP_MAPRED_HOME $HADOOP_PREFIX
ENV HADOOP_YARN_HOME $HADOOP_PREFIX
ENV HADOOP_CONF_DIR $HADOOP_PREFIX/etc/hadoop
ENV YARN_CONF_DIR $HADOOP_PREFIX/etc/hadoop
ENV NM_CONTAINER_EXECUTOR_PATH $HADOOP_PREFIX/bin/container-executor
ENV HADOOP_BIN_HOME $HADOOP_PREFIX/bin
ENV PATH $PATH:$HADOOP_BIN_HOME

ENV KRB_REALM EXAMPLE.COM
ENV DOMAIN_REALM example.com
ENV KERBEROS_ADMIN admin/admin
ENV KERBEROS_ADMIN_PASSWORD admin
ENV KERBEROS_ROOT_USER_PASSWORD password
ENV KEYTAB_DIR /etc/security/keytabs
ENV FQDN hadoop.com

RUN mkdir -p $HADOOP_PREFIX/input

# fetch hadoop source code to build some binaries natively
# for this, protobuf is needed

RUN apt-get install -y autoconf pkg-config make automake cmake g++-5 openjdk-8-jdk

RUN wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz
RUN tar -xf protobuf-2.5.0.tar.gz -C /tmp/
RUN cd /tmp/protobuf-2.5.0 \
    && ./configure \
    && make \
    && make install
ENV HADOOP_PROTOC_PATH /usr/local/bin/protoc

RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.3/hadoop-2.7.3-src.tar.gz
RUN tar -xf hadoop-2.7.3-src.tar.gz -C /tmp

RUN wget https://www-us.apache.org/dist/maven/maven-3/3.6.2/binaries/apache-maven-3.6.2-bin.tar.gz
RUN tar -xf apache-maven-3.6.2-bin.tar.gz -C /usr/local
RUN cd /usr/local && ln -s ./apache-maven-3.6.2/ maven
ENV PATH $PATH:/usr/local/maven/bin
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/local/lib

ADD ./setting.xml /usr/local/maven/conf

# Hive
RUN wget https://www.apache.org/dist/hive/hive-1.2.2/apache-hive-1.2.2-bin.tar.gz
RUN tar -xf apache-hive-1.2.2-bin.tar.gz
RUN mv apache-hive-1.2.2-bin /usr/lib/hive
# RUN chown -R hadoop:hadoop /usr/lib/hive
ENV HIVE_HOME /usr/lib/hive
ENV PATH $PATH:$HIVE_HOME/bin

# build native hadoop-common libs to remove warnings because of 64 bit OS

RUN rm -rf $HADOOP_PREFIX/lib/native
WORKDIR /tmp/hadoop-2.7.3-src/hadoop-common-project/hadoop-common
RUN mvn dependency:go-offline
RUN ln -sf bash /bin/sh
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN apt-get install -y --no-install-recommends libcurl4-openssl-dev libssl-dev uuid-dev zlib1g-dev
RUN bash && mvn compile -Pnative
RUN cp target/native/target/usr/local/lib/libhadoop.a $HADOOP_PREFIX/lib/native
RUN cp target/native/target/usr/local/lib/libhadoop.so.1.0.0 $HADOOP_PREFIX/lib/native

# build container-executor binary
WORKDIR /tmp/hadoop-2.7.3-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager
RUN mvn dependency:go-offline
RUN bash && mvn compile -Pnative
RUN cp target/native/target/usr/local/bin/container-executor $HADOOP_PREFIX/bin/
RUN chmod 6050 $HADOOP_PREFIX/bin/container-executor

ADD config_files/ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/config
RUN chown root:root /root/.ssh/config

# workingaround docker.io build error
RUN ls -la $HADOOP_PREFIX/etc/hadoop/*-env.sh
RUN chmod +x $HADOOP_PREFIX/etc/hadoop/*-env.sh
RUN ls -la $HADOOP_PREFIX/etc/hadoop/*-env.sh

# fix the 254 error code
RUN sed  -i "/^[^#]*UsePAM/ s/.*/#&/"  /etc/ssh/sshd_config
RUN echo "UsePAM no" >> /etc/ssh/sshd_config
RUN echo "Port 2122" >> /etc/ssh/sshd_config

RUN cp $HADOOP_PREFIX/etc/hadoop/*.xml $HADOOP_PREFIX/input

RUN mkdir -p /usr/java/
WORKDIR /usr/java/
RUN ln -fs $JAVA_HOME default
ENV JAVA $JAVA_HOME/bin/java
RUN chmod +x $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

VOLUME /tmp

ADD config_files/hadoop-env.sh $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh
ADD config_files/core-site.xml $HADOOP_PREFIX/etc/hadoop/core-site.xml
ADD config_files/hdfs-site.xml $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml
ADD config_files/mapred-site.xml $HADOOP_PREFIX/etc/hadoop/mapred-site.xml
ADD config_files/yarn-site.xml $HADOOP_PREFIX/etc/hadoop/yarn-site.xml
ADD config_files/container-executor.cfg $HADOOP_PREFIX/etc/hadoop/container-executor.cfg
RUN mkdir $HADOOP_PREFIX/nm-local-dirs \
    && mkdir $HADOOP_PREFIX/nm-log-dirs 
ADD config_files/ssl-server.xml $HADOOP_PREFIX/etc/hadoop/ssl-server.xml
ADD config_files/ssl-client.xml $HADOOP_PREFIX/etc/hadoop/ssl-client.xml
ADD config_files/keystore.jks $HADOOP_PREFIX/lib/keystore.jks

ENV BOOTSTRAP /etc/bootstrap.sh
ADD bootstrap.sh $BOOTSTRAP
RUN chown root:root $BOOTSTRAP
RUN chmod 700 $BOOTSTRAP

ADD config_files/krb5.conf /root/krb5.conf

ADD create_dir_from_info.sh /root/create_dir_from_info.sh
RUN chmod +x /root/create_dir_from_info.sh

ADD load_swap_dir.sh /root/load_swap_dir.sh
RUN chmod +x /root/load_swap_dir.sh

ADD create_hive_warehouse.sh /root/create_hive_warehouse.sh
RUN chmod +x /root/create_hive_warehouse.sh

# Pass the generated krb5.conf to the host
RUN mkdir -p /conf_dir
VOLUME /conf_dir

WORKDIR /

CMD ["/etc/bootstrap.sh", "-d"]

# Hdfs ports
EXPOSE 50010 50020 50070 50075 50090 8020 9000
# Mapred ports
EXPOSE 10020 19888
#Yarn ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088
#Other ports
EXPOSE 49707 2122
# Hive port
EXPOSE 10000