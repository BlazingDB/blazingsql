#!/bin/bash

# HDFS client Debug mode on
#$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh
#echo "export HADOOP_OPTS=\"$HADOOP_OPTS -Djavax.net.debug=ssl -Dsun.security.krb5.debug=true\"" >> $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

# installing libraries if any - (resource urls added comma separated to the ACP system variable)
cd $HADOOP_PREFIX/share/hadoop/common ; for cp in ${ACP//,/ }; do  echo == $cp; curl -LO $cp ; done; cd -

# kerberos client
sed -i "s/EXAMPLE.COM/${KRB_REALM}/g" /root/krb5.conf
sed -i "s/example.com/${DOMAIN_REALM}/g" /root/krb5.conf
cp /root/krb5.conf /etc

# update config files
sed -i "s/HOSTNAME/${FQDN}/g" $HADOOP_PREFIX/etc/hadoop/core-site.xml
sed -i "s/EXAMPLE.COM/${KRB_REALM}/g" $HADOOP_PREFIX/etc/hadoop/core-site.xml
sed -i "s#/etc/security/keytabs#${KEYTAB_DIR}#g" $HADOOP_PREFIX/etc/hadoop/core-site.xml

sed -i "s/EXAMPLE.COM/${KRB_REALM}/g" $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml
sed -i "s/HOSTNAME/${FQDN}/g" $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml
sed -i "s#/etc/security/keytabs#${KEYTAB_DIR}#g" $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml

sed -i "s/EXAMPLE.COM/${KRB_REALM}/g" $HADOOP_PREFIX/etc/hadoop/yarn-site.xml
sed -i "s/HOSTNAME/${FQDN}/g" $HADOOP_PREFIX/etc/hadoop/yarn-site.xml
sed -i "s#/etc/security/keytabs#${KEYTAB_DIR}#g" $HADOOP_PREFIX/etc/hadoop/yarn-site.xml

sed -i "s/EXAMPLE.COM/${KRB_REALM}/g" $HADOOP_PREFIX/etc/hadoop/mapred-site.xml
sed -i "s/HOSTNAME/${FQDN}/g" $HADOOP_PREFIX/etc/hadoop/mapred-site.xml
sed -i "s#/etc/security/keytabs#${KEYTAB_DIR}#g" $HADOOP_PREFIX/etc/hadoop/mapred-site.xml

sed -i "s#/usr/local/hadoop/bin/container-executor#${NM_CONTAINER_EXECUTOR_PATH}#g" $HADOOP_PREFIX/etc/hadoop/yarn-site.xml

# create namenode kerberos principal and keytab
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "addprinc -pw ${KERBEROS_ROOT_USER_PASSWORD} root@${KRB_REALM}"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "addprinc -randkey nn/$(hostname -f)@${KRB_REALM}"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "addprinc -randkey dn/$(hostname -f)@${KRB_REALM}"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "addprinc -randkey HTTP/$(hostname -f)@${KRB_REALM}"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "addprinc -randkey jhs/$(hostname -f)@${KRB_REALM}"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "addprinc -randkey yarn/$(hostname -f)@${KRB_REALM}"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "addprinc -randkey rm/$(hostname -f)@${KRB_REALM}"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "addprinc -randkey nm/$(hostname -f)@${KRB_REALM}"

kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "xst -k nn.service.keytab nn/$(hostname -f)"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "xst -k dn.service.keytab dn/$(hostname -f)"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "xst -k spnego.service.keytab HTTP/$(hostname -f)"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "xst -k jhs.service.keytab jhs/$(hostname -f)"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "xst -k yarn.service.keytab yarn/$(hostname -f)"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "xst -k rm.service.keytab rm/$(hostname -f)"
kadmin -p ${KERBEROS_ADMIN} -w ${KERBEROS_ADMIN_PASSWORD} -q "xst -k nm.service.keytab nm/$(hostname -f)"

mkdir -p ${KEYTAB_DIR}
mv nn.service.keytab ${KEYTAB_DIR}
mv dn.service.keytab ${KEYTAB_DIR}
mv spnego.service.keytab ${KEYTAB_DIR}
mv jhs.service.keytab ${KEYTAB_DIR}
mv yarn.service.keytab ${KEYTAB_DIR}
mv rm.service.keytab ${KEYTAB_DIR}
mv nm.service.keytab ${KEYTAB_DIR}
chmod 400 ${KEYTAB_DIR}/nn.service.keytab
chmod 400 ${KEYTAB_DIR}/dn.service.keytab
chmod 400 ${KEYTAB_DIR}/spnego.service.keytab
chmod 400 ${KEYTAB_DIR}/jhs.service.keytab
chmod 400 ${KEYTAB_DIR}/yarn.service.keytab
chmod 400 ${KEYTAB_DIR}/rm.service.keytab
chmod 400 ${KEYTAB_DIR}/nm.service.keytab

$HADOOP_PREFIX/bin/hdfs namenode -format
service ssh start
$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh
$HADOOP_PREFIX/sbin/start-dfs.sh
$HADOOP_PREFIX/sbin/start-yarn.sh
$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh start historyserver

# Important! gen krb credentials for this container
echo "Auth to KRB server"
echo 'password' | kinit
sleep 4

cp /tmp/krb5cc_0 /conf_dir
chmod a+rwX /conf_dir/krb5cc_0

cp /etc/krb5.conf /conf_dir
chmod a+rwX /conf_dir/krb5.conf
echo "/etc/krb5.conf:"
cat /conf_dir/krb5.conf

echo "READY!!!"

echo "Initializing HIVE ..."

${HIVE_HOME}/bin/hive  --service hiveserver2 \
                       --hiveconf hive.server2.thrift.port=10000 \
                       --hiveconf hive.root.logger=INFO,console \
                       --hiveconf hive.server2.authentication.kerberos.principal=nn/$(hostname -f)@${KRB_REALM} \
                       --hiveconf hive.server2.authentication.kerberos.keytab=${KEYTAB_DIR}/nn.service.keytab \
                       --hiveconf hive.server2.enable.doAs=false &

sleep 10

echo "ready_krbhdfs_hadoop_secure"

if [[ $1 == "-d" ]]; then
  while true; do sleep 1000; done
fi

if [[ $1 == "-bash" ]]; then
  /bin/bash
fi
