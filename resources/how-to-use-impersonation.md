Here are minimal sttings to check if impersonation works well in a Kerberos secure cluster.

## How to use impersonation in the Spark SQL JDBC/ODBC server

### Preparation

    hadoop$ sudo yum -y install git wget postgresql jsvc NetworkManager krb5-server krb5-workstation
    hadoop$ sudo yum -y install java-1.8.0-openjdk-devel.x86_64
    hadoop$ sudo yum -y groupinstall 'Development tools'

    hadoop$ java -version
    openjdk version "1.8.0_171"
    OpenJDK Runtime Environment (build 1.8.0_171-b10)
    OpenJDK 64-Bit Server VM (build 25.171-b10, mixed mode)

    hadoop$ cat /etc/redhat-release
    CentOS Linux release 7.4.1708 (Core)

    hadoop$ uname -a
    Linux hostname.example.com 3.10.0-693.5.2.el7.x86_64 #1 SMP Fri Oct 20 20:32:50 UTC 2017 x86_64 x86_64 x86_64 GNU/Linux

    hadoop$ sudo systemctl start NetworkManager && sudo systemctl enable NetworkManager
    hadoop$ nmcli general hostname
    hostname.example.com

    hadoop$ cat /etc/hostname
    hostname.example.com

### Configures Kerberos

    hadoop$ sudo systemctl start chronyd.service && sudo systemctl enable chronyd.service


    hadoop$ sudo vi /var/kerberos/krb5kdc/kdc.conf
    [kdcdefaults]
     kdc_ports = 88
     kdc_tcp_ports = 88

    [realms]
     EXAMPLE.COM = {
      master_key_type = aes256-cts
      acl_file = /var/kerberos/krb5kdc/kadm5.acl
      dict_file = /usr/share/dict/words
      admin_keytab = /var/kerberos/krb5kdc/kadm5.keytab
      supported_enctypes = aes256-cts:normal aes128-cts:normal arcfour-hmac:normal
     }


    hadoop$ sudo kdb5_util create -r EXAMPLE.COM -s


    hadoop$ sudo vi /etc/krb5.conf
    [logging]
     default = FILE:/var/log/krb5libs.log
     kdc = FILE:/var/log/krb5kdc.log
     admin_server = FILE:/var/log/kadmind.log

    [libdefaults]
     dns_lookup_realm = false
     ticket_lifetime = 24h
     renew_lifetime = 7d
     forwardable = true
     rdns = false
     default_realm = EXAMPLE.COM
     # default_ccache_name = KEYRING:persistent:%{uid}

    [realms]
     EXAMPLE.COM = {
      kdc = hostname.example.com
      admin_server = hostname.example.com
     }

    [domain_realm]
     .example.com = EXAMPLE.COM
     example.com = EXAMPLE.COM


    hadoop$ sudo systemctl start krb5kdc && sudo systemctl enable krb5kdc
    hadoop$ sudo systemctl start kadmin && sudo systemctl enable kadmin

### Creates principals & keytabs

    hadoop$ sudo kadmin.local
    kadmin.local:  addprinc -randkey hdfs/hostname.example.com@EXAMPLE.COM
    kadmin.local:  addprinc -randkey yarn/hostname.example.com@EXAMPLE.COM
    kadmin.local:  addprinc -randkey HTTP/hostname.example.com@EXAMPLE.COM
    kadmin.local:  addprinc -randkey user/hostname.example.com@EXAMPLE.COM
    kadmin.local:  listprincs
    hdfs/hostname.example.com@EXAMPLE.COM
    yarn/hostname.example.com@EXAMPLE.COM
    HTTP/hostname.example.com@EXAMPLE.COM
    user/hostname.example.com@EXAMPLE.COM
    ...
    kadmin.local:  ktadd -norandkey -k hdfs.keytab hdfs/hostname.example.com@EXAMPLE.COM HTTP/hostname.example.com@EXAMPLE.COM user/hostname.example.com@EXAMPLE.COM
    kadmin.local:  ktadd -norandkey -k yarn.keytab yarn/hostname.example.com@EXAMPLE.COM HTTP/hostname.example.com@EXAMPLE.COM
    kadmin.local:  ktadd -norandkey -k user.keytab user/hostname.example.com@EXAMPLE.COM
    kadmin.local:  quit


    hadoop$ sudo chown hadoop:hadoop hdfs.keytab yarn.keytab
    hadoop$ sudo chown user:user user.keytab
    hadoop$ mv hdfs.keytab yarn.keytab $HADOOP_HOME/etc/hadoop/
    hadoop$ mv user.keytab /home/user/

### Configures Hadoop-2.7.6

    hadoop$ sudo vi /etc/selinux/config
    SELINUX=disabled


    hadoop$ wget http://ftp.jaist.ac.jp/pub/apache/hadoop/common/hadoop-2.7.6/hadoop-2.7.6.tar.gz
    hadoop$ tar xvf hadoop-2.7.6.tar.gz
    hadoop$ echo "export HADOOP_HOME=/home/hadoop/hadoop-2.7.6" >> ~/.bashrc
    hadoop$ source ~/.bashrc

    hadoop$ ssh-keygen
    hadoop$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    hadoop$ chmod 600 ~/.ssh/authorized_keys
    hadoop$ ssh localhost

    hadoop$ mkdir -p ~/var/lib/hdfs/{name,data}


    hadoop$ vi $HADOOP_HOME/etc/hadoop/hadoop-env.sh
    export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.171-8.b10.el7_5.x86_64
    export JSVC_HOME=/usr/bin
    export HADOOP_SECURE_DN_USER=hadoop


    // Installs (Unlimited Strength Jurisdiction Policy Files)[http://www.oracle.com/technetwork/jp/java/javase/downloads/jce-7-download-432124.html]
    hadoop$ unzip UnlimitedJCEPolicyJDK7.zip
    hadoop$ sudo cp UnlimitedJCEPolicyJDK7/*.jar $JAVA_HOME/jre/lib/security


    hadoop$ vi $HADOOP_HOME/etc/hadoop/core-site.xml
```
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>

  <!-- For Kerberos -->
  <property>
    <name>hadoop.security.authentication</name>
    <value>kerberos</value>
  </property>
  <property>
    <name>hadoop.security.authorization</name>
    <value>true</value>
  </property>
</configuration>
```

    hadoop$ vi $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```
<configuration>
  <property>
    <name>dfs.name.dir</name>
    <value>/home/hadoop/var/lib/hdfs/name</value>
  </property>
  <property>
    <name>dfs.data.dir</name>
    <value>/home/hadoop/var/lib/hdfs/data</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>

  <!-- For Kerberos -->
  <!-- General HDFS security config -->
  <property>
    <name>dfs.block.access.token.enable</name>
    <value>true</value>
  </property>

  <!-- NameNode security config -->
  <property>
    <name>dfs.namenode.keytab.file</name>
    <value>/home/hadoop/hadoop-2.7.6/etc/hadoop/hdfs.keytab</value>
  </property>
  <property>
    <name>dfs.namenode.kerberos.principal</name>
    <value>hdfs/hostname.example.com@EXAMPLE.COM</value>
  </property>
  <property>
    <name>dfs.namenode.kerberos.internal.spnego.principal</name>
    <value>HTTP/hostname.example.com@EXAMPLE.COM</value>
  </property>

  <!-- Secondary NameNode security config -->
  <property>
    <name>dfs.secondary.namenode.keytab.file</name>
    <value>/home/hadoop/hadoop-2.7.6/etc/hadoop/hdfs.keytab</value>
  </property>
  <property>
    <name>dfs.secondary.namenode.kerberos.principal</name>
    <value>hdfs/hostname.example.com@EXAMPLE.COM</value>
  </property>
  <property>
    <name>dfs.secondary.namenode.kerberos.internal.spnego.principal</name>
    <value>HTTP/hostname.example.com@EXAMPLE.COM</value>
  </property>

  <!-- DataNode security config -->
  <property>
    <name>dfs.datanode.data.dir.perm</name>
    <value>700</value>
  </property>
  <property>
    <name>dfs.datanode.address</name>
    <value>0.0.0.0:1004</value>
  </property>
  <property>
    <name>dfs.datanode.http.address</name>
    <value>0.0.0.0:1006</value>
  </property>
  <property>
    <name>dfs.datanode.keytab.file</name>
    <value>/home/hadoop/hadoop-2.7.6/etc/hadoop/hdfs.keytab</value>
  </property>
  <property>
    <name>dfs.datanode.kerberos.principal</name>
    <value>hdfs/hostname.example.com@EXAMPLE.COM</value>
  </property>

  <!-- Web Authentication config -->
  <property>
    <name>dfs.web.authentication.kerberos.principal</name>
    <value>HTTP/hostname.example.com@EXAMPLE.COM</value>
  </property>

  <!-- Impersonation config -->
  <property>
    <name>hadoop.proxyuser.hdfs.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.hdfs.groups</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.hdfs.users</name>
    <value>*</value>
  </property>
</configuration>
```

    hadoop$ vi $HADOOP_HOME/etc/hadoop/yarn-site.xml
```
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>hostname.example.com</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>8192</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>8192</value>
  </property>
  <property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>

  <!-- For Kerberos -->
  <!-- ResourceManager security configs -->
  <property>
    <name>yarn.resourcemanager.keytab</name>
    <value>/home/hadoop/hadoop-2.7.6/etc/hadoop/yarn.keytab</value>
  </property>
  <property>
    <name>yarn.resourcemanager.principal</name>
    <value>yarn/hostname.example.com@EXAMPLE.COM</value>
  </property>

  <!-- NodeManager security configs -->
  <property>
    <name>yarn.nodemanager.keytab</name>
    <value>/home/hadoop/hadoop-2.7.6/etc/hadoop/yarn.keytab</value>
  </property>
  <property>
    <name>yarn.nodemanager.principal</name>
    <value>yarn/hostname.example.com@EXAMPLE.COM</value>
  </property>
</configuration>
```

    hadoop$ $HADOOP_HOME/bin/hadoop namenode -format
    hadoop$ $HADOOP_HOME/sbin/start-dfs.sh
    hadoop$ sudo $HADOOP_HOME/sbin/start-secure-dns.sh
    hadoop$ $HADOOP_HOME/sbin/start-yarn.sh

### Configures the Spark SQL JDBC/ODBC server

    hadoop$ git clone https://github.com/maropu/spark-sql-server.git
    hadoop$ cd spark-sql-server

    hadoop$ ./sbin/start-sql-server.sh \
      --conf spark.sql.server.psql.enabled=true \
      --conf spark.sql.server.executionMode=multi-context \
      --conf spark.yarn.impersonation.enabled=[false|true] \
      --conf spark.yarn.principal=hdfs/hostname.example.com@EXAMPLE.COM \
      --conf spark.yarn.keytab=/home/hadoop/hadoop-2.7.6/etc/hadoop/hdfs.keytab

### Interactive shell with impersonation disabled

    user$ kinit user/hostname.example.com@EXAMPLE.COM -kt /home/user/user.keytab
    user$ psql \
      -U user/hostname.example.com@EXAMPLE.COM \
      -h hostname.example.com \
      "dbname=default sslmode=disable krbsrvname=user"

    default=> CREATE EXTERNAL TABLE test(c1 int, c2 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/test';
    default=> INSERT INTO TABLE test VALUES (1, 'a'), (2, 'b');

    hadoop$ $HADOOP_HOME/bin/hdfs dfs -ls /tmp
    Found 2 items
    drwx-wx-wx   - hdfs supergroup          0 2018-06-13 14:31 /tmp/test
    ...

### Interactive shell with impersonation enabled

    user$ kinit user/hostname.example.com@EXAMPLE.COM -kt /home/user/user.keytab
    user$ psql \
      -U user/hostname.example.com@EXAMPLE.COM \
      -h hostname.example.com \
      "dbname=default sslmode=disable krbsrvname=user"

    default=> CREATE EXTERNAL TABLE test(c1 int, c2 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/test';
    default=> INSERT INTO TABLE test VALUES (1, 'a'), (2, 'b');

    hadoop$ $HADOOP_HOME/bin/hdfs dfs -ls /tmp
    Found 2 items
    drwx-wx-wx   - user supergroup          0 2018-06-13 14:33 /tmp/test
    ...

