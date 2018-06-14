Here are minimal sttings to check if impersonation works well in a Kerberos secure cluster.

## How to use impersonation in the Spark SQL JDBC/ODBC server

### Preparation

    // Installs all the required packages
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

    hadoop$ sudo vi /var/kerberos/krb5kdc/kdc.conf (See ./kdc.conf)
    ...
    hadoop$ sudo kdb5_util create -r EXAMPLE.COM -s
    hadoop$ sudo vi /etc/krb5.conf (See ./krb5.conf)
    ...
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

    // Disables SELINUX
    hadoop$ sudo vi /etc/selinux/config (See ./config)
    ...

    hadoop$ wget http://ftp.jaist.ac.jp/pub/apache/hadoop/common/hadoop-2.7.6/hadoop-2.7.6.tar.gz
    hadoop$ tar xvf hadoop-2.7.6.tar.gz
    hadoop$ echo "export HADOOP_HOME=/home/hadoop/hadoop-2.7.6" >> ~/.bashrc
    hadoop$ source ~/.bashrc

    hadoop$ ssh-keygen
    hadoop$ cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    hadoop$ chmod 600 ~/.ssh/authorized_keys
    hadoop$ ssh localhost

    hadoop$ mkdir -p ~/var/lib/hdfs/{name,data}

    // Adds some environment variables in hadoop-env.sh
    hadoop$ vi $HADOOP_HOME/etc/hadoop/hadoop-env.sh
    export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.171-8.b10.el7_5.x86_64
    export JSVC_HOME=/usr/bin
    export HADOOP_SECURE_DN_USER=hadoop

    // Installs [Unlimited Strength Jurisdiction Policy Files](http://www.oracle.com/technetwork/jp/java/javase/downloads/jce-7-download-432124.html)
    hadoop$ unzip UnlimitedJCEPolicyJDK7.zip
    hadoop$ sudo cp UnlimitedJCEPolicyJDK7/*.jar $JAVA_HOME/jre/lib/security

    hadoop$ vi $HADOOP_HOME/etc/hadoop/core-site.xml (See ./core-site.xml)
    ...
    hadoop$ vi $HADOOP_HOME/etc/hadoop/hdfs-site.xml (See ./hdfs-site.xml)
    ...
    hadoop$ vi $HADOOP_HOME/etc/hadoop/yarn-site.xml (See ./yarn-site.xml)
    ...

    hadoop$ $HADOOP_HOME/bin/hadoop namenode -format
    hadoop$ $HADOOP_HOME/sbin/start-dfs.sh
    hadoop$ sudo $HADOOP_HOME/sbin/start-secure-dns.sh
    hadoop$ $HADOOP_HOME/sbin/start-yarn.sh

### Configures the Spark SQL JDBC/ODBC server

    hadoop$ git clone https://github.com/maropu/spark-sql-server.git
    hadoop$ cd spark-sql-server
    hadoop$ ./sbin/start-sql-server.sh \
      --master yarn \
      --deploy-mode client \
      --driver-memory 1g \
      --executor-memory 2g \
      --executor-cores 2 \
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

