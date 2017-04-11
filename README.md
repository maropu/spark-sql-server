[![Build Status](https://travis-ci.org/maropu/spark-sql-server.svg?branch=master)](https://travis-ci.org/maropu/spark-sql-server)

A Spark SQL server based on the PostgreSQL V3 protocol.
This is just a prototype to check feasibility for yet another SQL JDBC/ODBC server in Apache Spark
(See [SPARK-15816](https://issues.apache.org/jira/browse/SPARK-15816) for related discussions).

## Running the SQL JDBC/ODBC server

To start the JDBC/ODBC server, run the following in the root directory:

    $ ./sbin/start-sql-server.sh

This script accepts all `bin/spark-submit` command line options in Spark, plus options
for the SQL server. You may run `./sbin/start-sql-server.sh --help` for a complete list of
all available options. By default, the server listens on localhost:5432.

Now you can use a PostgreSQL `psql` command to test the SQL JDBC/ODBC server:

    $ psql -h localhost -d default

## Use PostgreSQL JDBC drivers

To connect the SQL server, you can use mature and widely-used [PostgreSQL JDBC drivers](https://jdbc.postgresql.org/).
You can get the driver, add it to a class path, and write code like;

```java
import java.sql.*;
import java.util.Properties;

public class JdbcTest {
  public static void main(String[] args) {
    try {
      // Register the PostgreSQL JDBC driver
      Class.forName("org.postgresql.Driver");

      // Connect to a 'default' database in the SPARK SQL server
      String dbName = "default";
      Properties props = new Properties();
      props.put("user", "maropu");
      Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + dbName, props);

      // Do something...
      Statement stmt = con.createStatement();
      stmt.executeQuery("CREATE TEMPORARY VIEW t AS SELECT * FROM VALUES (1, 1), (1, 2) AS t(a, b)").close();
      ResultSet rs = stmt.executeQuery("SELECT * FROM t");
      while (rs.next()){
        System.out.println("a=" + rs.getInt("a") + " b=" + rs.getInt("b"));
      }
      rs.close();
      stmt.close();
      con.close();
    } catch (Exception e) {
      // Do error handling here...
    }
  }
}
```

## PostgreSQL syntax

The SQL server supports some of PostgreSQL dialect;

    $psql -h localhost
    Type "help" for help.

    maropu=> CREATE TEMPORARY VIEW t AS SELECT id AS key, id :: TEXT AS value FROM generate_series(0, 20, 5);
    --
    (0 rows)

    maropu=> SELECT * FROM t WHERE value ~ '^1;
     key | value
    -----+-------
      10 | 10
      15 | 15
    (2 rows)

## Authentication

### SSL Encryption

To enable SSL encryption, you need to set the following configurations in `start-sql-server.sh`;

    $ ./sbin/start-sql-server.sh \
        --conf spark.sql.server.ssl.enabled=true \
        --conf spark.sql.server.ssl.keystore.path=<your keystore path> \
        --conf spark.sql.server.ssl.keystore.passwd=<your keystore password>

If you use self-signed certificates, you follow 3 steps below to create self-signed SSL certificates;

    // Create the self signed certificate and add it to a keystore file
    $ keytool -genkey -alias ssltest -keyalg RSA -keystore server.keystore -keysize 2048

    // Export this certificate from server.keystore to a certificate file
    $ keytool -export -alias ssltest -file ssltest.crt -keystore server.keystore

    // Add this certificate to the client's truststore to establish trust
    $ keytool -import -trustcacerts -alias ssltest -file ssltest.crt -keystore client.truststore

You set the generated `server.keystore` to `spark.sql.server.ssl.keystore.path` and add a new entry (`ssl`=`true`) in `Properties`
when creating a JDBC connection. Then, you pass `client.truststore` when running `JdbcTest`
(See [the PostgreSQL JDBC driver documentation](https://jdbc.postgresql.org//documentation/head/ssl-client.html) for more information);

    $ javac JdbcTest.java

    $ java -Djavax.net.ssl.trustStore=client.truststore -Djavax.net.ssl.trustStorePassword=<password> JdbcTest

### Kerberos (GSSAPI) Supports

You can use the SQL server on a Kerberos-enabled cluster only in the YARN mode because Spark supports Kerberos only in that mode.
To enable GSSAPI, you need to set the following configurations in `start-sql-server.sh`;

    $ ./sbin/start-sql-server.sh \
        --conf spark.yarn.keytab=<keytab path for server principal> \
        --conf spark.yarn.principal=<Kerberos principal server>

Then, you set a Kerberos service name (`kerberosServerName`) in `Properties` when creating a JDBC connection.
See [Connection Parameters](https://jdbc.postgresql.org/documentation/head/connect.html) for more information.

## High Availability

A high availability policy of the Spark SQL server is along with [stand-alone Master one](http://spark.apache.org/docs/latest/spark-standalone.html#high-availability);
by utilizing ZooKeeper, you can launch multiple SQL servers connected to the same ZooKeeper instance.
One will be elected “leader” and the others will remain in standby mode.
If the current leader dies, another Master will be elected and initialize `SQLContext` with given configurations.
After you have a ZooKeeper cluster set up, you can enable high availability by starting multiple servers
with the same ZooKeeper configuration (ZooKeeper URL and directory) as follows;

    $ ./sbin/start-sql-server.sh \
        --conf spark.sql.server.recoveryMode=ZOOKEEPER \
        --conf spark.deploy.zookeeper.url=<ZooKeeper URL>
        --conf spark.deploy.zookeeper.dir=<ZooKeeper directory to store recovery state>

## Bug reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/spark-sql-server/issues)
or Twitter([@maropu](http://twitter.com/#!/maropu)).

