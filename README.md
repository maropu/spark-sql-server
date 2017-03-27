[![Build Status](https://travis-ci.org/maropu/spark-sql-server.svg?branch=master)](https://travis-ci.org/maropu/spark-sql-server)

A SQL server based on the PostgreSQL V3 protocol.
This is just a prototype to check feasibility for yet another SQL JDBC/ODBC server in Apache Spark
(See [SPARK-15816](https://issues.apache.org/jira/browse/SPARK-15816) for related discussions).

### Running the SQL JDBC/ODBC server

To start the JDBC/ODBC server, run the following in the root directory:

    ./sbin/start-sql-server.sh

This script accepts all `bin/spark-submit` command line options in Spark, plus options
for the SQL server. You may run `./sbin/start-sql-server.sh --help` for a complete list of
all available options. By default, the server listens on localhost:5432.

Now you can use a PostgreSQL `psql` command to test the SQL JDBC/ODBC server:

    psql -h localhost

### Use PostgreSQL JDBC drivers

To connect the SQL server, you can use mature and widely-used [PostgreSQL JDBC drivers](https://jdbc.postgresql.org/).
You can get the driver, add it to a class path, and write code like:

```java
import java.sql.*;

public class JdbcTest {
  public static void main(String[] args) {
    try {
      // Register the PostgreSQL JDBC driver
      Class.forName("org.postgresql.Driver");

      // Connect to the SPARK SQL server
      Connection con = DriverManager.getConnection("jdbc:postgresql://localhost/spark", "maropu", "");

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

### Bug reports

If you hit some bugs and requests, please leave some comments on [Issues](https://github.com/maropu/spark-sql-server/issues)
or Twitter([@maropu](http://twitter.com/#!/maropu)).
