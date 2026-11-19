# JDBC Source — Driver Installation Guide

> **When to read this:** You have configured a model with `source_type: jdbc` in your
> `settings.yaml` and need to ensure the correct JDBC driver is available on your
> Databricks cluster before running the generated notebooks.

---

## How JDBC Sources Work

When a model uses `source_type: jdbc`, the generated notebooks read source data directly
from an external relational database using Spark's built-in JDBC reader:

```python
source_df = spark.read.format("jdbc").option("url", ...).option("dbtable", ...).load()
```

Spark routes the connection through a JDBC driver JAR. Some drivers are bundled with
Databricks Runtime (DBR); others must be installed as a cluster library before your
notebooks will run.

---

## Driver Availability by Database

| Database | Driver class | Bundled in DBR? | Install method |
|---|---|---|---|
| SQL Server / Azure SQL | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | ✅ Yes (DBR 7.x+) | Nothing to do |
| PostgreSQL | `org.postgresql.Driver` | ❌ No | Maven — see below |
| MySQL / MariaDB | `com.mysql.cj.jdbc.Driver` | ❌ No | Maven — see below |
| Snowflake (JDBC) | `net.snowflake.jdbc.SnowflakeDriver` | ❌ No | Maven — see below |
| Oracle | `oracle.jdbc.driver.OracleDriver` | ❌ No | JAR upload — see below |

> **Note:** Driver versions bundled with Databricks Runtime change between releases.
> Always verify against the specific DBR version your cluster uses. Check
> [Databricks Runtime release notes](https://docs.databricks.com/release-notes/runtime/index.html)
> for the authoritative list.

---

## Installing a Driver — Maven (PostgreSQL, MySQL, Snowflake)

This is the recommended approach for drivers published to Maven Central.

### Step 1 — Open the cluster Libraries tab

1. In the Databricks workspace sidebar, click **Compute**.
2. Click the cluster name.
3. Click the **Libraries** tab.
4. Click **Install new**.

### Step 2 — Choose Maven and enter the coordinates

Select **Maven** as the library source, then enter the Maven coordinate for your database:

| Database | Maven coordinate | Notes |
|---|---|---|
| PostgreSQL | `org.postgresql:postgresql:42.7.3` | Use latest stable 42.x release |
| MySQL / MariaDB | `mysql:mysql-connector-java:8.0.33` | For MariaDB, use `org.mariadb.jdbc:mariadb-java-client:3.3.3` |
| Snowflake JDBC | `net.snowflake:snowflake-jdbc:3.15.0` | Use latest stable; check [Snowflake release notes](https://docs.snowflake.com/en/release-notes/clients-drivers/jdbc) |

> **Tip:** Prefer the Snowflake Spark Connector (`net.snowflake:spark-snowflake_2.12`) over
> raw JDBC for Snowflake sources. It has better performance and supports Snowflake-specific
> features. Ask the team if you need it set up.

### Step 3 — Restart the cluster

After installing, restart the cluster for the library to take effect. Libraries installed
on a running cluster are not active until the next restart.

---

## Installing a Driver — JAR Upload (Oracle)

Oracle does not publish `ojdbc8.jar` to Maven Central due to licensing restrictions.
You must download it manually and upload it to Databricks.

### Step 1 — Download the JAR

Download `ojdbc8.jar` from the
[Oracle Technology Network](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html).
You will need an Oracle account.

Match the JAR version to your Oracle database server version:
- Oracle 19c → `ojdbc8-19.x.x.x.jar`
- Oracle 21c → `ojdbc8-21.x.x.x.jar`
- Oracle 23c → `ojdbc8-23.x.x.x.jar`

### Step 2 — Upload to DBFS or a Unity Catalog volume

```bash
# Using Databricks CLI
databricks fs cp ojdbc8.jar dbfs:/FileStore/jars/ojdbc8.jar
```

Or upload via the Databricks UI: **Data** → **DBFS** → **FileStore/jars** → **Upload**.

If your workspace uses Unity Catalog, upload to a volume instead:

```bash
databricks fs cp ojdbc8.jar /Volumes/my_catalog/my_schema/jars/ojdbc8.jar
```

### Step 3 — Attach as a cluster library

1. In the cluster **Libraries** tab, click **Install new**.
2. Select **DBFS/S3** (or **Volumes** if using UC).
3. Enter the path: `dbfs:/FileStore/jars/ojdbc8.jar`
4. Click **Install**, then restart the cluster.

---

## Verifying the Driver is Available

After installing and restarting, run this cell in a notebook on the cluster to confirm
Spark can load the driver class:

```python
# Replace with your driver class name
_driver = "org.postgresql.Driver"

try:
    spark._jvm.Class.forName(_driver)
    print(f"✓ Driver loaded: {_driver}")
except Exception as e:
    print(f"✗ Driver not found: {_driver}\n  {e}")
```

If this fails after a restart, check that:
- The Maven coordinate or JAR path is correct.
- The library status shows **Installed** (not **Pending** or **Failed**) in the Libraries tab.
- The cluster was restarted *after* the library was installed.

---

## Secret Scope Setup

JDBC credentials must never be stored in YAML or generated notebooks. The framework
reads them at runtime from a Databricks secret scope.

### Create a secret scope (if you don't have one)

```bash
databricks secrets create-scope --scope my-jdbc-scope
```

### Add the required secrets

The key names must match what you set in `settings.yaml`:

```bash
# Username
databricks secrets put --scope my-jdbc-scope --key my_model_jdbc_username

# Password
databricks secrets put --scope my-jdbc-scope --key my_model_jdbc_password

# Host (if using secret-based host resolution instead of environment profiles)
databricks secrets put --scope my-jdbc-scope --key my_model_jdbc_host
```

### Verify the scope is accessible from the cluster

```python
# Run in a notebook on the cluster
dbutils.secrets.list("my-jdbc-scope")
```

---

## Required Database Permissions

The database account used for JDBC reads needs only:

| Permission | Purpose |
|---|---|
| `SELECT` on source tables | Read data |
| `SELECT` on `INFORMATION_SCHEMA` views | Only needed by `SqlMetadataImporter` during local schema discovery — not required at Databricks runtime |

Do **not** grant `INSERT`, `UPDATE`, `DELETE`, `DDL`, or `EXECUTE` permissions. The
framework never writes back to JDBC sources.

---

## Connection Encryption

Always enable TLS/SSL for JDBC connections to production databases:

| Database | URL option to add |
|---|---|
| SQL Server | `encrypt=true;trustServerCertificate=false` |
| PostgreSQL | `?sslmode=require` or `?sslmode=verify-full` |
| MySQL | `?useSSL=true&requireSSL=true` |

Example SQL Server URL template in `settings.yaml`:

```yaml
jdbc_url_template: "jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=false;loginTimeout=30"
```

For internal dev environments where a self-signed certificate is in use, you may set
`trustServerCertificate=true` temporarily — but this must never be used in production.

---

## Troubleshooting

| Error | Likely cause | Fix |
|---|---|---|
| `ClassNotFoundException: com.microsoft.sqlserver...` | DBR version < 7.x | Upgrade cluster DBR, or install the JAR manually |
| `ClassNotFoundException: org.postgresql.Driver` | Driver not installed | Install via Maven (see above) |
| `No suitable driver found for jdbc:...` | Driver class not specified or wrong | Set `jdbc_driver_class` in `settings.yaml` |
| `Login failed for user ''` | Secret scope key missing or wrong | Check key names match `jdbc_username_key` / `jdbc_password_key` in settings |
| `Cannot open server ... requested by the login` | Wrong host or firewall | Check `jdbc_host` secret value; ensure Databricks cluster can reach the host on the JDBC port |
| `SSL connection error` | TLS options missing | Add `encrypt=true` (SQL Server) or `sslmode=require` (PostgreSQL) to URL template |
| `Connection reset` / timeout | `fetchsize` too large | Lower `jdbc_fetchsize` to 1000–5000 for wide rows |
| Partition read fails | `jdbc_lower_bound` / `jdbc_upper_bound` out of range | Adjust bounds to match actual data range in `jdbc_partition_column` |
