---
title: "CREATE 语句"
weight: 4
type: docs
aliases:
  - /zh/dev/table/sql/create.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# CREATE 语句



CREATE 语句用于向当前或指定的 [Catalog]({{< ref "docs/dev/table/catalogs" >}}) 中注册表、视图或函数。注册后的表、视图和函数可以在 SQL 查询中使用。

目前 Flink SQL 支持下列 CREATE 语句：

- CREATE TABLE
- [CREATE OR] REPLACE TABLE
- CREATE CATALOG
- CREATE DATABASE
- CREATE VIEW
- CREATE FUNCTION
- CREATE MODEL

## 执行 CREATE 语句

{{< tabs "execute" >}}
{{< tab "Java" >}}

可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行 CREATE 语句。 若 CREATE 操作执行成功，`executeSql()` 方法返回 'OK'，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 CREATE 语句。
{{< /tab >}}
{{< tab "Scala" >}}

可以使用 `TableEnvironment` 中的 `executeSql()` 方法执行 CREATE 语句。 若 CREATE 操作执行成功，`executeSql()` 方法返回 'OK'，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 CREATE 语句。

{{< /tab >}}
{{< tab "Python" >}}

可以使用 `TableEnvironment` 中的 `execute_sql()` 方法执行 CREATE 语句。 若 CREATE 操作执行成功，`execute_sql()` 方法返回 'OK'，否则会抛出异常。

以下的例子展示了如何在 `TableEnvironment` 中执行一个 CREATE 语句。

{{< /tab >}}
{{< tab "SQL CLI" >}}

可以在 [SQL CLI]({{< ref "docs/dev/table/sqlClient" >}}) 中执行 CREATE 语句。

以下的例子展示了如何在 SQL CLI 中执行一个 CREATE 语句。

{{< /tab >}}
{{< /tabs >}}

{{< tabs "43d7f18a-0f7f-4b9c-8367-d731238d4d41" >}}
{{< tab "Java" >}}
```java
EnvironmentSettings settings = EnvironmentSettings.newInstance()...
TableEnvironment tableEnv = TableEnvironment.create(settings);

// 对已注册的表进行 SQL 查询
// 注册名为 “Orders” 的表
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");
// 在表上执行 SQL 查询，并把得到的结果作为一个新的表
Table result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

// 对已注册的表进行 INSERT 操作
// 注册 TableSink
tableEnv.executeSql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)");
// 在表上执行 INSERT 语句并向 TableSink 发出结果
tableEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv = TableEnvironment.create(...)

// 对已注册的表进行 SQL 查询
// 注册名为 “Orders” 的表
tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)")
// 在表上执行 SQL 查询，并把得到的结果作为一个新的表
val result = tableEnv.sqlQuery(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")

// 对已注册的表进行 INSERT 操作
// 注册 TableSink
tableEnv.executeSql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH ('connector.path'='/path/to/file' ...)")
// 在表上执行 INSERT 语句并向 TableSink 发出结果
tableEnv.executeSql(
  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table_env = TableEnvironment.create(...)

# 对已经注册的表进行 SQL 查询
# 注册名为 “Orders” 的表
table_env.execute_sql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...)");
# 在表上执行 SQL 查询，并把得到的结果作为一个新的表
result = table_env.sql_query(
  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");

# 对已注册的表进行 INSERT 操作
# 注册 TableSink
table_env.execute_sql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH (...)")
# 在表上执行 INSERT 语句并向 TableSink 发出结果
table_env \
    .execute_sql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'")
```
{{< /tab >}}
{{< tab "SQL CLI" >}}
```sql
Flink SQL> CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> CREATE TABLE RubberOrders (product STRING, amount INT) WITH (...);
[INFO] Table has been created.

Flink SQL> INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%';
[INFO] Submitting SQL update statement to the cluster...
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

##  CREATE TABLE

```text
CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
  (
    { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
    [ <table_constraint> ][ , ...n]
  )
  [COMMENT table_comment]
  [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
  [ <distribution> ]
  WITH (key1=val1, key2=val2, ...)
  [ LIKE source_table [( <like_options> )] | AS select_query ]
   
<physical_column_definition>:
  column_name column_type [ <column_constraint> ] [COMMENT column_comment]
  
<column_constraint>:
  [CONSTRAINT constraint_name] PRIMARY KEY NOT ENFORCED

<table_constraint>:
  [CONSTRAINT constraint_name] PRIMARY KEY (column_name, ...) NOT ENFORCED

<metadata_column_definition>:
  column_name column_type METADATA [ FROM metadata_key ] [ VIRTUAL ]

<computed_column_definition>:
  column_name AS computed_column_expression [COMMENT column_comment]

<watermark_definition>:
  WATERMARK FOR rowtime_column_name AS watermark_strategy_expression

<source_table>:
  [catalog_name.][db_name.]table_name

<like_options>:
{
   { INCLUDING | EXCLUDING } { ALL | CONSTRAINTS | DISTRIBUTION | PARTITIONS }
 | { INCLUDING | EXCLUDING | OVERWRITING } { GENERATED | OPTIONS | WATERMARKS } 
}[, ...]

<distribution>:
{
    DISTRIBUTION BY [ { HASH | RANGE } ] (bucket_column_name1, bucket_column_name2, ...) [INTO n BUCKETS]
  | DISTRIBUTION INTO n BUCKETS
}

```

根据指定的表名创建一个表，如果同名表已经在 catalog 中存在了，则无法注册。

### Columns

**Physical / Regular Columns**

Physical columns are regular columns known from databases. They define the names, the types, and the
order of fields in the physical data. Thus, physical columns represent the payload that is read from
and written to an external system. Connectors and formats use these columns (in the defined order)
to configure themselves. Other kinds of columns can be declared between physical columns but will not
influence the final physical schema.

The following statement creates a table with only regular columns:

```sql
CREATE TABLE MyTable (
  `user_id` BIGINT,
  `name` STRING
) WITH (
  ...
);
```

**Metadata Columns**

Metadata columns are an extension to the SQL standard and allow to access connector and/or format specific
fields for every row of a table. A metadata column is indicated by the `METADATA` keyword. For example,
a metadata column can be be used to read and write the timestamp from and to Kafka records for time-based
operations. The [connector and format documentation]({{< ref "docs/connectors/table/overview" >}}) lists the
available metadata fields for every component. However, declaring a metadata column in a table's schema
is optional.

The following statement creates a table with an additional metadata column that references the metadata field `timestamp`:

```sql
CREATE TABLE MyTable (
  `user_id` BIGINT,
  `name` STRING,
  `record_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'    -- reads and writes a Kafka record's timestamp
) WITH (
  'connector' = 'kafka'
  ...
);
```

Every metadata field is identified by a string-based key and has a documented data type. For example,
the Kafka connector exposes a metadata field with key `timestamp` and data type `TIMESTAMP_LTZ(3)`
that can be used for both reading and writing records.

In the example above, the metadata column `record_time` becomes part of the table's schema and can be
transformed and stored like a regular column:

```sql
INSERT INTO MyTable SELECT user_id, name, record_time + INTERVAL '1' SECOND FROM MyTable;
```

For convenience, the `FROM` clause can be omitted if the column name should be used as the identifying metadata key:

```sql
CREATE TABLE MyTable (
  `user_id` BIGINT,
  `name` STRING,
  `timestamp` TIMESTAMP_LTZ(3) METADATA    -- use column name as metadata key
) WITH (
  'connector' = 'kafka'
  ...
);
```

For convenience, the runtime will perform an explicit cast if the data type of the column differs from
the data type of the metadata field. Of course, this requires that the two data types are compatible.

```sql
CREATE TABLE MyTable (
  `user_id` BIGINT,
  `name` STRING,
  `timestamp` BIGINT METADATA    -- cast the timestamp as BIGINT
) WITH (
  'connector' = 'kafka'
  ...
);
```

By default, the planner assumes that a metadata column can be used for both reading and writing. However,
in many cases an external system provides more read-only metadata fields than writable fields. Therefore,
it is possible to exclude metadata columns from persisting using the `VIRTUAL` keyword.

```sql
CREATE TABLE MyTable (
  `timestamp` BIGINT METADATA,       -- part of the query-to-sink schema
  `offset` BIGINT METADATA VIRTUAL,  -- not part of the query-to-sink schema
  `user_id` BIGINT,
  `name` STRING,
) WITH (
  'connector' = 'kafka'
  ...
);
```

In the example above, the `offset` is a read-only metadata column and excluded from the query-to-sink
schema. Thus, source-to-query schema (for `SELECT`) and query-to-sink (for `INSERT INTO`) schema differ:

```text
source-to-query schema:
MyTable(`timestamp` BIGINT, `offset` BIGINT, `user_id` BIGINT, `name` STRING)

query-to-sink schema:
MyTable(`timestamp` BIGINT, `user_id` BIGINT, `name` STRING)
```

**Computed Columns**

Computed columns are virtual columns that are generated using the syntax `column_name AS computed_column_expression`.

A computed column evaluates an expression that can reference other columns declared in the same table.
Both physical columns and metadata columns can be accessed. The column itself is not physically stored
within the table. The column's data type is derived automatically from the given expression and does
not have to be declared manually.

The planner will transform computed columns into a regular projection after the source. For optimization
or [watermark strategy push down]({{< ref "docs/dev/table/sourcesSinks" >}}), the evaluation might be spread
across operators, performed multiple times, or skipped if not needed for the given query.

For example, a computed column could be defined as:
```sql
CREATE TABLE MyTable (
  `user_id` BIGINT,
  `price` DOUBLE,
  `quantity` DOUBLE,
  `cost` AS price * quantity  -- evaluate expression and supply the result to queries
) WITH (
  'connector' = 'kafka'
  ...
);
```

The expression may contain any combination of columns, constants, or functions. The expression cannot
contain a subquery.

Computed columns are commonly used in Flink for defining [time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}})
in `CREATE TABLE` statements.
- A [processing time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time)
can be defined easily via `proc AS PROCTIME()` using the system's `PROCTIME()` function.
- An [event time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}#event-time) timestamp
can be pre-processed before the `WATERMARK` declaration. For example, the computed column can be used
if the original field is not `TIMESTAMP(3)` type or is nested in a JSON string.

Similar to virtual metadata columns, computed columns are excluded from persisting. Therefore, a computed
column cannot be the target of an `INSERT INTO` statement. Thus, source-to-query schema (for `SELECT`)
and query-to-sink (for `INSERT INTO`) schema differ:

```text
source-to-query schema:
MyTable(`user_id` BIGINT, `price` DOUBLE, `quantity` DOUBLE, `cost` DOUBLE)

query-to-sink schema:
MyTable(`user_id` BIGINT, `price` DOUBLE, `quantity` DOUBLE)
```

### `WATERMARK`

`WATERMARK` 定义了表的事件时间属性，其形式为 `WATERMARK FOR rowtime_column_name  AS watermark_strategy_expression` 。

`rowtime_column_name` 把一个现有的列定义为一个为表标记事件时间的属性。该列的类型必须为 `TIMESTAMP(3)`，且是 schema 中的顶层列，它也可以是一个计算列。

`watermark_strategy_expression` 定义了 watermark 的生成策略。它允许使用包括计算列在内的任意非查询表达式来计算 watermark ；表达式的返回类型必须是 `TIMESTAMP(3)`，表示了从 Epoch 以来的经过的时间。
返回的 watermark 只有当其不为空且其值大于之前发出的本地 watermark 时才会被发出（以保证 watermark 递增）。每条记录的 watermark 生成表达式计算都会由框架完成。
框架会定期发出所生成的最大的 watermark ，如果当前 watermark 仍然与前一个 watermark 相同、为空、或返回的 watermark 的值小于最后一个发出的 watermark ，则新的 watermark 不会被发出。
Watermark 根据 [`pipeline.auto-watermark-interval`]({{< ref "docs/deployment/config" >}}#pipeline-auto-watermark-interval) 中所配置的间隔发出。
若 watermark 的间隔是 `0ms` ，那么每条记录都会产生一个 watermark，且 watermark 会在不为空并大于上一个发出的 watermark 时发出。

使用事件时间语义时，表必须包含事件时间属性和 watermark 策略。

Flink 提供了几种常用的 watermark 策略。

- 严格递增时间戳： `WATERMARK FOR rowtime_column AS rowtime_column`。

  发出到目前为止已观察到的最大时间戳的 watermark ，时间戳大于最大时间戳的行被认为没有迟到。

- 递增时间戳： `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '0.001' SECOND`。

  发出到目前为止已观察到的最大时间戳减 1 的 watermark ，时间戳大于或等于最大时间戳的行被认为没有迟到。

- 有界乱序时间戳： `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL 'string' timeUnit`。

  发出到目前为止已观察到的最大时间戳减去指定延迟的 watermark ，例如， `WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL '5' SECOND` 是一个 5 秒延迟的 watermark 策略。

```sql
CREATE TABLE Orders (
    `user` BIGINT,
    product STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH ( . . . );
```


### `PRIMARY KEY`

主键用作 Flink 优化的一种提示信息。主键限制表明一张表或视图的某个（些）列是唯一的并且不包含 Null 值。
主键声明的列都是非 nullable 的。因此主键可以被用作表行级别的唯一标识。

主键可以和列的定义一起声明，也可以独立声明为表的限制属性，不管是哪种方式，主键都不可以重复定义，否则 Flink 会报错。

**有效性检查**

SQL 标准主键限制可以有两种模式：`ENFORCED` 或者 `NOT ENFORCED`。 它申明了是否输入/出数据会做合法性检查（是否唯一）。Flink 不存储数据因此只支持 `NOT ENFORCED` 模式，即不做检查，用户需要自己保证唯一性。

Flink 假设声明了主键的列都是不包含 Null 值的，Connector 在处理数据时需要自己保证语义正确。

**Notes:** 在 CREATE TABLE 语句中，创建主键会修改列的 nullable 属性，主键声明的列默认都是非 Nullable 的。

### `PARTITIONED BY`

根据指定的列对已经创建的表进行分区。若表使用 filesystem sink ，则将会为每个分区创建一个目录。

### `DISTRIBUTED`

Buckets enable load balancing in an external storage system by splitting data into disjoint subsets. These subsets group rows with potentially "infinite" keyspace into smaller and more manageable chunks that allow for efficient parallel processing.

Bucketing depends heavily on the semantics of the underlying connector. However, a user can influence the bucketing behavior by specifying the number of buckets, the bucketing algorithm, and (if the algorithm allows it) the columns which are used for target bucket calculation.

All bucketing components (i.e. bucket number, distribution algorithm, bucket key columns) are
optional from a SQL syntax perspective.

Given the following SQL statements:

```sql
-- Example 1
CREATE TABLE MyTable (uid BIGINT, name STRING) DISTRIBUTED BY HASH(uid) INTO 4 BUCKETS;

-- Example 2
CREATE TABLE MyTable (uid BIGINT, name STRING) DISTRIBUTED BY (uid) INTO 4 BUCKETS;

-- Example 3
CREATE TABLE MyTable (uid BIGINT, name STRING) DISTRIBUTED BY (uid);

-- Example 4
CREATE TABLE MyTable (uid BIGINT, name STRING) DISTRIBUTED INTO 4 BUCKETS;
```

Example 1 declares a hash function on a fixed number of 4 buckets (i.e. HASH(uid) % 4 = target
bucket). Example 2 leaves the selection of an algorithm up to the connector. Additionally,
Example 3 leaves the number of buckets up  to the connector.
In contrast, Example 4 only defines the number of buckets.

### `WITH` Options

表属性用于创建 table source/sink ，一般用于寻找和创建底层的连接器。

表达式 `key1=val1` 的键和值必须为字符串文本常量。请参考 [连接外部系统]({{< ref "docs/connectors/table/overview" >}}) 了解不同连接器所支持的属性。

**注意：** 表名可以为以下三种格式 1. `catalog_name.db_name.table_name` 2. `db_name.table_name` 3. `table_name`。使用`catalog_name.db_name.table_name` 的表将会与名为 "catalog_name" 的 catalog 和名为 "db_name" 的数据库一起注册到 metastore 中。使用 `db_name.table_name` 的表将会被注册到当前执行的 table environment 中的 catalog 且数据库会被命名为 "db_name"；对于 `table_name`, 数据表将会被注册到当前正在运行的catalog和数据库中。

**注意：** 使用 `CREATE TABLE` 语句注册的表均可用作 table source 和 table sink。 在被 DML 语句引用前，我们无法决定其实际用于 source 抑或是 sink。

### `LIKE`

`LIKE` 子句来源于两种 SQL 特性的变体/组合（Feature T171，“表定义中的 LIKE 语法” 和 Feature T173，“表定义中的 LIKE 语法扩展”）。LIKE 子句可以基于现有表的定义去创建新表，并且可以扩展或排除原始表中的某些部分。与 SQL 标准相反，LIKE 子句必须在 CREATE 语句中定义，并且是基于 CREATE 语句的更上层定义，这是因为 LIKE 子句可以用于定义表的多个部分，而不仅仅是 schema 部分。

你可以使用该子句，重用（或改写）指定的连接器配置属性或者可以向外部表添加 watermark 定义，例如可以向 Apache Hive 中定义的表添加 watermark 定义。

示例如下：

```sql
CREATE TABLE Orders (
    `user` BIGINT,
    product STRING,
    order_time TIMESTAMP(3)
) WITH ( 
    'connector' = 'kafka',
    'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE Orders_with_watermark (
    -- 添加 watermark 定义
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    -- 改写 startup-mode 属性
    'scan.startup.mode' = 'latest-offset'
)
LIKE Orders;
```

结果表 `Orders_with_watermark` 等效于使用以下语句创建的表：

```sql
CREATE TABLE Orders_with_watermark (
    `user` BIGINT,
    product STRING,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    'connector' = 'kafka',
    'scan.startup.mode' = 'latest-offset'
);
```

表属性的合并逻辑可以用 `like options` 来控制。

可以控制合并的表属性如下：

* CONSTRAINTS - 主键和唯一键约束
* GENERATED - 计算列
* OPTIONS - 连接器信息、格式化方式等配置项
* DISTRIBUTION - distribution definition
* PARTITIONS - 表分区信息
* WATERMARKS - watermark 定义

并且有三种不同的表属性合并策略：

* INCLUDING - 新表包含源表（source table）所有的表属性，如果和源表的表属性重复则会直接失败，例如新表和源表存在相同 key 的属性。
* EXCLUDING - 新表不包含源表指定的任何表属性。
* OVERWRITING - 新表包含源表的表属性，但如果出现重复项，则会用新表的表属性覆盖源表中的重复表属性，例如，两个表中都存在相同 key 的属性，则会使用当前语句中定义的 key 的属性值。

并且你可以使用 `INCLUDING/EXCLUDING ALL` 这种声明方式来指定使用怎样的合并策略，例如使用 `EXCLUDING ALL INCLUDING WATERMARKS`，那么代表只有源表的 WATERMARKS 属性才会被包含进新表。

示例如下：
```sql

-- 存储在文件系统的源表
CREATE TABLE Orders_in_file (
    `user` BIGINT,
    product STRING,
    order_time_string STRING,
    order_time AS to_timestamp(order_time_string)
    
)
PARTITIONED BY (`user`) 
WITH ( 
    'connector' = 'filesystem',
    'path' = '...'
);

-- 对应存储在 kafka 的源表
CREATE TABLE Orders_in_kafka (
    -- 添加 watermark 定义
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND 
) WITH (
    'connector' = 'kafka',
    ...
)
LIKE Orders_in_file (
    -- 排除需要生成 watermark 的计算列之外的所有内容。
    -- 去除不适用于 kafka 的所有分区和文件系统的相关属性。
    EXCLUDING ALL
    INCLUDING GENERATED
);
```

如果未提供 like 配置项（like options），默认将使用 `INCLUDING ALL OVERWRITING OPTIONS` 的合并策略。

**注意：** 您无法选择物理列的合并策略，当物理列进行合并时就如使用了 `INCLUDING` 策略。

**注意：** 源表 `source_table` 可以是一个组合 ID。您可以指定不同 catalog 或者 DB 的表作为源表: 例如，`my_catalog.my_db.MyTable` 指定了源表 `MyTable` 来源于名为 `MyCatalog` 的 catalog  和名为 `my_db` 的 DB ，`my_db.MyTable` 指定了源表 `MyTable` 来源于当前 catalog  和名为 `my_db` 的 DB。

### `AS select_statement`

表也可以通过一个 CTAS 语句中的查询结果来创建并填充数据，CTAS 是一种简单、快捷的创建表并插入数据的方法。

CTAS 有两个部分，SELECT 部分可以是 Flink SQL 支持的任何 [SELECT 查询]({{< ref "docs/dev/table/sql/queries/overview" >}})。 CREATE 部分从 SELECT 查询中获取列信息，并创建目标表。 与 `CREATE TABLE` 类似，CTAS 要求必须在目标表的 WITH 子句中指定必填的表属性。

CTAS 的建表操作需要依赖目标 Catalog。比如，Hive Catalog 会自动在 Hive 中创建物理表。但是基于内存的 Catalog 只会将表的元信息注册在执行 SQL 的 Client 的内存中。

示例如下:

```sql
CREATE TABLE my_ctas_table
WITH (
    'connector' = 'kafka',
    ...
)
AS SELECT id, name, age FROM source_table WHERE mod(id, 10) = 0;
```

结果表 `my_ctas_table` 等效于使用以下语句创建表并写入数据:
```sql
CREATE TABLE my_ctas_table (
    id BIGINT,
    name STRING,
    age INT
) WITH (
    'connector' = 'kafka',
    ...
);
 
INSERT INTO my_ctas_table SELECT id, name, age FROM source_table WHERE mod(id, 10) = 0;
```

The `CREATE` part allows you to specify explicit columns. The resulting table schema will contain the columns defined in the `CREATE` part first followed by the columns from the `SELECT` part. Columns named in both parts, in the `CREATE` and `SELECT` parts, keep the same column position as defined in the `SELECT` part. The data type of `SELECT` columns can also be overridden if specified in the `CREATE` part.

Consider the example statement below:

```sql
CREATE TABLE my_ctas_table (
    desc STRING,
    quantity DOUBLE,   
    cost AS price * quantity,
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,
) WITH (
    'connector' = 'kafka',
    ...
) AS SELECT id, price, quantity, order_time FROM source_table;
```

The resulting table `my_ctas_table` will be equivalent to create the following table and insert the data with the following statement:

```
CREATE TABLE my_ctas_table (
    desc STRING,
    cost AS price * quantity,
    id BIGINT,
    price DOUBLE,
    quantity DOUBLE,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    ...
);

INSERT INTO my_ctas_table (id, price, quantity, order_time)
    SELECT id, price, quantity, order_time FROM source_table;
```

The `CREATE` part also lets you specify primary keys and distribution strategies. Notice that primary keys work only on `NOT NULL` columns. Currently, primary keys only allow you to define columns from the `SELECT` part which may be `NOT NULL`. The `CREATE` part does not allow `NOT NULL` column definitions.

Consider the example statement below where `id` is a not null column in the `SELECT` part:

```sql
CREATE TABLE my_ctas_table (
    PRIMARY KEY (id) NOT ENFORCED
) DISTRIBUTED BY (id) INTO 4 buckets 
AS SELECT id, name FROM source_table;
```

The resulting table `my_ctas_table` will be equivalent to create the following table and insert the data with the following statement:

```
CREATE TABLE my_ctas_table (
    id BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,
    name STRING 
) DISTRIBUTED BY (id) INTO 4 buckets;

INSERT INTO my_ctas_table SELECT id, name FROM source_table;
```

`CTAS` also allows you to reorder the columns defined in the `SELECT` part by specifying all column names without data types in the `CREATE` part. This feature is equivalent to the `INSERT INTO` statement.
The columns specified must match the names and number of columns in the `SELECT` part. This definition cannot be combined with new columns, which requires defining data types.

Consider the example statement below:

```sql
CREATE TABLE my_ctas_table (
    order_time, price, quantity, id
) WITH (
    'connector' = 'kafka',
    ...
) AS SELECT id, price, quantity, order_time FROM source_table;
```

The resulting table `my_ctas_table` will be equivalent to create the following table and insert the data with the following statement:

```
CREATE TABLE my_ctas_table (
    order_time TIMESTAMP(3),
    price DOUBLE,
    quantity DOUBLE,
    id BIGINT
) WITH (
    'connector' = 'kafka',
    ...
);

INSERT INTO my_ctas_table (order_time, price, quantity, id)
    SELECT id, price, quantity, order_time FROM source_table;
```

**Note:** CTAS has these restrictions:
* Does not support creating a temporary table yet.
* Does not support creating partitioned table yet.

**注意：** CTAS 有如下约束：
* 暂不支持创建临时表。
* 暂不支持创建分区表。

**注意：** 默认情况下，CTAS 是非原子性的，这意味着如果在向表中插入数据时发生错误，该表不会被自动删除。

#### 原子性

如果要启用 CTAS 的原子性，则应确保：
* 对应的 Connector sink 已经实现了 CTAS 的原子性语义，你可能需要阅读对应 Connector 的文档看是否已经支持了原子性语义。如果开发者想要实现原子性语义，请参考文档 [SupportsStaging]({{< ref "docs/dev/table/sourcesSinks" >}}#sink-abilities)。
* 设置配置项 [table.rtas-ctas.atomicity-enabled]({{< ref "docs/dev/table/config" >}}#table-rtas-ctas-atomicity-enabled) 为 `true`。

{{< top >}}

## [CREATE OR] REPLACE TABLE
```sql
[CREATE OR] REPLACE TABLE [catalog_name.][db_name.]table_name
  [(
    { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
    [ <table_constraint> ][ , ...n]
  )]
[COMMENT table_comment]
[ <distribution> ]
WITH (key1=val1, key2=val2, ...)
AS select_query
```

**注意：** RTAS 有如下语义:
* REPLACE TABLE AS SELECT 语句：要被替换的目标表必须存在，否则会报错。
* CREATE OR REPLACE TABLE AS SELECT 语句：要被替换的目标表如果不存在，引擎会自动创建；如果存在的话，引擎就直接替换它。

表可以通过一个 [CREATE OR] REPLACE TABLE AS SELECT（RTAS）语句中的查询结果来替换（或创建）并填充数据，RTAS 是一种简单快捷的替换（或创建）表并插入数据的方法。

RTAS 有两个部分：SELECT 部分可以是 Flink SQL 支持的任何 [SELECT 查询]({{< ref "docs/dev/table/sql/queries/overview" >}})， `REPLACE TABLE` 部分会先删除已经存在的目标表，然后根据从 `SELECT` 查询中获取列信息，创建新的目标表。 与 `CREATE TABLE` 和 `CTAS` 类似，RTAS 要求必须在目标表的 WITH 子句中指定必填的表属性。

示例如下:

```sql
REPLACE TABLE my_rtas_table
WITH (
    'connector' = 'kafka',
    ...
)
AS SELECT id, name, age FROM source_table WHERE mod(id, 10) = 0;
```

`REPLACE TABLE AS SELECT` 语句等价于使用以下语句先删除表，然后创建表并写入数据:
```sql
DROP TABLE my_rtas_table;

CREATE TABLE my_rtas_table (
    id BIGINT,
    name STRING,
    age INT
) WITH (
    'connector' = 'kafka',
    ...
);
 
INSERT INTO my_rtas_table SELECT id, name, age FROM source_table WHERE mod(id, 10) = 0;
```

Similar to `CREATE TABLE AS`, `REPLACE TABLE AS` allows you to specify explicit columns, watermarks, primary keys and distribution strategies. The resulting table schema is built from the `CREATE` part first followed by the columns from the `SELECT` part. Columns named in both parts, in the `CREATE` and `SELECT` parts, keep the same column position as defined in the `SELECT` part. The data type of `SELECT` columns can also be overridden if specified in the `CREATE` part.

Consider the example statement below:

```sql
REPLACE TABLE my_rtas_table (
    desc STRING,
    quantity DOUBLE,   
    cost AS price * quantity,
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,
    PRIMARY KEY (id) NOT ENFORCED
) DISTRIBUTED BY (id) INTO 4 buckets
AS SELECT id, price, quantity, order_time FROM source_table;
```

The resulting table `my_rtas_table` will be equivalent to create the following table and insert the data with the following statement:

```sql
DROP TABLE my_rtas_table;

CREATE TABLE my_rtas_table (
    desc STRING,
    cost AS price * quantity,
    id BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,
    price DOUBLE,
    quantity DOUBLE,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    ...
);

INSERT INTO my_rtas_table (id, price, quantity, order_time)
    SELECT id, price, quantity, order_time FROM source_table;
```

**注意：** RTAS 有如下约束：
* 暂不支持替换临时表。
* 暂不支持创建分区表。

**注意：** 默认情况下，RTAS 是非原子性的，这意味着如果在向表中插入数据时发生错误，该表不会被自动删除或还原成原来的表。
**注意：** RTAS 会先删除表，然后创建表并写入数据。但如果表是在基于内存的 Catalog 里，删除表只会将其从 Catalog 里移除，并不会移除物理表中的数据。因此，执行RTAS语句之前的数据仍然存在。

### 原子性

如果要启用 RTAS 的原子性，则应确保：
* 对应的 Connector sink 已经实现了 RTAS 的原子性语义，你可能需要阅读对应 Connector 的文档看是否已经支持了原子性语义。如果开发者想要实现原子性语义，请参考文档 [SupportsStaging]({{< ref "docs/dev/table/sourcesSinks" >}}#sink-abilities)。
* 设置配置项 [table.rtas-ctas.atomicity-enabled]({{< ref "docs/dev/table/config" >}}#table-rtas-ctas-atomicity-enabled) 为 `true`。

{{< top >}}

## CREATE CATALOG

```sql
CREATE CATALOG [IF NOT EXISTS] catalog_name
  [COMMENT catalog_comment]
  WITH (key1=val1, key2=val2, ...)
```

根据给定的属性创建 catalog。若已存在同名 catalog，会抛出异常。

**IF NOT EXISTS**

若 catalog 已经存在，则不会进行任何操作。

**WITH OPTIONS**

catalog 属性一般用于存储关于这个 catalog 的额外的信息。
表达式 `key1=val1` 中的键和值都需要是字符串文本常量。

详情见 [Catalogs]({{< ref "docs/dev/table/catalogs" >}})。

{{< top >}}

## CREATE DATABASE

```sql
CREATE DATABASE [IF NOT EXISTS] [catalog_name.]db_name
  [COMMENT database_comment]
  WITH (key1=val1, key2=val2, ...)
```

根据给定的表属性创建数据库。若数据库中已存在同名表会抛出异常。

**IF NOT EXISTS**

若数据库已经存在，则不会进行任何操作。

**WITH OPTIONS**

数据库属性一般用于存储关于这个数据库额外的信息。
表达式 `key1=val1` 中的键和值都需要是字符串文本常量。

{{< top >}}

## CREATE VIEW
```sql
CREATE [TEMPORARY] VIEW [IF NOT EXISTS] [catalog_name.][db_name.]view_name
  [{columnName [, columnName ]* }] [COMMENT view_comment]
  AS query_expression
```

根据给定的 query 语句创建一个视图。若数据库中已经存在同名视图会抛出异常.

**TEMPORARY**

创建一个有 catalog 和数据库命名空间的临时视图，并覆盖原有的视图。

**IF NOT EXISTS**

若该视图已经存在，则不会进行任何操作。

{{< top >}}

## CREATE FUNCTION
```sql
CREATE [TEMPORARY|TEMPORARY SYSTEM] FUNCTION
  [IF NOT EXISTS] [[catalog_name.]db_name.]function_name
  AS identifier [LANGUAGE JAVA|SCALA|PYTHON]
  [USING JAR '<path_to_filename>.jar' [, JAR '<path_to_filename>.jar']* ]
```

创建一个有 catalog 和数据库命名空间的 catalog function ，需要指定一个 identifier ，可指定 language tag 。 若 catalog 中，已经有同名的函数注册了，则无法注册。

如果 language tag 是 JAVA 或者 SCALA ，则 identifier 是 UDF 实现类的全限定名。关于 JAVA/SCALA UDF 的实现，请参考 [自定义函数]({{< ref "docs/dev/table/functions/udfs" >}})。

如果 language tag 是 PYTHON，则 identifier 是 UDF 对象的全限定名，例如 `pyflink.table.tests.test_udf.add`。关于 PYTHON UDF 的实现，请参考 [Python UDFs]({{< ref "docs/dev/python/table/udfs/python_udfs" >}})。

如果 language tag 是 PYTHON，而当前程序是 Java／Scala 程序或者纯 SQL 程序，则需要[配置 Python 相关的依赖]({{< ref "docs/dev/python/dependency_management" >}}#python-dependency-in-javascala-program)。

**TEMPORARY**

创建一个有 catalog 和数据库命名空间的临时 catalog function ，并覆盖原有的 catalog function 。

**TEMPORARY SYSTEM**

创建一个没有数据库命名空间的临时系统 catalog function ，并覆盖系统内置的函数。

**IF NOT EXISTS**

若该函数已经存在，则不会进行任何操作。

**LANGUAGE JAVA\|SCALA\|PYTHON**

Language tag 用于指定 Flink runtime 如何执行这个函数。目前，只支持 JAVA, SCALA 和 PYTHON，且函数的默认语言为 JAVA。

**USING**

指定包含该函数的实现及其依赖的 jar 资源列表。该 jar 应该位于 Flink 当前支持的本地或远程[文件系统]({{< ref "docs/deployment/filesystems/overview" >}}) 中，比如 hdfs/s3/oss。

<span class="label label-danger">注意</span> 目前只有 JAVA、SCALA 语言支持 USING 子句。

## CREATE MODEL
```sql
CREATE [TEMPORARY] MODEL [IF NOT EXISTS] [catalog_name.][db_name.]model_name
  [(
    { <input_column_definition> }[ , ...n]
    { <output_column_definition> }[ , ...n]
  )]
  [COMMENT model_comment]
  WITH (key1=val1, key2=val2, ...)

<input_column_definition>:
  column_name column_type [COMMENT column_comment]

<output_column_definition>:
  column_name column_type [COMMENT column_comment]
```

创建一个有 catalog 和数据库命名空间的模型。若 catalog 中已经存在同名模型，则无法注册。

**TEMPORARY**

创建一个有 catalog 和数据库命名空间的临时模型，并覆盖原有的模型。

**IF NOT EXISTS**

若该模型已经存在，则不会进行任何操作。

**Input/Output**

输入列定义了将用于模型推理的特征。输出列定义了模型将产生的预测结果。每个列必须有名称和数据类型。

**COMMENT**

为模型添加注释。

**WITH OPTIONS**

用于存储与此模型相关的额外信息的模型属性。这些属性通常用于查找和创建底层模型提供者。表达式 `key1=val1` 中的键和值都应该是字符串字面量。

**注意：** 模型属性和支持的模型类型可能因底层模型提供者而异。

### 示例

以下示例展示了 `CREATE MODEL` 语句的使用方法：

```sql
CREATE MODEL sentiment_analysis_model 
INPUT (text STRING COMMENT '用于情感分析的输入文本') 
OUTPUT (sentiment STRING COMMENT '预测的情感（positive/negative/neutral/mixed）')
COMMENT '用于文本情感分析的模型'
WITH (
    'provider' = 'openai',
    'endpoint' = 'https://api.openai.com/v1/chat/completions',
    'api-key' = '<YOUR KEY>',
    'model'='gpt-3.5-turbo',
    'system-prompt' = 'Classify the text below into one of the following labels: [positive, negative, neutral, mixed]. Output only the label.'
);
```

{{< top >}}
