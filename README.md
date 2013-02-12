hive-udaf-maxrow
================

hive-udaf-maxrow is a simple user-defined aggregate function (UDAF) for Hive.

The `maxrow()` aggregate function is similar to the built-in `max()` function,
but it allows you to refer to additional columns in the maximal row.

Example
-------

For example, given the following data in a Hive table:

<table>
<tr><th>id</th><th>ts</th><th>somedata</th></tr>
<tr><td>1</td><td>2</td><td>data-1,2</td></tr>
<tr><td>1</td><td>3</td><td>data-1,3</td></tr>
<tr><td>1</td><td>4</td><td>data-1,4</td></tr>
<tr><td>2</td><td>5</td><td>data-2,5</td></tr>
<tr><td>2</td><td>3</td><td>data-2,3</td></tr>
<tr><td>2</td><td>4</td><td>data-2,4</td></tr>
<tr><td>3</td><td>6</td><td>data-3,6</td></tr>
<tr><td>3</td><td>1</td><td>data-3,1</td></tr>
<tr><td>3</td><td>4</td><td>data-3,4</td></tr>
</table>

You can query this table using the `maxrow()` function:

    hive> ADD JAR hive-udaf-maxrow.jar;
    hive> CREATE TEMPORARY FUNCTION maxrow AS 'com.scribd.hive.udaf.GenericUDAFMaxRow';
    hive> SELECT id, maxrow(ts, somedata) FROM sometable GROUP BY id;

<table>
<tr><th>id</th><th>maxrow</th></tr>
<tr><td>1</td><td>{"col0":4,"col1":"data-1,4"}</td></tr>
<tr><td>2</td><td>{"col0":5,"col1":"data-2,5"}</td></tr>
<tr><td>3</td><td>{"col0":6,"col1":"data-3,6"}</td></tr>
</table>

While `maxrow()` looks only at its first parameter ("`ts`" in this case) to compute
the maximum value, it carries along any additional values ("`somedata`" in this
case).

Since `maxrow()` returns a "struct" value (see below), you can parse the result
with Hive's built-in "dot" notation.  For example:

    hive> SELECT id, m.col0 as ts, m.col1 as somedata FROM (
              SELECT id, maxrow(ts, somedata) as m FROM sometable GROUP BY id
          ) s;

<table>
<tr><th>id</th><th>ts</th><th>somedata</th></tr>
<tr><td>1</td><td>4</td><td>data-1,4</td></tr>
<tr><td>2</td><td>5</td><td>data-2,5</td></tr>
<tr><td>3</td><td>6</td><td>data-3,6</td></tr>
</table>

Limitations
-----------

As can be seen from the example above, there are a couple of limitations due to
how Hive UDAFs work:

* A UDAF can only output a value for a single column.  Therefore, `maxrow()`
  returns a complex-valued "struct" object.
* Hive does not provide the UDAF with the name of the columns that are being
  passed as input to the UDAF.  Therefore, `maxrow()` generates simple names
  such as "col0", "col1", etc.

Building
--------

To build hive-udaf-maxrow, you need to specify the location of your Hadoop and
Hive jar files using the `HADOOP_HOME` and `HIVE_HOME` environment variables.
The build classpath will include all of the jar files in these directories and
and their `lib/` subdirectories.  For example:

    > HADOOP_HOME=/path/to/hadoop HIVE_HOME=/path/to/hive ant

A successful build will create the `dist/hive-udaf-maxrow.jar` file.  You can
add this jar file to your Hive session using the `ADD JAR` command shown above.
