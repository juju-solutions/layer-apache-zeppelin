## Overview

Apache Zeppelin is a web-based notebook that enables interactive data analytics.
You can make beautiful data-driven, interactive, and collaborative documents
with SQL, Scala and more.

As a Multi-purpose Notebook, Apache Zeppelin is the place for interactive:

 * Data Ingestion
 * Data Discovery
 * Data Analytics
 * Data Visualization & Collaboration


## Usage

This is a subordinate charm that requires the `apache-spark` interface. This
means that you will need to deploy a base Apache Spark cluster to use
Zeppelin. An easy way to deploy the recommended environment is to use the
[apache-hadoop-spark-zeppelin](https://jujucharms.com/apache-hadoop-spark-zeppelin)
bundle. This will deploy the Apache Hadoop platform with an Apache Spark +
Zeppelin unit that communicates with the cluster by relating to the
`apache-hadoop-plugin` subordinate charm:

    juju-quickstart apache-hadoop-spark-zeppelin

Alternatively, you may manually deploy the recommended environment as follows:

    juju deploy apache-hadoop-hdfs-master hdfs-master
    juju deploy apache-hadoop-yarn-master yarn-master
    juju deploy apache-hadoop-compute-slave compute-slave
    juju deploy apache-hadoop-plugin plugin
    juju deploy apache-spark spark
    juju deploy apache-zeppelin zeppelin

    juju add-relation yarn-master hdfs-master
    juju add-relation compute-slave yarn-master
    juju add-relation compute-slave hdfs-master
    juju add-relation plugin yarn-master
    juju add-relation plugin hdfs-master
    juju add-relation spark plugin
    juju add-relation zeppelin spark

Once deployment is complete, expose Zeppelin. Since this is subordinate to
Spark, you will need to expose both services:

    juju expose spark
    juju expose zeppelin

You may now access the web interface at
http://{spark_unit_ip_address}:9090. The ip address can be found by running
`juju status spark | grep public-address`.


## Testing the deployment

By default, this deployment uses Spark in YARN mode and supports storing
job data in HDFS. To test this, access the Zeppelin web interface at
http://{spark_unit_ip_address}:9090. The ip address can be found by running
`juju status spark | grep public-address`.

  - Verify there is a green icon in the upper-right corner that says "Connected"
  - Click the `Zeppelin HDFS Tutorial` link
  - Click the `Save` button to bind the tutorial to our supported interpreters
  - Click the `Play` button (arrow at the top of the page)
  - Click `OK` when prompted to run all paragraphs

The tutorial may take 5-10 minutes to run as it retrieves sample data,
processes jobs, and stores results in HDFS. When successful, each paragraph will
report `FINISHED` in their respective upper-right corners


## Limitations

### Spark Interpreter Settings
Zeppelin Spark interpreter configuration is set according to environment
variable values at deploy time. If you alter these variables post
deployment (e.g., `juju set spark spark_execution_mode=NEW_VALUE`), you will
need to edit Zeppelin's Spark interpreter to match the new value. Do this on
the `Interpreter` tab of the Zeppelin web interface.

 * Affected Spark Interpreter configuration includes:

   * master
   * spark.executor.memory


## Contact Information

- <bigdata@lists.ubuntu.com>


## Help

- [Juju mailing list](https://lists.ubuntu.com/mailman/listinfo/juju)
- [Juju community](https://jujucharms.com/community)
