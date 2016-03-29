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

Once deployment is complete, expose Zeppelin:

    juju expose zeppelin

You may now access the web interface at
http://{spark_unit_ip_address}:9090. The ip address can be found by running
`juju status spark | grep public-address`.


## Verify the deployment

### Status and Smoke Test

The services provide extended status reporting to indicate when they are ready:

    juju status --format=tabular

This is particularly useful when combined with `watch` to track the on-going
progress of the deployment:

    watch -n 0.5 juju status --format=tabular

The message for each unit will provide information about that unit's state.
Once they all indicate that they are ready, you can perform a "smoke test"
to verify that Zeppelin is working as expected using the built-in `smoke-test`
action:

    juju action do zeppelin/0 smoke-test

After a few seconds or so, you can check the results of the smoke test:

    juju action status

You will see `status: completed` if the smoke test was successful, or
`status: failed` if it was not.  You can get more information on why it failed
via:

    juju action fetch <action-id>


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
