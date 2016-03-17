# pylint: disable=unused-argument
from charms.reactive import when, when_not
from charms.reactive import set_state, remove_state
from charmhelpers.core import hookenv
from charms.layer.apache_zeppelin import Zeppelin


@when('spark.ready')
@when_not('zeppelin.installed')
def install_zeppelin(hadoop):
    zepp = Zeppelin()
    if zepp.verify_resources():
        hookenv.status_set('maintenance', 'Installing Zeppelin')
        zepp.install()
        set_state('zeppelin.installed')


@when('zeppelin.installed', 'spark.ready')
@when_not('zeppelin.started')
def configure_zeppelin(spark):
    hookenv.status_set('maintenance', 'Setting up Zeppelin')
    zepp = Zeppelin()
    zepp.setup_zeppelin()
    zepp.configure_zeppelin()
    zepp.start()
    zepp.open_ports()
    set_state('zeppelin.started')
    hookenv.status_set('active', 'Ready')


@when('zeppelin.started')
@when_not('spark.ready')
def stop_zeppelin():
    zepp = Zeppelin()
    zepp.stop()
    remove_state('zepplin.started')


@when_not('spark.joined')
def report_blocked():
    hookenv.status_set('blocked', 'Waiting for relation to Apache Spark')


@when('spark.joined')
@when_not('spark.ready')
def report_waiting(spark):
    hookenv.status_set('waiting', 'Waiting for Apache Spark to become ready')
