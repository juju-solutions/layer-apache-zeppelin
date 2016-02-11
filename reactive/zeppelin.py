# pylint: disable=unused-argument
from charms.reactive import when, when_not
from charms.reactive import set_state, remove_state
from charmhelpers.core import hookenv
from charms.zeppelin import Zeppelin


def get_dist_config():
    from jujubigdata.utils import DistConfig  # no available until after bootstrap

    if not getattr(get_dist_config, 'value', None):
        zeppelin_reqs = ['vendor', 'packages', 'dirs', 'ports']
        get_dist_config.value = DistConfig(filename='dist.yaml', required_keys=zeppelin_reqs)
    return get_dist_config.value


@when('spark.available')
@when_not('zeppelin.installed')
def install_zeppelin(hadoop):
    zepp = Zeppelin(get_dist_config())
    if zepp.verify_resources():
        hookenv.status_set('maintenance', 'Installing Zeppelin')
        zepp.install()
        set_state('zeppelin.installed')


@when('zeppelin.installed', 'spark.available')
@when_not('zeppelin.started')
def configure_zeppelin(spark):
    hookenv.status_set('maintenance', 'Setting up Zeppelin')
    zepp = Zeppelin(get_dist_config())
    zepp.setup_zeppelin()
    zepp.configure_zeppelin()
    zepp.start()
    zepp.open_ports()
    set_state('zeppelin.started')
    hookenv.status_set('active', 'Ready')


@when('zeppelin.started')
@when_not('spark.available')
def stop_zeppelin():
    zepp = Zeppelin(get_dist_config())
    zepp.stop()
    remove_state('zepplin.started')


@when_not('spark.related')
def report_blocked():
    hookenv.status_set('blocked', 'Waiting for relation to Apache Spark')


@when('spark.related')
@when_not('spark.available')
def report_waiting(spark):
    hookenv.status_set('waiting', 'Waiting for Apache Spark to become ready')
