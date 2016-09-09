# pylint: disable=unused-argument
import json

from charmhelpers.core import unitdata
from charms.reactive import when, when_not
from charms.reactive import set_state, remove_state
from charmhelpers.core import hookenv
from charms.layer.apache_zeppelin import Zeppelin, ZeppelinAPI


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
    hookenv.status_set('maintenance', 'Stopping Zeppelin')
    zepp = Zeppelin()
    zepp.close_ports()
    zepp.stop()
    remove_state('zeppelin.started')


@when_not('spark.joined')
def report_blocked():
    hookenv.status_set('blocked', 'Waiting for relation to Apache Spark')


@when('spark.joined')
@when_not('spark.ready')
def report_waiting(spark):
    hookenv.status_set('waiting', 'Waiting for Apache Spark to become ready')


@when('zeppelin.installed', 'client.notebook.registered')
def register_notebook(client):
    notebooks = client.unregistered_notebooks()
    api = ZeppelinAPI()
    id_map = unitdata.kv().get('zeppelin.notebooks.id_map', {})
    for notebook in notebooks:
        old_id = json.loads(notebook)['id']
        new_id = api.import_notebook(notebook)
        hookenv.log('Registered notebook: {} -> {}'.format(old_id, new_id))
        id_map[old_id] = new_id
    unitdata.kv().set('zeppelin.notebooks.id_map', id_map)
    client.notebooks_registered()


@when('zeppelin.installed', 'client.notebook.removed')
def remove_notebook(client):
    notebooks = client.removed_notebooks()
    api = ZeppelinAPI()
    id_map = unitdata.kv().get('zeppelin.notebooks.id_map', {})
    for notebook in notebooks:
        old_id = json.loads(notebook)['id']
        new_id = id_map.get(old_id, None)
        if not new_id:
            hookenv.log('Skipping removing unknown notebook: {}'.format(old_id))
            continue
        hookenv.log('Removing notebook: {} -> {}'.format(old_id, new_id))
        api.delete_notebook(new_id)
        del id_map[old_id]
    unitdata.kv().set('zeppelin.notebooks.id_map', id_map)
    client.notebooks_removed()
