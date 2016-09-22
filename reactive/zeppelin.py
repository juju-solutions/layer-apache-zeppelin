# pylint: disable=unused-argument
import hashlib

from charmhelpers.core import unitdata
from charms.reactive import when, when_not
from charms.reactive import set_state, remove_state
from charms.reactive.helpers import data_changed
from charmhelpers.core import hookenv
from charms.layer.apache_zeppelin import Zeppelin, ZeppelinAPI


@when('spark.ready')
@when_not('zeppelin.installed')
def install_zeppelin(hadoop):
    zepp = Zeppelin.get()
    hookenv.status_set('maintenance', 'Installing Zeppelin')
    if zepp.install():
        zepp.setup_zeppelin()
        set_state('zeppelin.installed')
    else:
        hookenv.status_set('blocked', 'unable to fetch zeppelin resource')


@when('zeppelin.installed', 'spark.ready')
@when_not('zeppelin.started')
def configure_zeppelin(spark):
    hookenv.status_set('maintenance', 'Setting up Zeppelin')
    zepp = Zeppelin.get()
    zepp.configure_zeppelin()
    zepp.start()
    zepp.open_ports()
    set_state('zeppelin.started')
    hookenv.status_set('active', 'Ready')


@when('zeppelin.started', 'spark.ready')
def update_spark_master(spark):
    zepp = Zeppelin.get()
    api = ZeppelinAPI()
    master_url = spark.get_master_url()
    if data_changed('spark.master', master_url):
        api.modify_interpreter('spark', properties={
            'master': master_url,
        })
        zepp.restart()


@when('zeppelin.started')
@when_not('spark.ready')
def stop_zeppelin():
    hookenv.status_set('maintenance', 'Stopping Zeppelin')
    zepp = Zeppelin.get()
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


@when('zeppelin.started', 'client.notebook.registered')
def register_notebook(client):
    notebooks = client.unregistered_notebooks()
    api = ZeppelinAPI()
    id_map = unitdata.kv().get('zeppelin.notebooks.id_map', {})
    for notebook in notebooks:
        notebook_md5 = hashlib.md5(notebook.encode('utf8')).hexdigest()
        notebook_id = api.import_notebook(notebook)
        if notebook_id:
            hookenv.log('Registered notebook: {} -> {}'.format(notebook_md5,
                                                               notebook_id))
            id_map[notebook_md5] = notebook_id
            client.accept_notebook(notebook)
        else:
            hookenv.log('Rejected notebook: {}'.format(notebook_md5))
            client.reject_notebook(notebook)
    unitdata.kv().set('zeppelin.notebooks.id_map', id_map)


@when('zeppelin.started', 'client.notebook.removed')
def remove_notebook(client):
    notebooks = client.unremoved_notebooks()
    api = ZeppelinAPI()
    id_map = unitdata.kv().get('zeppelin.notebooks.id_map', {})
    for notebook in notebooks:
        notebook_md5 = hashlib.md5(notebook.encode('utf8')).hexdigest()
        notebook_id = id_map.get(notebook_md5, None)
        if not notebook_id:
            hookenv.log('Skipping removing unknown notebook: {}'.format(
                notebook_md5))
            continue
        hookenv.log('Removing notebook: {} -> {}'.format(notebook_md5,
                                                         notebook_id))
        api.delete_notebook(notebook_id)
        client.remove_notebook(notebook)
        del id_map[notebook_md5]
    unitdata.kv().set('zeppelin.notebooks.id_map', id_map)


@when('zeppelin.started', 'client.interpreter.change')
def modify_interpreter(client):
    zepp = Zeppelin.get()
    api = ZeppelinAPI()
    for interpreter in client.interpreter_changes():
        try:
            api.modify_interpreter(interpreter['name'],
                                   interpreter['properties'])
            client.accept_interpreter_change(interpreter)
            zepp.restart()
        except ValueError as e:
            hookenv.log('Rejected change for "{}" interpreter: {}'.format(
                interpreter['name'], e))
            client.reject_interpreter_change(interpreter, str(e))
