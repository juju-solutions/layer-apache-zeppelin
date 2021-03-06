import os
import json
import time
import socket
from importlib import import_module

import jujuresources

from path import Path
from jujubigdata import utils
import requests
from urllib.parse import urljoin
from subprocess import call
from charmhelpers.core import unitdata, hookenv, host
from charmhelpers import fetch

from charms import layer
from charms.templating.jinja2 import render


class Zeppelin(object):
    """
    This class manages the Zeppelin deployment steps.

    :param DistConfig dist_config: The configuration container object needed.
    """
    @classmethod
    def factory(cls):
        """
        Dynamically instantiate this or a subclass, to allow other layers
        to override portions of this implementation.
        """
        impl = layer.options('apache-zeppelin')['implementation_class']
        module_name, cls_name = impl.rsplit('.', 1)
        cls = getattr(import_module(module_name), cls_name)
        return cls()

    def __init__(self, dist_config=None):
        self.dist_config = dist_config or utils.DistConfig()
        self.resources = {
            'zeppelin': 'zeppelin-%s' % utils.cpu_arch(),
        }

    def verify_resources(self):
        try:
            filename = hookenv.resource_get('zeppelin')
            if not filename:
                return False
            if Path(filename).size == 0:
                # work around charm store resource upload issue
                # by falling-back to pulling from S3
                raise NotImplementedError()
            return True
        except NotImplementedError:
            if not jujuresources.resource_defined(self.resources['zeppelin']):
                return False
            return utils.verify_resources(*self.resources.values())()

    def install(self, force=False):
        '''
        Create the directories. This method is to be called only once.

        :param bool force: Force the execution of the installation even if this
        is not the first installation attempt.
        '''
        destination = self.dist_config.path('zeppelin')

        if not self.verify_resources():
            return False

        if destination.exists() and not force:
            return True

        try:
            filename = hookenv.resource_get('zeppelin')
            if not filename:
                return False
            if Path(filename).size == 0:
                # work around charm store resource upload issue
                # by falling-back to pulling from S3
                raise NotImplementedError()
            destination.rmtree_p()  # if reinstalling
            extracted = Path(fetch.install_remote('file://' + filename))
            extracted.dirs()[0].copytree(destination)  # only copy nested dir
        except NotImplementedError:
            if not jujuresources.resource_defined(self.resources['zeppelin']):
                return False
            if not utils.verify_resources(*self.resources.values())():
                return False
            jujuresources.install(self.resources['zeppelin'],
                                  destination=destination,
                                  skip_top_level=True)

        self.dist_config.add_dirs()
        self.dist_config.add_packages()
        return True

    def setup_zeppelin(self):
        self.setup_zeppelin_config()
        self.setup_init_scripts()
        self.setup_zeppelin_tutorial()

    def setup_zeppelin_config(self):
        '''
        copy the default configuration files to zeppelin_conf property
        '''
        default_conf = self.dist_config.path('zeppelin') / 'conf'
        zeppelin_conf = self.dist_config.path('zeppelin_conf')
        zeppelin_conf.rmtree_p()
        default_conf.copytree(zeppelin_conf)

        zeppelin_env = self.dist_config.path('zeppelin_conf') / 'zeppelin-env.sh'
        if not zeppelin_env.exists():
            (self.dist_config.path('zeppelin_conf') / 'zeppelin-env.sh.template').copy(zeppelin_env)
        zeppelin_site = self.dist_config.path('zeppelin_conf') / 'zeppelin-site.xml'
        if not zeppelin_site.exists():
            (self.dist_config.path('zeppelin_conf') / 'zeppelin-site.xml.template').copy(zeppelin_site)

    def setup_init_scripts(self):
        if host.init_is_systemd():
            template_path = '/etc/systemd/system/zeppelin.service'
            template_name = 'systemd.conf'
        else:
            template_path = '/etc/init/zeppelin.conf'
            template_name = 'upstart.conf'
        if os.path.exists(template_path):
            template_path_backup = "{}.backup".format(template_path)
            if os.path.exists(template_path_backup):
                os.remove(template_path_backup)
            os.rename(template_path, template_path_backup)

        render(
            template_name,
            template_path,
            context={
                'zeppelin_home': self.dist_config.path('zeppelin'),
                'zeppelin_conf': self.dist_config.path('zeppelin_conf')
            },
        )

        if host.init_is_systemd():
            utils.run_as('root', 'systemctl', 'enable', 'zeppelin.service')
            utils.run_as('root', 'systemctl', 'daemon-reload')

    def setup_zeppelin_tutorial(self):
        # The default zepp tutorial doesn't work with spark+hdfs (which is our
        # default env). Include our own tutorial, which does work in a
        # spark+hdfs env. Inspiration for this notebook came from here:
        #   https://github.com/apache/incubator-zeppelin/pull/46
        notebook_dir = self.dist_config.path('zeppelin_notebooks')
        dist_notebook_dir = self.dist_config.path('zeppelin') / 'notebook'
        dist_tutorial_dir = dist_notebook_dir.dirs()[0]
        notebook_dir.rmtree_p()
        dist_tutorial_dir.move(notebook_dir)
        self.copy_tutorial("hdfs-tutorial")
        self.copy_tutorial("flume-tutorial")
        dist_notebook_dir.rmtree_p()
        # move the tutorial dir included in the tarball to our notebook dir and
        # symlink that dir under our zeppelin home. we've seen issues where
        # zepp doesn't honor ZEPPELIN_NOTEBOOK_DIR and instead looks for
        # notebooks in ZEPPELIN_HOME/notebook.
        notebook_dir.symlink(dist_notebook_dir)

        # make sure the notebook dir's contents are owned by our user
        cmd = "chown -R ubuntu:hadoop {}".format(notebook_dir)
        call(cmd.split())

    def copy_tutorial(self, tutorial_name):
        tutorial_source = Path('resources/{}'.format(tutorial_name))
        tutorial_source.copytree(self.dist_config.path('zeppelin_notebooks') / tutorial_name)

    def configure_zeppelin(self):
        '''
        Configure zeppelin environment for all users
        '''
        zeppelin_bin = self.dist_config.path('zeppelin') / 'bin'
        with utils.environment_edit_in_place('/etc/environment') as env:
            if zeppelin_bin not in env['PATH']:
                env['PATH'] = ':'.join([env['PATH'], zeppelin_bin])
            env['ZEPPELIN_CONF_DIR'] = self.dist_config.path('zeppelin_conf')

        zeppelin_site = self.dist_config.path('zeppelin_conf') / 'zeppelin-site.xml'
        with utils.xmlpropmap_edit_in_place(zeppelin_site) as xml:
            xml['zeppelin.server.port'] = self.dist_config.port('zeppelin')
            xml['zeppelin.notebook.dir'] = self.dist_config.path('zeppelin_notebooks')

        etc_env = utils.read_etc_env()
        hadoop_conf_dir = etc_env.get('HADOOP_CONF_DIR', '/etc/hadoop/conf')
        hadoop_extra_classpath = etc_env.get('HADOOP_EXTRA_CLASSPATH', '')
        spark_home = etc_env.get('SPARK_HOME', '/usr/lib/spark')
        spark_driver_mem = etc_env.get('SPARK_DRIVER_MEMORY', '1g')
        spark_exe_mode = os.environ.get('MASTER', 'yarn-client')
        spark_executor_mem = etc_env.get('SPARK_EXECUTOR_MEMORY', '1g')
        zeppelin_env = self.dist_config.path('zeppelin_conf') / 'zeppelin-env.sh'
        with open(zeppelin_env, "a") as f:
            f.write('export ZEPPELIN_CLASSPATH_OVERRIDES={}\n'.format(hadoop_extra_classpath))
            f.write('export ZEPPELIN_HOME={}\n'.format(self.dist_config.path('zeppelin')))
            f.write('export ZEPPELIN_JAVA_OPTS="-Dspark.driver.memory={} -Dspark.executor.memory={}"\n'.format(
                spark_driver_mem,
                spark_executor_mem))
            f.write('export ZEPPELIN_LOG_DIR={}\n'.format(self.dist_config.path('zeppelin_logs')))
            f.write('export ZEPPELIN_MEM="-Xms128m -Xmx1024m -XX:MaxPermSize=512m"\n')
            f.write('export ZEPPELIN_NOTEBOOK_DIR={}\n'.format(self.dist_config.path('zeppelin_notebooks')))
            f.write('export SPARK_HOME={}\n'.format(spark_home))
            f.write('export SPARK_SUBMIT_OPTIONS="--driver-memory {} --executor-memory {}"\n'.format(
                spark_driver_mem,
                spark_executor_mem))
            f.write('export HADOOP_CONF_DIR={}\n'.format(hadoop_conf_dir))
            f.write('export PYTHONPATH={s}/python:{s}/python/lib/py4j-0.8.2.1-src.zip\n'.format(s=spark_home))
            f.write('export MASTER={}\n'.format(spark_exe_mode))

        # User needs write access to zepp's conf to write interpreter.json
        # on server start. chown the whole conf dir, though we could probably
        # touch that file and chown it, leaving the rest owned as root:root.
        # TODO: weigh implications of have zepp's conf dir owned by non-root.
        cmd = "chown -R ubuntu:hadoop {}".format(self.dist_config.path('zeppelin_conf'))
        call(cmd.split())

    def update_master(self, master_url, master_ip):
        api = ZeppelinAPI()
        api.modify_interpreter('spark', properties={
            'master': master_url,
        })
        self.restart()

    def start(self):
        # Start if we're not already running. We currently dont have any
        # runtime config options, so no need to restart when hooks fire.
        if not utils.jps("zeppelin"):
            host.service_start('zeppelin')
            # wait up to 30s for server to start responding, lest API requests fail
            self.wait_for_api(30)

    def check_connect(self, addr, port):
        try:
            with socket.create_connection((addr, port), timeout=10):
                return True
        except OSError:
            return False

    def wait_for_api(self, timeout):
        start = time.time()
        while time.time() - start < timeout:
            if self.check_connect('localhost', self.dist_config.port('zeppelin')):
                return True
            time.sleep(2)
        raise utils.TimeoutError('Timed-out waiting for connection to Zeppelin')

    def wait_for_stop(self, timeout):
        start = time.time()
        while utils.jps("zeppelin"):
            time.sleep(1)
            if time.time() - start > timeout:
                raise utils.TimeoutError('Zeppelin did not stop')

    def stop(self):
        if utils.jps("zeppelin"):
            host.service_stop('zeppelin')
            # wait for the process to stop, since issuing a start while the
            # process is still running (i.e., restart) could cause it to not
            # start up again
            self.wait_for_stop(30)

    def restart(self):
        self.stop()
        self.start()

    def open_ports(self):
        for port in self.dist_config.exposed_ports('zeppelin'):
            hookenv.open_port(port)

    def close_ports(self):
        for port in self.dist_config.exposed_ports('zeppelin'):
            hookenv.close_port(port)

    def cleanup(self):
        self.dist_config.remove_dirs()
        unitdata.kv().set('zeppelin.installed', False)


class ZeppelinAPI(object):
    """
    Helper for interacting with the Appache Zeppelin REST API.
    """
    def _url(self, *parts):
        dc = utils.DistConfig()
        url = 'http://localhost:{}/api/'.format(dc.port('zeppelin'))
        for part in parts:
            url = urljoin(url, part)
        return url

    def import_notebook(self, contents):
        response = requests.post(self._url('notebook'), data=contents)
        if response.status_code != 201:
            return None
        return response.json()['body']

    def delete_notebook(self, notebook_id):
        requests.delete(self._url('notebook/', notebook_id))

    def modify_interpreter(self, interpreter_name, properties):
        response = requests.get(self._url('interpreter/', 'setting'))
        try:
            body = response.json()['body']
        except json.JSONDecodeError:
            hookenv.log('Invalid response from API server: {} {}'.format(response, response.text),
                        hookenv.ERROR)
            raise
        for interpreter_data in body:
            if interpreter_data['name'] == interpreter_name:
                break
        else:
            raise ValueError('Interpreter not found: {}'.format(interpreter_name))
        interpreter_data['properties'].update(properties)
        response = requests.put(self._url('interpreter/', 'setting/',
                                          interpreter_data['id']),
                                data=json.dumps(interpreter_data))
        if response.status_code != 200:
            raise ValueError('Unable to update interpreter: {}'.format(response.text))
