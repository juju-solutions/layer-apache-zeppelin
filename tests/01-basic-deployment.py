#!/usr/bin/env python3

import unittest
import amulet


class TestDeploy(unittest.TestCase):
    """
    Trivial deployment test for Apache Zeppelin.

    This charm cannot do anything useful by itself, so integration testing
    is done in the bundle.
    """

    def test_deploy(self):
        self.d = amulet.Deployment(series='trusty')
        self.d.add('spark', 'apache-spark')
        self.d.add('zeppelin', 'apache-zeppelin')
        self.d.relate('spark:client', 'zeppelin:spark')
        self.d.setup(timeout=900)
        self.d.sentry.wait(timeout=1800)
        self.unit = self.d.sentry['zeppelin'][0]


if __name__ == '__main__':
    unittest.main()
