"""Contract checks on packaged/rendered resources, without a Kubernetes cluster."""
import os
from pathlib import Path
import subprocess
import tarfile
import unittest
import yaml

MODULE = Path(__file__).resolve().parents[1]
CHART = MODULE / 'target/candybox-chart'
HELM = os.environ.get('HELM', 'helm')


def render(*args, success=True):
    result = subprocess.run([HELM, 'template', 'sample', str(CHART), '-n', 'testing', *args],
                            capture_output=True, text=True)
    if success:
        if result.returncode:
            raise AssertionError(result.stderr)
        return [d for d in yaml.safe_load_all(result.stdout) if d]
    if result.returncode == 0:
        raise AssertionError('Expected invalid configuration to fail')
    return result.stderr


def find(docs, kind, suffix):
    return next(d for d in docs if d['kind'] == kind and d['metadata']['name'].endswith(suffix))


class ChartTest(unittest.TestCase):
    def test_package_version_and_bundled_dependency(self):
        chart = yaml.safe_load((CHART / 'Chart.yaml').read_text())
        self.assertEqual(chart['version'], chart['appVersion'])
        with tarfile.open(MODULE / f"target/candybox-chart-{chart['version']}.tgz") as archive:
            names = archive.getnames()
            self.assertIn('candybox-chart/charts/zookeeper/Chart.yaml', names)
            self.assertIn('candybox-chart/charts/zookeeper/charts/common/Chart.yaml', names)

    def test_demo_has_one_bookie_and_no_persistent_claims(self):
        docs = render('-f', str(CHART / 'values-demo.yaml'))
        for d in docs:
            if d['kind'] == 'StatefulSet':
                self.assertEqual(1, d['spec']['replicas'])
                self.assertFalse(d['spec'].get('volumeClaimTemplates'))
        config = find(docs, 'ConfigMap', '-chart-config')['data']['node.properties']
        for role in ('wal', 'manifest', 'sstable', 'syrup'):
            self.assertIn(f'quorum.{role}=1/1/1', config)
        self.assertFalse(any(d['kind'] == 'PodDisruptionBudget' for d in docs))

    def test_production_persistence_secrets_and_recovery(self):
        docs = render('-f', str(CHART / 'values-production.yaml'))
        bookie = find(docs, 'StatefulSet', '-bookkeeper')
        self.assertEqual(4, bookie['spec']['replicas'])
        self.assertEqual({'journal', 'ledgers'},
                         {c['metadata']['name'] for c in bookie['spec']['volumeClaimTemplates']})
        self.assertEqual('Parallel', bookie['spec']['podManagementPolicy'])
        find(docs, 'Deployment', '-autorecovery')
        node = find(docs, 'StatefulSet', '-node')['spec']['template']['spec']
        self.assertIn({'secretRef': {'name': 'candybox-node-env'}}, node['containers'][0]['envFrom'])
        self.assertTrue(node['containers'][0]['securityContext']['readOnlyRootFilesystem'])
        config = find(docs, 'ConfigMap', '-chart-config')['data']['node.properties']
        self.assertIn('auth.required=true', config)

    def test_external_clusters_create_no_storage_workloads(self):
        docs = render('--set', 'zookeeper.enabled=false', '--set', 'bookkeeper.enabled=false',
                      '--set-string', 'externalZookeeper.servers=coord.example:2181',
                      '--set-string', 'externalBookkeeper.metadataServiceUri=zk://bk.example:2181/custom')
        sts = [d for d in docs if d['kind'] == 'StatefulSet']
        self.assertEqual(1, len(sts))
        config = find(docs, 'ConfigMap', '-chart-config')['data']['node.properties']
        self.assertIn('zookeeper.connect=coord.example:2181', config)
        self.assertIn('bookkeeper.metadataServiceUri=zk://bk.example:2181/custom', config)

    def test_dependency_name_override_is_used_in_clients(self):
        docs = render('--set', 'zookeeper.fullnameOverride=custom-zk')
        self.assertIn('zookeeper.connect=custom-zk.testing.svc:2181',
                      find(docs, 'ConfigMap', '-chart-config')['data']['node.properties'])

    def test_invalid_quorums_and_missing_external_endpoints_fail(self):
        self.assertIn('ensemble exceeds', render('--set', 'bookkeeper.replicas=1', success=False))
        self.assertIn('ack <= write', render('--set-string', 'node.quorums.wal=3/2/3', success=False))
        render('--set-string', 'node.quorums.wal=broken', success=False)
        self.assertIn('externalZookeeper.servers', render('--set', 'zookeeper.enabled=false', success=False))
        self.assertIn('externalBookkeeper.metadataServiceUri', render('--set', 'bookkeeper.enabled=false', success=False))

    def test_disabled_frontends_and_digest_override(self):
        digest = 'sha256:' + 'a' * 64
        docs = render('--set', 'gateway.enabled=false', '--set', 'admin.enabled=false',
                      '--set-string', f'image.digest={digest}')
        self.assertFalse(any(d['kind'] == 'Deployment' and
                             d['metadata']['name'].endswith(('-admin', '-gateway')) for d in docs))
        node = find(docs, 'StatefulSet', '-node')['spec']['template']['spec']['containers'][0]
        self.assertEqual('zetaplusae/candybox@' + digest, node['image'])

    def test_test_client_secret_and_ingress(self):
        docs = render('--set', 'test.envFrom[0].secretRef.name=cli-auth',
                      '--set', 'gateway.ingress.enabled=true')
        test = find(docs, 'Pod', '-test')['spec']['containers'][0]
        self.assertIn({'secretRef': {'name': 'cli-auth'}}, test['envFrom'])
        ingress = find(docs, 'Ingress', '-gateway')
        self.assertEqual(9711, ingress['spec']['rules'][0]['http']['paths'][0]['backend']['service']['port']['number'])


if __name__ == '__main__':
    unittest.main()
