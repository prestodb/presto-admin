from tests.product.mode_installers import StandaloneModeInstaller
from tests.product.prestoadmin_installer import PrestoadminInstaller
from tests.product.topology_installer import TopologyInstaller
from tests.product.standalone.presto_installer import StandalonePrestoInstaller


STANDALONE_BARE_CLUSTER = 'bare'
BARE_CLUSTER = 'bare'
STANDALONE_PA_CLUSTER = 'pa_only_standalone'
STANDALONE_PRESTO_CLUSTER = 'presto'

cluster_types = {
    BARE_CLUSTER: [],
    STANDALONE_PA_CLUSTER: [PrestoadminInstaller,
                            StandaloneModeInstaller],
    STANDALONE_PRESTO_CLUSTER: [PrestoadminInstaller,
                                StandaloneModeInstaller,
                                TopologyInstaller,
                                StandalonePrestoInstaller],
}
