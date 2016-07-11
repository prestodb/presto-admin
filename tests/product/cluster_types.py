from tests.product.mode_installers import StandaloneModeInstaller, \
    YarnSliderModeInstaller
from tests.product.prestoadmin_installer import PrestoadminInstaller
from tests.product.topology_installer import TopologyInstaller
from tests.product.yarn_slider.slider_installer import SliderInstaller
from tests.product.standalone.presto_installer import StandalonePrestoInstaller


STANDALONE_BARE_CLUSTER = 'bare'
BARE_CLUSTER = 'bare'
STANDALONE_PA_CLUSTER = 'pa_only_standalone'
STANDALONE_PRESTO_CLUSTER = 'presto'

YARN_SLIDER_PA_CLUSTER = 'pa_only_ys'
YARN_SLIDER_PA_AND_SLIDER_CLUSTER = 'pa_slider'

cluster_types = {
    BARE_CLUSTER: [],
    STANDALONE_PA_CLUSTER: [PrestoadminInstaller,
                            StandaloneModeInstaller],
    STANDALONE_PRESTO_CLUSTER: [PrestoadminInstaller,
                                StandaloneModeInstaller,
                                TopologyInstaller,
                                StandalonePrestoInstaller],
    YARN_SLIDER_PA_CLUSTER: [PrestoadminInstaller,
                             YarnSliderModeInstaller],
    YARN_SLIDER_PA_AND_SLIDER_CLUSTER: [PrestoadminInstaller,
                                        YarnSliderModeInstaller,
                                        SliderInstaller]
}
