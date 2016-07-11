from tests.docker_cluster import DockerCluster
from tests.hdp_bare_image_provider import HdpBareImageProvider
from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider

from tests.product.base_product_case import BaseProductTestCase
from tests.product.cluster_types import STANDALONE_BARE_CLUSTER, STANDALONE_PA_CLUSTER, \
    STANDALONE_PRESTO_CLUSTER, YARN_SLIDER_PA_CLUSTER, cluster_types

import argparse


class ImageBuilder:
    def __init__(self, testcase):
        self.testcase = testcase
        self.testcase.default_keywords = {}
        self.testcase.cluster = None

    def _run_installers(self, installers):
        cluster = self.testcase.cluster
        for installer in installers:
            dependencies = installer.get_dependencies()

            for dependency in dependencies:
                dependency.assert_installed(self.testcase)

            installer_instance = installer(self.testcase)
            installer_instance.install()

            self.testcase.default_keywords.update(installer_instance.get_keywords())
            cluster.postinstall(installer)

    def _setup_image(self, bare_image_provider, cluster_type):
        installers = cluster_types[cluster_type]

        self.testcase.cluster, bare_cluster = DockerCluster.start_cluster(
            bare_image_provider, cluster_type)

        # If we got a bare cluster back, we need to run the installers on it.
        # applying the post-install hooks and updating the replacement
        # keywords is handled internally in _run_installers.
        #
        # If we got a non-bare cluster back, that means the image already exists
        # and we created the cluster using that image.
        if bare_cluster:
            self._run_installers(installers)

            if isinstance(self.testcase.cluster, DockerCluster):
                self.testcase.cluster.commit_images(bare_image_provider, cluster_type)

        self.testcase.cluster.tear_down()

    def _setup_image_with_no_hadoop_provider(self, cluster_type):
        self._setup_image(NoHadoopBareImageProvider(),
                          cluster_type)

    def setup_standalone_presto_images(self):
        cluster_type = STANDALONE_PRESTO_CLUSTER
        self._setup_image_with_no_hadoop_provider(cluster_type)

    def setup_standalone_presto_admin_images(self):
        cluster_type = STANDALONE_PA_CLUSTER
        self._setup_image_with_no_hadoop_provider(cluster_type)

    def setup_standalone_bare_images(self):
        cluster_type = STANDALONE_BARE_CLUSTER
        self._setup_image_with_no_hadoop_provider(cluster_type)

    def setup_yarn_slider_presto_admin_images(self):
        cluster_type = YARN_SLIDER_PA_CLUSTER
        self._setup_image_with_no_hadoop_provider(cluster_type)

    def _setup_cluster_with_hdp_image_provider(self, cluster_type):
        self._setup_image(HdpBareImageProvider(),
                          cluster_type)

    def setup_yarn_slider_presto_admin_and_slider_images(self):
        cluster_type = self.testcase.YARN_SLIDER_PA_AND_SLIDER_CLUSTER
        self._setup_cluster_with_hdp_image_provider(cluster_type)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("image_type", metavar="image_type", type=str, nargs="+",
                        choices=["standalone_presto", "standalone_presto_admin",
                                 "standalone_bare", "yarn_slider_presto_admin",
                                 "all"],
                        help="Specify the type of image to create. The available choices are: "
                             "standalone_presto, standalone_presto_admin, standalone_bare, "
                             "yarn_slider_presto_admin, all")

    # ImageBuilder needs an input testcase with access to unittest assertions
    # so the installers can check their resulting installations as well as some
    # product test helper functions.
    # This supplies a dummy testcase. BaseProductTestCase inherits from
    # unittest. A unittest instance can be successfully created if the name
    # of an existing method of the class is passed into the constructor.
    dummy_testcase = BaseProductTestCase('__init__')
    image_builder = ImageBuilder(dummy_testcase)

    args = parser.parse_args()
    if "all" in args.image_type:
        image_builder.setup_standalone_presto_images()
        image_builder.setup_standalone_presto_admin_images()
        image_builder.setup_standalone_bare_images()
        image_builder.setup_yarn_slider_presto_admin_images()
    else:
        if "standalone_presto" in args.image_type:
            image_builder.setup_standalone_presto_images()
        if "standalone_presto_admin" in args.image_type:
            image_builder.setup_standalone_presto_admin_images()
        if "standalone_bare" in args.image_type:
            image_builder.setup_standalone_bare_images()
        if "yarn_slider_presto_admin" in args.image_type:
            image_builder.setup_yarn_slider_presto_admin_images()
