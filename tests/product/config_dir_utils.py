import os

from prestoadmin.util.constants import COORDINATOR_DIR_NAME, WORKERS_DIR_NAME, CATALOG_DIR_NAME


# gets the information for presto-admin config directories on the cluster
def get_config_directory():
    return os.path.join('~', '.prestoadmin')


def get_config_file_path():
    return os.path.join(get_config_directory(), 'config.json')


def get_coordinator_directory():
    return os.path.join(get_config_directory(), COORDINATOR_DIR_NAME)


def get_workers_directory():
    return os.path.join(get_config_directory(), WORKERS_DIR_NAME)


def get_catalog_directory():
    return os.path.join(get_config_directory(), CATALOG_DIR_NAME)


def get_log_directory():
    return os.path.join(get_config_directory(), 'log')


def get_mode_config_path():
    return os.path.join(get_config_directory(), 'mode.json')


def get_install_directory():
    return os.path.join('~', 'prestoadmin')


def get_presto_admin_path():
    return os.path.join(get_install_directory(), 'presto-admin')
