# -*- coding: utf-8 -*-

import fnmatch
import getpass
import os

from prestoadmin import main_dir

CONFIG_FILE_GLOB = r'*.yaml'
user_password = ''


def check_for_cluster_config():
    config_name = fnmatch.filter(os.listdir(main_dir), CONFIG_FILE_GLOB)
    if config_name:
        return config_name[0]
    else:
        return None


def setup_package():
    if check_for_cluster_config():
        global user_password
        user_password = getpass.getpass('Password for cluster: ')
