# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import base64
import getpass
import json
import os
import re
import subprocess

from util import __version__
from util.http import send_get_request, send_authorized_post_request
from util.semantic_version import SemanticVersion

try:
    from setuptools import Command
except ImportError:
    from distutils.core import Command

GITHUB_REPOSITORY_API_PATH = 'https://api.github.com/repos/prestodb/presto-admin'
CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))


class ReleaseFetcher:
    def __init__(self, directory, github_api_path):
        self.directory = directory
        self.github_api_path = github_api_path
        self.release_validator = ReleaseValidator(directory)

    def get_latest_release(self):
        headers, contents = send_get_request(self.github_api_path + '/releases/latest')
        return json.loads(contents)

    def _get_remote_branches(self):
        headers, contents = send_get_request(self.github_api_path + '/branches')
        return json.loads(contents)

    def _get_current_branch(self):
        return subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], cwd=self.directory).strip()

    def _get_last_remote_commit(self, branch):
        headers, contents = send_get_request(self.github_api_path + '/commits/' + branch)
        return json.loads(contents)

    def _get_last_local_commit(self):
        return subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=self.directory).strip()

    def _get_latest_tag(self):
        latest_release = self.get_latest_release()
        return latest_release['tag_name']

    def get_requested_release_tag(self):
        release_note_docs = self._get_all_release_note_docs()
        release_note_names = [os.path.splitext(release_note_doc)[0] for release_note_doc in release_note_docs]
        versions = [SemanticVersion(release_note_name.split('-')[1]) for release_note_name in release_note_names]
        latest_version_number = sorted(versions, reverse=True)[0]
        return str(latest_version_number)

    @staticmethod
    def _is_valid_release_doc_name(release_doc_name):
        return re.match('^release-[0-9]+(\.[0-9]+){0,2}\.rst$', release_doc_name)

    def _get_all_release_note_docs(self):
        release_docs_directory = os.path.join(self.directory, 'docs/release/')
        return [release_doc_name for release_doc_name in os.listdir(release_docs_directory)
                if (os.path.isfile(os.path.join(release_docs_directory, release_doc_name)) and
                    ReleaseFetcher._is_valid_release_doc_name(release_doc_name))]

    @staticmethod
    def _find_nth(haystack, needle, n):
        start = haystack.find(needle)
        while start >= 0 and n > 1:
            start = haystack.find(needle, start+1)
            n -= 1
        return start

    def get_body_from_release_notes(self, tag_name):
        release_notes_file_path = os.path.join(self.directory, 'docs/release/release-%s.rst' % tag_name)
        with open(release_notes_file_path, 'r') as release_notes_file:
            release_notes = release_notes_file.read()
            release_notes_without_header = release_notes.strip()[ReleaseFetcher._find_nth(release_notes, '\n', 3):]
            return release_notes_without_header.strip()

    def _get_and_check_branch(self):
        current_local_branch = self._get_current_branch()
        ReleaseValidator.check_branch_remote_exists(current_local_branch, self._get_remote_branches())
        return current_local_branch

    def get_and_check_target_commitish(self):
        self.release_validator.check_repo()
        branch = self._get_and_check_branch()
        last_remote_commit = self._get_last_remote_commit(branch)['sha']
        last_local_commit = self._get_last_local_commit()
        ReleaseValidator.check_commit(last_local_commit, last_remote_commit)
        return last_remote_commit

    def get_and_check_tag(self):
        """
        This functions finds the requested release tag by looking at the names of the
        release documents. It checks that the requested release tag is an acceptable bump
        from the latest release tag.
        """
        latest_tag = self._get_latest_tag()
        requested_release_tag = self.get_requested_release_tag()
        ReleaseValidator.check_tag(latest_tag, requested_release_tag)
        return requested_release_tag


class ReleaseValidator:
    def __init__(self, directory):
        self.directory = directory

    def check_repo(self):
        if subprocess.check_output(['git', 'status', '--porcelain'], cwd=self.directory).strip():
            exit('Repository is not clean. Commit or stash all changes')
        else:
            print 'Repository is clean'

    @staticmethod
    def check_branch_remote_exists(local_branch_name, remote_branches):
        for remote_branch in remote_branches:
            if local_branch_name == remote_branch['name']:
                print 'Local branch %s exists remotely' % local_branch_name
                return
        exit('Local branch %s does not exist remotely' % local_branch_name)

    @staticmethod
    def check_tag(latest_tag, requested_release_tag):
        print 'The latest release tag is %s.\n' \
              'Detected requested release tag: %s' \
              % (latest_tag, requested_release_tag)

        latest_version = SemanticVersion(latest_tag)
        acceptable_tags = latest_version.get_acceptable_version_bumps()
        if requested_release_tag not in acceptable_tags:
            exit('Detected release tag %s is not part of the acceptable release tags: %s'
                 % (requested_release_tag, acceptable_tags))

    @staticmethod
    def check_commit(last_local_commit, last_remote_commit):
        if last_remote_commit != last_local_commit:
            exit('Last local and remote commits do not match')
        else:
            print 'Last local and remote commits match'

    @staticmethod
    def _get_and_check_release_file(file_path, string_contained=None, string_begins=None):
        with open(file_path, 'r') as release_file:
            file_contents = release_file.read()
            if string_contained:
                if string_contained not in file_contents:
                    exit('Expected "%s" to be in %s' % (string_contained, file_path))
            if string_begins:
                if not file_contents.startswith(string_begins):
                    print file_contents
                    exit('Expected %s to begin with "%s"' % (file_path, string_contained))

            return file_contents

    @staticmethod
    def _confirm_version_changed(tag_name):
        if __version__ != tag_name:
            exit('Version in prestoadmin/_version is %s, but expected %s' % (__version__, tag_name))

    def _confirm_release_docs_format(self, tag_name):
        """
        This function checks the format of the release documents.
        It checks the release document to make sure it has a header and that the
        release document name has been added to the file with the list of releases.
        """
        release_doc_name = 'release-' + tag_name + '.rst'
        release_doc_path = os.path.join(self.directory, 'docs/release', release_doc_name)
        release_doc_header = 'Release ' + tag_name
        release_doc_header = ('=' * len(release_doc_header)) + '\n' + release_doc_header + '\n' + \
                             ('=' * len(release_doc_header)) + '\n'
        ReleaseValidator._get_and_check_release_file(release_doc_path,
                                                     string_begins=release_doc_header)

        string_contained = 'release/release-' + tag_name
        release_list_doc_path = os.path.join(self.directory, 'docs/release.rst')
        ReleaseValidator._get_and_check_release_file(release_list_doc_path,
                                                     string_contained=string_contained)

        print 'Release docs confirmed for tag %s' % tag_name

    def confirm_all_release_file_changes(self, tag_name):
        ReleaseValidator._confirm_version_changed(tag_name)
        self._confirm_release_docs_format(tag_name)


class GithubReleaser:
    def __init__(self, directory, github_api_path):
        self.directory = directory
        self.github_api_path = github_api_path
        self.release_fetcher = ReleaseFetcher(directory, github_api_path)
        self.release_validator = ReleaseValidator(directory)
        self.username = None
        self.password = None
        self.tag_name = None
        self.release_name = None
        self.target_commitish = None
        self.name = None
        self.body = None
        self.is_draft = 'false'
        self.is_prerelease = 'false'

    def _prompt_username(self):
        self.username = raw_input('Please input your Github username: ')

    def _prompt_password(self):
        self.password = getpass.getpass("Enter password for '%s': " % self.username)

    def _get_authorization_string(self):
        self._prompt_username()
        self._prompt_password()
        return base64.standard_b64encode('%s:%s' % (self.username, self.password))

    def _check_and_set_release_fields(self):
        """
        This functions checks that files have been added and/or modified for the release.
        It sets the fields necessary to release to Github.
        """
        self.target_commitish = self.release_fetcher.get_and_check_target_commitish()
        self.tag_name = self.release_fetcher.get_and_check_tag()
        self.release_validator.confirm_all_release_file_changes(self.tag_name)
        self.body = self.release_fetcher.get_body_from_release_notes(self.tag_name)
        self.body = GithubReleaser._escape_newlines(self.body)
        self.release_name = 'Release ' + self.tag_name

    @staticmethod
    def _escape_newlines(multiline_string):
        return multiline_string.replace('\n', '\\n')

    def _build_json_post_contents(self):
        return '{"tag_name": "%s", "target_commitish": "%s", "name": "%s", "body": "%s",' \
               ' "draft": %s, "prerelease": %s}' \
               % (self.tag_name, self.target_commitish, self.release_name,
                  self.body, self.is_draft, self.is_prerelease)

    @staticmethod
    def _send_github_create_release_post_request(url, json_data, authorization_string):
        send_authorized_post_request(url, json_data, authorization_string, 'application/json', len(json_data))
        print 'Successfully created Github release'

    @staticmethod
    def _send_bztar_post_request(url, bztar_data, authorization_string, content_length):
        send_authorized_post_request(url, bztar_data, authorization_string, 'application/octet-stream', content_length)

    def _send_installer_post_request(self, release_url, installer_name, authorization_string, command_args):
        installer_path = os.path.join(self.directory, 'dist/', installer_name)
        with open(os.devnull, 'w') as dev_null:
            subprocess.check_call(command_args, stdout=dev_null, stderr=dev_null)
        with open(installer_path, mode='rb') as online_installer:
            GithubReleaser._send_bztar_post_request('%s?name=%s' % (release_url, installer_name),
                                                    online_installer,
                                                    authorization_string,
                                                    os.path.getsize(installer_path))
        print 'Successfully posted %s' % installer_name

    def _send_online_installer_post_request(self, release_url, online_install_name, authorization_string):
        self._send_installer_post_request(release_url, online_install_name, authorization_string,
                                          ['make', 'dist-online'])

    def _send_offline_installer_post_request(self, release_url, offline_install_name, authorization_string):
        self._send_installer_post_request(release_url, offline_install_name, authorization_string,
                                          ['make', 'dist-offline'])

    def _send_github_release_posts(self, json_data):
        # Creating a release:
        # https://developer.github.com/v3/repos/releases/#create-a-release
        authorization_string = self._get_authorization_string()
        GithubReleaser._send_github_create_release_post_request(self.github_api_path + '/releases',
                                                                json_data, authorization_string)

        latest_release = self.release_fetcher.get_latest_release()
        release_tag = latest_release['tag_name']
        # Each release has an associated upload url that allows it to link to other resources:
        # https://developer.github.com/v3/#hypermedia
        release_url = latest_release['upload_url'].split('{')[0]

        # The expected names of the online and offline installers
        online_install_name = 'prestoadmin-%s-online.tar.gz' % release_tag
        offline_install_name = 'prestoadmin-%s-offline.tar.gz' % release_tag

        # Upload release assets:
        # https://developer.github.com/v3/repos/releases/#upload-a-release-asset
        self._send_online_installer_post_request(release_url, online_install_name, authorization_string)
        self._send_offline_installer_post_request(release_url, offline_install_name, authorization_string)
        print 'Successfully created release and uploaded assets to Github'

    def check_and_create_new_github_release(self):
        print '\nCreating a new Github release'
        self._check_and_set_release_fields()

        json_post_contents = self._build_json_post_contents()
        self._send_github_release_posts(json_post_contents)


class PypiReleaser:
    def __init__(self, directory, github_api_directory):
        self.directory = directory
        self.github_api_directory = github_api_directory
        self.release_fetcher = ReleaseFetcher(directory, github_api_directory)
        self.release_validator = ReleaseValidator(directory)

    def _confirm_pypi_release_state(self):
        self.release_fetcher.get_and_check_target_commitish()
        requested_release_tag = self.release_fetcher.get_requested_release_tag()
        self.release_validator.confirm_all_release_file_changes(requested_release_tag)

    @staticmethod
    def _check_pypi_success(output):
        if 'Server response (200): OK' in output:
            return True
        else:
            return False

    def _run_pypi_command(self, command):
        try:
            output = subprocess.check_output(command, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print e.output
            raise
        if PypiReleaser._check_pypi_success(output):
            return True
        else:
            print output
            return False

    def _check_pypi_setup(self):
            command = ['python', 'setup.py', 'register', '-r', 'pypi']
            if self._run_pypi_command(command):
                print 'Setup correctly for Pypi release'
                return
            else:
                exit('Not setup correctly for Pypi release')

    def _submit_pypi_release(self):
        command = ['python', 'setup.py', 'bdist_wheel', 'upload', '-r', 'pypi']
        if self._run_pypi_command(command):
            print 'Released successfully to Pypi'
            return
        else:
            exit('Failed to release to Pypi')

    def create_new_pypi_release(self):
        print '\nCreating a new Pypi release'
        self._confirm_pypi_release_state()
        self._check_pypi_setup()
        self._submit_pypi_release()


class release(Command):
    description = 'create release to github and/or pypi'

    user_options = [('github', None,
                     'boolean flag indicating if a release should be created for github'),
                    ('pypi', None,
                     'boolean flag indicating if a release should be created for pypi'),
                    ('all', None,
                     'boolean flag indicating if a release should be created for github and pypi')]

    def initialize_options(self):
        self.github = False
        self.pypi = False
        self.all = True

    def finalize_options(self):
        if self.github or self.pypi:
            self.all = False

    def run(self):
        github_releaser = GithubReleaser(CURRENT_DIRECTORY, GITHUB_REPOSITORY_API_PATH)
        pypi_releaser = PypiReleaser(CURRENT_DIRECTORY, GITHUB_REPOSITORY_API_PATH)
        if self.all:
            github_releaser.check_and_create_new_github_release()
            pypi_releaser.create_new_pypi_release()
        else:
            if self.github:
                github_releaser.check_and_create_new_github_release()
            if self.pypi:
                pypi_releaser.create_new_pypi_release()
        print 'Now might be a good time to update the version to SNAPSHOT'
