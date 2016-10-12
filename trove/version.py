# Copyright 2011 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

class VersionInfo(object):
    def __init__(self, package):
        self.package = package
        self.version = None

    def __str__(self):
        return self.version_string()

    def cached_version_string(self):
        return self.version_string()

    def version_string(self):
        return "1.9.3"

version_info = VersionInfo('trove')
