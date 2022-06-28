# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2022
#
# This file is part of Barman.
#
# Barman is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Barman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.

import logging
import os
import shutil
import subprocess
import re
import sys

from barman.clients.cloud_compression import decompress_to_file
from barman.cloud import CloudInterface, DecompressingStreamingIO, DEFAULT_DELIMITER

try:
    # Python 3.x
    from urllib.parse import urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse

BASE_DIR = "/nsr/cache/cloudboost/barman"

class NetworkerInterface(CloudInterface):
    """
    This class implements CloudInterface for Networker
    It depends on the installation of the Networker Base Client 
    and the Networker Extended Client (mminfo)
    On Linux these are packaged as e.g. lgtoclnt and lgtoxtdclnt
    They are available for many platforms.

    """
    MAX_CHUNKS_PER_FILE = 1

    # Since there is only on chunk  min size is the same as max archive size
    MIN_CHUNK_SIZE = 1 << 40

    # As there is no documented limit for the networker maximum save set size,
    # we just leave the defaults in.
    # MAX_ARCHIVE_SIZE - we set a maximum of 1TB per file
    MAX_ARCHIVE_SIZE = 1 << 40

    def __init__(self, url, jobs=1, tags=None, server_name=None):
        """
        :param str url: Full URL of the cloud destination/source (ex: )
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 1.
        :param List[tuple] tags: List of tags as k,v tuples to be added to all
          uploaded objects
        :param str server_name: networker client name of this machine
        """
        self.bucket_name, self.path = self._parse_url(url)
        super().__init__(
            url=url,
            jobs=jobs,
            tags=tags,
        )
        self.bucket_exists = None
        self.server_name = server_name
        # self._reinit_session()

    def __del__(self):
        """
        cleanup the local staging area as best as we can. All previous points are too soon
        and will remove still needed files
        """
        command = os.path.basename(sys.argv[0])
        if 'restore' in command:
           shutil.rmtree(os.path.join(BASE_DIR,self.path,self.server_name), ignore_errors=True)
        if (os.path.isdir(BASE_DIR + BASE_DIR)):
           shutil.rmtree(BASE_DIR + BASE_DIR)

    @staticmethod
    def _parse_url(url):
        """
        Parse url and return bucket name and path. Raise ValueError otherwise.
        """
        if not url.startswith("nw://"):
            msg = "Networker storage URL {} is malformed. Expected format are '{}'".format(
                url,
                "nw://server-name",
            )
            raise ValueError(msg)
        parsed_url = urlparse(url)
        if not parsed_url.netloc:
            raise ValueError(
                "Storage URL {} is malformed. Server name not found".format(
                    url
                )
            )
        return parsed_url.netloc, parsed_url.path.strip("/")

    def _reinit_session(self):
        """
        Create a new session
        """
        self.container_client = self.bucket_name

    def test_connectivity(self):
        """
        Test gcs connectivity by trying to access a container
        """
        try:
            # We are not even interested in the existence of the bucket,
            # we just want to see if google cloud storage is reachable.
            self.bucket_exists = self._check_bucket_existence()
            return True
        except Exception as e:
            logging.error("Can't connect to Server")
            return False

    def _check_bucket_existence(self):
        """
        Check Server

        :return: True if the container exists, False otherwise
        :rtype: bool
        """
        logging.debug("_check_bucket_existence")
        response = subprocess.run(["ping", "-q", "-c 1", self.bucket_name], stdout=subprocess.DEVNULL)
        if response.returncode == 0:
           self._create_bucket()
           return True
        else:
           return False

    def _create_bucket(self):
        """
        Create a local temporary directory
        This will serve as a staging area for the backup files, until they're saved via the networker cmds.
        The optimal way would be to stream the data directly into networker, but there no api available that
        works in a usable compatible way for us.

        """
        logging.debug("_create_bucket")
        if not os.path.isdir(BASE_DIR):
           os.mkdir(BASE_DIR)
           os.chmod(BASE_DIR, 0o777)

    def list_bucket(self, prefix="", delimiter=DEFAULT_DELIMITER):
        """
        List networker saveset content in a directory manner

        :param str prefix: Prefix used to filter blobs
        :param str delimiter: Delimiter, used with prefix to emulate hierarchy
        :return: List of objects and dirs right under the prefix
        :rtype: List[str]
        """
        logging.debug("list_bucket: {}, {}".format(prefix, delimiter))
        client = prefix.replace(self.path + '/', "")
        client = client.split('/')[0]
        cp = subprocess.run(['mminfo', '-xc,', '-s', self.bucket_name, '-r', 'name,family', 
                             '-q', 'client=' + client], stdout=subprocess.PIPE,encoding='utf-8') 
        objects = []
        dirs    = []
        for line in cp.stdout.splitlines():
           (key,family) = line.split(',')
           if family != 'disk':
              continue
           if not re.match(prefix,key):
              continue
           objects.append(key)
           if delimiter == '':
               continue
           (dir,file) = os.path.split(key)
           dir += os.sep
           if dir not in dirs:
              dirs.append(dir)
        logging.debug("objects {}".format(objects))
        logging.debug("dirs {}".format(dirs))
        return objects + dirs

    def download_file(self, key, dest_path, decompress):
        """
        Download a file out of networker

        :param str key: The key identifying the file to download
        :param str dest_path: Where to put the destination file
        :param str|None decompress: Compression scheme to use for decompression
        """
        logging.debug("Cloud download of {} from {}".format(dest_path,key))
        client = key.replace(self.path + os.sep, "")
        client = client.split('/')[0]
        cp = subprocess.run(['mminfo', '-s', self.bucket_name, '-r', 'ssid', 
                             '-q', 'client=' + client + ',name=' + key], stdout=subprocess.PIPE,encoding='utf-8')
        if cp.returncode > 0 or not cp.stdout:
           logging.debug("Key: {} does not exist".format(key))
           return None
        cp = subprocess.run(['recover','-s', self.bucket_name, '-d', BASE_DIR, '-a',
                             '-S', cp.stdout.rstrip()], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, encoding='utf-8')
        blob_reader = None
        for s_line in cp.stdout.splitlines():
           if re.search(key + '$',s_line):
              s_line = re.sub('^\.', '', s_line)
              logging.debug ('Blob Path: ' + BASE_DIR + s_line)
              blob_reader = open(BASE_DIR + s_line, "rb")
        if blob_reader is None:
            logging.error("Networker saveset recovery failed." + cp.stdout)
            return
        with open(dest_path, "wb") as dest_file:
            os.chmod(dest_path, 0o666)
            if decompress is None:
                dest_file.write(blob_reader.read())
                return
            decompress_to_file(blob_reader, dest_file, decompress)
            blob_reader.close()
            os.remove(blob_reader.name)

    def remote_open(self, key, decompressor=None):
        """
        Recover remote objects from networker to local disk and return a readable stream

        :param str key: The key identifying the object to open
        :param barman.clients.cloud_compression.ChunkedCompressor decompressor:
          A ChunkedCompressor object which will be used to decompress chunks of bytes
          as they are read from the stream
        :return: google.cloud.storage.fileio.BlobReader | DecompressingStreamingIO | None A file-like object from which
          the stream can be read or None if the key does not exist
        """
        logging.debug("remote_open of {}".format(key))
        client = key.replace(self.path + '/', "")
        client = client.split('/')[0]
        cp = subprocess.run(['mminfo', '-s', self.bucket_name, '-r', 'ssid', 
                             '-q', 'client=' + client + ',name=' + key], stdout=subprocess.PIPE,encoding='utf-8')
        if cp.returncode > 0 or not cp.stdout:
           logging.debug("Key: {} does not exist".format(key))
           return None
        cp = subprocess.run(['recover','-s', self.bucket_name, '-d', BASE_DIR, '-a',
                             '-S', cp.stdout.rstrip()], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, encoding='utf-8')
        blob_reader = None
        for s_line in cp.stdout.splitlines():
           if re.search(key + '$',s_line):
              s_line = re.sub('^\.', '', s_line)
              logging.debug ('Blob Path: ' + BASE_DIR + s_line) 
              blob_reader = open(BASE_DIR + s_line, "rb")
        if blob_reader is None:
           logging.debug("Blob Stream not available." + cp.stdout)
           return None
        if decompressor:
            return DecompressingStreamingIO(blob_reader, decompressor)
        return blob_reader

    def upload_fileobj(self, fileobj, key, override_tags=None):
        """
        Create a stage file from a file-object stream and save it with networker.

        :param fileobj IOBase: File-like object to upload
        :param str key: The key to identify the uploaded object
        :param List[tuple] override_tags: List of tags as k,v tuples to be added to the
          uploaded object
        """
        tags = override_tags or self.tags
        logging.debug("upload_fileobj to {}".format(key))
        (dir,file) = os.path.split(key)

        dir = os.path.join(BASE_DIR, dir)
        oldmask=os.umask(000)
        os.makedirs(dir, mode=0o777)
        os.umask(oldmask)

        f = open(os.path.join(BASE_DIR, key), "wb")
        f.write(fileobj.read())
        f.close()

        args = ['save', '-q', '-b', self.path, '-N', key, os.path.join(BASE_DIR, key)]
        if tags is not None:
           for t in tags:
              if t[0] == 'nwargs':
                  args[2:2] = t[1].split(' ')
        subprocess.run(args, stderr=subprocess.DEVNULL, encoding='utf-8')

        if (os.path.isdir(dir)):
           shutil.rmtree(dir)

    def create_multipart_upload(self, key):
        """
        Networker does not support multipart savesets.
        So for now parallel upload is a simple upload.

        :param key: The key to use in the cloud service
        :return: The multipart upload metadata
        :rtype: dict[str, str]|None
        """
        return []

    def _upload_part(self, upload_metadata, key, body, part_number):
        """
        Upload a file

        The part metadata will included in a list of metadata for all parts of
        the upload which is passed to the _complete_multipart_upload method.

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        :param object body: A stream-like object to upload
        :param int part_number: Part number, starting from 1
        :return: The part metadata
        :rtype: dict[str, None|str]
        """
        self.upload_fileobj(body, key)
        return {
            "PartNumber": part_number,
        }

    def _complete_multipart_upload(self, upload_metadata, key, parts_metadata):
        """
        Finish a certain multipart upload
        There is nothing to do here as we are not using multipart.

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        :param List[dict] parts_metadata: The list of metadata for the parts
          composing the multipart upload. Each part is guaranteed to provide a
          PartNumber and may optionally contain additional metadata returned by
          the cloud provider such as ETags.
        """
        pass

    def _abort_multipart_upload(self, upload_metadata, key):
        """
        Abort a certain multipart upload

        The implementation of this method should clean up any dangling resources
        left by the incomplete upload.

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        """
        # Probably delete things here in case it has already been uploaded ?
        # Maybe catch some exceptions like file not found (equivalent)
        self.delete_objects(key)

    def delete_objects(self, paths):
        """
        Delete the objects at the specified paths
        :param List[str] paths:
        """
        client = prefix.replace(self.path + '/', "")
        client = client.split('/')[0]
        failures = {}
        for path in list(set(paths)):
            cp = subprocess.run(['mminfo', '-s', self.bucket_name, '-r', 'ssid', 
                             '-q', 'client=' + client + ',name=' + path], stdout=subprocess.PIPE,encoding='utf-8')
            if cp.returncode > 0 or not cp.stdout:
                logging.debug("Key: {} does not exist".format(path))
                failures[path] = ['Saveset does not exist', path]
                continue
            cp = subprocess.run(['nsrmm','-s', self.bucket_name, '-dy',
                 '-S', cp.stdout.rstrip()], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, encoding='utf-8')
            if cp.returncode > 0:
                logging.debug("Key: {} delete failed".format(path))
                failures[path] = ['Saveset delete failed', path]

        if failures:
            logging.error(failures)
            raise RuntimeError("Could not delete all networker savesets")
