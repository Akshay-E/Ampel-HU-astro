#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t3/TransientInfoDumper.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 15.08.2018
# Last Modified Date: 15.08.2018
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from ampel.base.TransientView import TransientView
from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.utils.json_serialization import AmpelEncoder, object_hook
import json
import requests
import uuid
from urllib.parse import urlencode, urlparse, urlunparse, ParseResult
from xml.etree import ElementTree
from io import StringIO, BytesIO
from gzip import GzipFile
from pydantic import BaseModel

def strip_auth_from_url(url):
	try:
		auth = requests.utils.get_auth_from_url(url)
		scheme, netloc, path, params, query, fragment = urlparse(url)
		netloc = netloc[netloc.index('@')+1:]
		url = urlunparse(ParseResult(scheme, netloc, path, params, query, fragment))
		return url, auth
	except KeyError:
		return url, None

def strip_path_from_url(url):
	scheme, netloc, path, params, query, fragment = urlparse(url)
	return urlunparse(ParseResult(scheme, netloc, '/', None, None, None))

class TransientViewDumper(AbsT3Unit):
	"""
	"""

	version = 0.1
	resources = ('desycloud.default',)

	class RunConfig(BaseModel):
		outputfile: str = None

	def __init__(self, logger, base_config=None, run_config=None, global_info=None):
		"""
		"""
		self.logger = logger
		self.count = 0
		self.config = run_config or self.RunConfig()
		if not self.config.outputfile:
			self.outfile = GzipFile(fileobj=BytesIO(), mode='w')
			self.path = '/AMPEL/dumps/' + str(uuid.uuid1()) + '.json.gz'
			url, auth = strip_auth_from_url(base_config['desycloud.default'])
			self.session = requests.Session()
			self.auth = auth
			self.webdav_base = url
			self.ocs_base = strip_path_from_url(url) + '/ocs/v1.php/apps/files_sharing/api/v1'
		else:
			self.outfile = GzipFile(self.config.outputfile+".json.gz", mode='w')
		# don't bother preserving immutable types
		self.encoder = AmpelEncoder(lossy=True)

	def add(self, transients):
		"""
		"""
		if transients is not None:

			batch_count = len(transients)
			self.count += batch_count

			for tran_view in transients:
				self.outfile.write(self.encoder.encode(tran_view).encode('utf-8'))
				self.outfile.write(b"\n")


	def done(self):
		"""
		"""
		self.outfile.flush()
		self.logger.info("Total number of transient printed: %i" % self.count)
		if self.config.outputfile:
			self.outfile.close()
			self.logger.info(self.config.outputfile+".json.gz")
		else:
			mb = len(self.outfile.fileobj.getvalue()) / 2.0 ** 20
			self.logger.info("{:.1f} MB of gzipped JSONy goodness".format(mb))
			self.session.put(self.webdav_base + self.path, data=self.outfile.fileobj.getvalue(), auth=self.auth).raise_for_status()
			response = self.session.post(self.ocs_base + '/shares',
			    data=dict(path=self.path, shareType=3),
			    auth=self.auth, 
			    headers={'OCS-APIRequest': 'true'} # i'm not a CSRF attack, i swear
			)
			self.outfile.close()
			if response.ok:
				public = ElementTree.fromstring(response.text).find('data/url').text
				self.logger.info(public)
			else:
				response.raise_for_status()
