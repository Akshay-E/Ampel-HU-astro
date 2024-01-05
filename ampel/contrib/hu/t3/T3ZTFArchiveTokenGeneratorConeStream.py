#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-ZTF/ampel/ztf/t3/resource/T3ZTFArchiveTokenGenerator.py
# License:             BSD-3-Clause
# Author:              Akshay Eranhalodi
# Date:                19.12.2023
# Last Modified Date:  19.12.2023
# Last Modified By:    akshay eranhalodi <firstname.lastname@desy.de>

import random
import time
from typing import Any
from astropy.time import Time  # type: ignore
from datetime import datetime

from requests_toolbelt.sessions import BaseUrlSession

from ampel.types import UBson
from ampel.struct.T3Store import T3Store
from ampel.struct.Resource import Resource
from ampel.struct.UnitResult import UnitResult
from ampel.secret.NamedSecret import NamedSecret
from ampel.abstract.AbsT3PlainUnit import AbsT3PlainUnit


class T3ZTFArchiveTokenGeneratorConeStream(AbsT3PlainUnit):

	archive_token: NamedSecret[str] = NamedSecret(label="ztf/archive/token")

	#: Base URL of archive service
	archive: str = "https://ampel.zeuthen.desy.de/api/ztf/archive/v3/"
	resource_name: str = 'ztf_stream_token'

	cone: None | dict[str, float] = None
	candidate: None | dict[str, Any] = None

	#: seconds to wait for query to complete
	timeout: float = 60

	debug: bool = False


	def process(self, t3s: T3Store) -> UBson | UnitResult:

		if self.cone:
			cone=self.cone

		if self.candidate:
			candidate = self.candidate
		else:
			candidate = {
				"rb": {"$gt": 0.3},
				"ndethist": {"$gt": 1},
				"isdiffpos": {"$in": ["t", "1"]}
			}


		session = BaseUrlSession(self.archive if self.archive.endswith("/") else self.archive + "/")
		session.headers["authorization"] = f"bearer {self.archive_token.get()}"

		response = session.post(
			"streams/from_query?programid=1",
			json = {
				"cone": cone,
				"candidate": candidate,
			}
		)

		rd = response.json()

		try:
			token = rd.pop("resume_token")
		except KeyError as exc:
			raise ValueError(f"Unexpected response: {rd}") from exc

		# wait for query to finish
		t0 = time.time()
		delay = 1
		while time.time() - t0 < self.timeout:
			response = session.get(f"stream/{token}")
			if response.status_code != 423:
				break
			time.sleep(random.uniform(0, delay))
			delay *= 2
		else:
			raise RuntimeError(f"{session.base_url}stream/{token} still locked after {time.time() - t0:.0f} s")
		response.raise_for_status()
		self.logger.info("Stream created", extra=response.json())

		r = Resource(name=self.resource_name, value=token)
		t3s.add_resource(r)

		if self.debug:
			return r.dict()

		return None
