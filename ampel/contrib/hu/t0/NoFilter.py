#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-contrib-HU/ampel/contrib/hu/t0/NoFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 05.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AbsPhotoAlertFilter import AbsPhotoAlertFilter

class NoFilter(AbsPhotoAlertFilter):

	def apply(self, alert):
		return self.on_match_default_t2_units
