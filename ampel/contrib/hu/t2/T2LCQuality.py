#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-contrib-HU/ampel/contrib/hu/t2/T2LCQuality.py
# License           : BSD-3-Clause
# Author            : matteo.giomi@desy.de
# Date              : 11.09.2018
# Last Modified Date: 05.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


import logging
from typing import Dict, List, Any
from astropy.table import Table
from scipy.interpolate import interp1d
from ampel.abstract.AbsT2Unit import AbsT2Unit
from ampel.view.LightCurve import LightCurve
logging.basicConfig()


class T2LCQuality(AbsT2Unit):
	"""
	determine the 'quality' of the light curve by computing ratios between
	the number of detection and that of upper limits.

	The LC 'quality' is measured by two numbers:
	* 'detection strenght' = n_det / n_obs
	* 'detection purity' = n_det / n_det + n_strong_ulims

	where:
	n_det:
		total number of detections

	n_obs:
		number of observations (detections + upper lims) computed
		from the time of the first detection.

	n_strong_ulims:
		number of upper limits which are below (higher magnitude) than
		what expected from a simple interpolation between the detections.

		That is, be interp_lc the function that iterpolates the detections
		(returning magnitude and accepting a time), an upper limit at
		time jd_ul of magnitude mag_ul is considered 'strong' if:
		interp_lc(jd_ul) < mag_ul

	NOTE that in the calculations of the strength, all the upper limits happening after
	the first detection are considered, while for the 'purity' metric, the default behaviour
	is to just consider ulims happening after the first, and before the last detection. This
	behaviour can be changed via the 'exclude_ulims' parameter of the run_config dictionary.
	"""

	filter_names: Dict = {1: 'g', 2: 'r', 3: 'i'}
	default_filter_ids: List[int] = [1, 2, 3]
	default_exclude_ulims_after: bool = True
	default_lc_filter: List[Dict[str, Any]] = [
		{
			'attribute': 'isdiffpos',
			'operator': '!=',
			'value': 'f'
		},
		{
			'attribute': 'isdiffpos',
			'operator': '!=',
			'value': '0'
		}
	]


	def count_strong_ulims(self, det_tab, ulim_tab):
		"""
		compute the number of strong upper limts in the light curve. This is
		defined as the number of upper limits which are below (higher magnitude) than
		what expected from a simple interpolation between the detections.
		"""

		# interpolate detections
		interp_lc = interp1d(det_tab['jd'], det_tab['mag'], kind="zero", fill_value="extrapolate")

		# loop on uls and count the strong ones
		n_strong = 0
		for ul in ulim_tab:
			expected_mag = interp_lc(ul['jd'])
#			self.logger.debug("upper limit at jd %f is at %f, source should be at %f"%
#				(ul['jd'], ul['mag'], expected_mag))
			if ul['mag'] > expected_mag:
				n_strong += 1
		return n_strong


	def compute_strength_purity(self, dets, ulims, exclude_ulims_after):
		"""
		given the detection and upper limit history, compute the
		strength and purity of the light curve.

		exclude_ul is a dict of the {'before': bool, 'after': bool} type, with
		flags can be used to mask out upper limts that happends before the
		first detections and/or after the last one.
		"""

		# compute time of first and last detection and mask out upper before first detection
		det_start, det_end = dets['jd'].min(), dets['jd'].max()
		ulims = ulims[ulims['jd'] > det_start]
		self.logger.debug(
			f"retained {len(ulims)} upper limits from start of detection (at {det_start} jd)"
		)

		# if you don't have any upper limit to consider, easy
		if len(ulims) == 0:
			return 0, 1, 1

		# compute number of detections, total observations, and upper limts.
		# for the strength, use all ulims from first detection on
		strength = float(len(dets)) / (len(dets) + len(ulims))

		# for the strong upper limits, eventually exclude those which happends after the last detection
		if exclude_ulims_after:
			ulims = ulims[ulims['jd'] < det_end]
		n_strong_ulims = self.count_strong_ulims(dets, ulims)
		purity = float(len(dets)) / (len(dets) + n_strong_ulims)

		# return
		return n_strong_ulims, strength, purity


	def test_plot(self, dets, ulims, n_strong_ulims, purity, strength, fid):
		"""
		but useful for debugging
		"""

		import matplotlib.pyplot as plt
		import numpy as np

		interp_lc = interp1d(dets['jd'], dets['mag'], kind='zero', fill_value="extrapolate")

		min_t = min([dets['jd'].min(), ulims['jd'].min()])
		max_t = max([dets['jd'].max(), ulims['jd'].max()])
		jd_int = np.arange(min_t, max_t, 0.1)
		plt.plot(dets['jd'], dets['mag'], 'o', label="data")
		plt.plot(ulims['jd'], ulims['mag'], 'o', label="ulims")
		plt.plot(jd_int, interp_lc(jd_int), label="interp")
		plt.gca().invert_yaxis()
		plt.legend()
		plt.xlabel("JD")
		plt.ylabel("Mag")

		# add text
		textstr = '$n_{det}$: %d, $n_{lim}$: %d, $n_{lim}^{strong}$: %d, purity: %.3f, strength: %.3f' % (
			len(dets), len(ulims), n_strong_ulims, purity, strength
		)
		plt.title("filter: %d " % fid + textstr)
		plt.show()


	def run(self, light_curve: LightCurve, run_config: Dict):
		"""
		:param run_config: `dict` or None
		configuration parameter for this job. If none is given, the
		default behaviour would be to compute the metrics for the light
		curve in all the three bands (if the corresponding light curves have
		some detection), to use zero-order (step-like) interpoaltion
		between the LC points, and to exclude points with negative detections
		(having isdiffpos in ['f', 0]).

		These defaults can be changed by the following keys of the run_config dictionary:

		lc_filter: `dict` or `list`
			to be passed to ampel.view.LightCurve.get_tuples.
			if list, the items must be dicts and they'll be combined
			with a logical and. Pass an empy list to disable the filter
			completely (filtering on the ztf bands will still be applied).

		filter_ids: `list` or `tuple`
			list of ints in the range 1 to 3 that specify the filter
			ids for which this job has to run. 1=g, 2=r, and 3=i

		exclude_ulims_after: `bool`
			specifies weather to consider upper limits that happens after
			the first last detection.

		:returns: dict with the strength, purity, and number of detections computed
		for the light curve in each of the band specified by the run_config
		(default is all of them). E.g.:
		{
			'g': {
				'strength': 0,
				'purity': 0,
				'ndet': 0
			},
			'r': {
				'strength': 1,
				'purity': 1,
				'ndet': 1
			},
			'i': {
				'strength': 0,
				'purity': 0,
				'ndet': 0
			}
		}
		"""

		# parse some run config params and use default if not given
		lc_filter = run_config.get('lc_filter')
		if lc_filter is None:
			lc_filter = self.default_lc_filter
		filter_ids = run_config.get('filter_ids')
		if filter_ids is None:
			filter_ids = self.default_filter_ids
		exclude_ulims_after = run_config.get('exclude_ulims_after')
		if exclude_ulims_after is None:
			exclude_ulims_after = self.default_exclude_ulims_after

		# run on the single bands individually
		out_dict = {}
		for fid in filter_ids:

			self.logger.debug(
				f"computing light curve quality for filter id {fid} "
				f"({self.filter_names[fid]}-band)"
			)

			# concatenate the filters
			filters: List[Dict[str, Any]] = [{'attribute': 'fid', 'operator': '==', 'value': fid}]
			if isinstance(lc_filter, (list, tuple)):
				filters += lc_filter
			elif isinstance(lc_filter, dict):
				filters += [lc_filter]
			else:
				raise ValueError(
					f"parameter 'lc_filter' must be either list or tuple. got {type(lc_filter)} instead"
				)

			self.logger.debug(f"applying filter: {repr(filters)}")

			# get upper limits and detections time series
			pps = light_curve.get_tuples('obs_date', 'magpsf', filters=filters)
			uls = light_curve.get_tuples('obs_date', 'diffmaglim', filters=filters, of_upper_limits=True)

			# if you have no detections, you're done
			if not pps:
				self.logger.debug("No detections in light curve for this band")
				out_dict[self.filter_names[fid]] = {'strength': 0, 'purity': 0, 'ndet': 0}
				continue

			# also easy
			if not uls:
				self.logger.debug("No upper limits in light curve for this band")
				out_dict[self.filter_names[fid]] = {'strength': 1, 'purity': 1, 'ndet': len(pps)}
				continue

			# cast to tables for convenience
			dets = Table(rows = pps, names=('jd', 'mag'))
			ulims = Table(rows = uls, names=('jd', 'mag'))
			self.logger.debug(
				f"got {len(dets)} detections and {len(ulims)} ulims for lightcurve"
			)

			# compute LC metrics and append to output
			n_strong_ulims, strength, purity = self.compute_strength_purity(dets, ulims, exclude_ulims_after)
			out_dict[self.filter_names[fid]] = {
				'strength': strength,
				'purity': purity,
				'ndet': len(dets)
			}

#			if len(dets)>5:
#				self.test_plot(dets, ulims, n_strong_ulims, purity, strength, fid)

		# return the info as dictionary
		return out_dict
