#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t2/TagAgn.py
# License           : BSD-3-Clause
# Author            : matteo.giomi@desy.de
# Date              : 24.08.2018
# Last Modified Date: 24.08.2018
# Last Modified By  : matteo.giomi@desy.de

import logging
from pymongo import MongoClient
from astropy.coordinates import SkyCoord
from astropy.table import Table
from urllib.parse import urlparse

from ampel.base.abstract.AbsT2Unit import AbsT2Unit
from ampel.core.flags.T2RunStates import T2RunStates

from extcats import CatalogQuery
from extcats.catquery_utils import get_closest
from catsHTM import cone_search



class T2CatalogMatch(AbsT2Unit):
	"""
		cross match the position of a transient to those of sources in a set
		of catalogs and attach the required information to the transient.
	"""
	
	version = 0.1
	resources = ('extcats.reader', 'catsHTM.default')

	def __init__(self, logger, base_config):
		"""
		"""
		
		self.logger = logger if logger is not None else logging.getLogger()
		self.base_config = {} if base_config is None else base_config
		
		# empty dict of suppoerted (AS WELL AS REQUESTED) extcats catalog query objects
		self.catq_objects = {}
		
		# initialize the catsHTM paths and the extcats query client.
		self.catshtm_path 			= urlparse(base_config['catsHTM.default']).path
		self.catq_client 			= MongoClient(base_config['extcats.reader'])
		self.catq_kwargs_global 		= {
										'logger': self.logger,
										'dbclient': self.catq_client,
										'ra_key': 'ra',
										'dec_key': 'dec'
										}
		
		# default parameters for LightCurve.get_pos method
		self.lc_get_pos_defaults = {'ret': "brightest", 'filters': None}
		
		# mandatory keys
		self.mandatory_keys = ['use', 'rs_arcsec', 'keys_to_append']
		
		
	def run(self, light_curve, run_config):
		""" 
			Parameters
			-----------
				light_curve: "ampel.base.LightCurve" instance. 
				 See the LightCurve docstring for more info.
			
				run_parameters: `dict`
						configuration parameter for this job. There is provision to
						pass arguments to the LightCurve.get_pos method used to derive
						the position of the transient from the lightcure. 
						Most importantly, the catalogs key correspond to a nested dictionary
						in which each entry specify a catalog in extcats or catsHTM format 
						and the parameters used for the queries. Eg:
				
						run_config = 
							{
							'get_lc_pos_kwargs': None, # optional see ampel.base.LightCurve doc
							'catalogs':
								{
								'sdss_spec': 
									{
										'use': 'extcats',
										'catq_kwargs': {
														'ra_key': 'ra',
														'dec_key': 'dec'
													},
										'rs_arcsec': 3,
										'keys_to_append': ['z', 'bptclass', 'subclass', 'dist2alert']
									},
								'NED': 
									{
										'use': 'catshtm',
										'rs_arcsec': 20,
										'keys_to_append': ['fuffa1', 'fuffa2', ..],
										'catq_kwargs': {
														'ra_key': 'RAJ2000',
														'dec_key': 'DECJ2000'
													},
									},
								...
								}
							}
			
			Returns
			-------
				dict with the keys to append to each transient.
		"""
		try:
			return self._run_(light_curve, run_config)
		except:
			self.logger.error("Exception occured", exc_info=1)
			return T2RunStates.EXCEPTION

	
	def init_extcats_query(self, catalog, catq_kwargs=None):
		"""
			Return the extcats.CatalogQuery object corresponding to the desired
			catalog. Repeated requests to the same catalog will not cause new duplicated
			CatalogQuery instances to be created.
			
			Returns:
			--------
				
				extcats.CatalogQuery instance.
			
		"""
		
		# check if the catalog exist as an extcats database
		if not catalog in self.catq_client.database_names():
			raise ValueError("cannot find %s among installed extcats catalogs"%(catalog))
		
		# check if you have already init this peculiar query
		catq = self.catq_objects.get(catalog)
		if catq is None:
			self.logger.debug("CatalogQuery object not previously instantiated. Doing it now.")
			
			# add catalog specific arguments to the general ones
			if catq_kwargs is None:
				catq_kwargs = self.catq_kwargs_global
			else:
				merged_kwargs = self.catq_kwargs_global.copy()
				merged_kwargs.update(catq_kwargs)
			self.logger.debug("Using arguments: %s", merged_kwargs)
			
			# init the catalog query and remember it
			catq = CatalogQuery.CatalogQuery(catalog, **merged_kwargs)
			self.catq_objects[catalog] = catq
			return catq
		else:
			self.logger.debug("CatalogQuery object for catalog %s already exists."%catalog)
			return catq

	def _run_(self, light_curve, run_config):
		""" 
		refer to docstring of run method.
		"""
		
		# get ra and dec from lightcurve object
		lc_get_pos_kwargs = run_config.get('lc_get_pos_kwargs')
		if lc_get_pos_kwargs is None:
			lc_get_pos_kwargs = self.lc_get_pos_defaults
		self.logger.debug("getting transient position from lightcurve using args: %s", lc_get_pos_kwargs)
		transient_ra, transient_dec = light_curve.get_pos(**lc_get_pos_kwargs)
		self.logger.debug("Transient position (ra, dec): %.4f, %.4f deg"%(transient_ra, transient_dec))
		
		# initialize the catalog quer(ies). Use instance variable to aviod duplicates
		out_dict = {}
		catalogs = run_config.get('catalogs')
		for catalog, cat_opts in catalogs.items():
			src, dist = None, None
			self.logger.info("Loading catalog %s using options:"%catalog, cat_opts)
			
			# check options:
			for opt_key in self.mandatory_keys:
				if not opt_key in cat_opts.keys():
					raise KeyError("options for catalog %s are missing mandatory %s argument. Check your run config."%
						(catalog, opt_key))
			
			# how do you want to support the catalog?
			use = cat_opts.get('use')
			if use == 'extcats':
				
				# get the catalog query object and do the query
				catq = self.init_extcats_query(catalog, catq_kwargs=cat_opts.get('catq_kwargs'))
				src, dist = catq.findclosest(transient_ra, transient_dec, cat_opts['rs_arcsec'])
				
			elif use == 'catsHTM':
				
				# catshtm needs coordinates in radians
				transient_coords = SkyCoord(transient_ra, transient_dec, unit='deg')
				srcs, colnames, colunits = cone_search(
													catalog,
													transient_coords.ra.rad, transient_coords.dec.rad,
													cat_opt['rs_arcsec'],
													catalogs_dir=self.catshtm_path)
				if len(srcs) > 0:
					
					# format to astropy Table
					srcs_tab = Table(srcs, names=colnames)
					
					# find out how ra/dec are called in the catalog
					catq_kwargs = cat_opts.get('catq_kwargs')
					if catq_kwargs is None:
						ra_key, dec_key = 'ra', 'dec'
					else:
						ra_key, dec_key = catq_kwargs.get('ra_key', 'ra'), catq_kwargs.get('dec_key', 'dec')
					
					# get the closest source and its distance
					src, dist = get_closest(transient_ra, transient_dec, srcs_tab, ra_key, dec_key)
			else:
				raise ValueError("use option can not be %s for catalog %s"%(use, catalog))
			
			# now add the results to the output dictionary
			if not src is None:
				
				# if you found a cp add the required field from the catalog
				self.logger.debug("found counterpart %.2f arcsec away from transient."%dist)
				for field in cat_opts['keys_to_append']:
					out_dict[catalog+"_"+field] = src[field]
				out_dict[catalog+'_dist2transient'] = dist
			else:
				out_dict[catalog] = "n/a"
			
		# return the info as dictionary
		return out_dict
