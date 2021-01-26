#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-contrib-HU/ampel/contrib/hu/t2/T2CatalogMatch.py
# License           : BSD-3-Clause
# Author            : matteo.giomi@desy.de
# Date              : 24.08.2018
# Last Modified Date: 26.01.2021
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any, Dict, Literal, Optional, Sequence, TYPE_CHECKING

from astropy.coordinates import SkyCoord
from astropy.table import Table
from extcats.catquery_utils import get_closest
from numpy import asarray, degrees

from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit
from ampel.content.DataPoint import DataPoint
from ampel.contrib.hu.base.CatsHTMUnit import CatsHTMUnit
from ampel.contrib.hu.base.ExtcatsUnit import ExtcatsUnit
from ampel.model.StrictModel import StrictModel
from ampel.t2.T2RunState import T2RunState
from ampel.type import T2UnitResult

if TYPE_CHECKING:
    from extcats.CatalogQuery import CatalogQuery


class CatalogModel(StrictModel):
    """
    :param use: either extcats or catsHTM, depending on how the catalog is set up.
    :param rs_arcsec: search radius for the cone search, in arcseconds
    :param catq_kwargs: parameter passed to the catalog query routine.

    In case 'use' is set to 'extcats', 'catq_kwargs' can (or MUST?) contain the names of the ra and dec
    keys in the catalog (see example below), all valid arguments to extcats.CatalogQuert.findclosest
    can be given, such as pre- and post cone-search query filters can be passed.

    In case 'use' is set to 'catsHTM', 'catq_kwargs' SHOULD contain the the names of the ra and dec
    keys in the catalog if those are different from 'ra' and 'dec' the 'keys_to_append' parameters
    is OPTIONAL and specifies which fields from the catalog should be returned in case of positional match:

    if not present: all the fields in the given catalog will be returned.
    if `list`: just take this subset of fields.

    Example (SDSS_spec):
    {
        'use': 'extcats',
        'catq_kwargs': {
            'ra_key': 'ra',
            'dec_key': 'dec'
        },
        'rs_arcsec': 3,
        'keys_to_append': ['z', 'bptclass', 'subclass']
    }

    Example (NED):
    {
        'use': 'catsHTM',
        'rs_arcsec': 20,
        'keys_to_append': ['fuffa1', 'fuffa2', ..],
        'catq_kwargs':
    }
    """
    use: Literal["extcats", "catsHTM"]
    rs_arcsec: float
    catq_kwargs: Dict[str, Any]
    keys_to_append: Optional[Sequence[str]]
    pre_filter: Optional[Dict[str, Any]]
    post_filter: Optional[Dict[str, Any]]


class T2CatalogMatch(ExtcatsUnit, CatsHTMUnit, AbsPointT2Unit):
    """
    Cross matches the position of a transient to those of sources in a set of catalogs
    """

    # run only on first datapoint by default
    # NB: this assumes that docs are created by DualPointT2Ingester
    ingest: Dict = {"eligible": {"pps": "first"}}

    # Each value specifies a catalog in extcats or catsHTM format and the query parameters
    catalogs: Dict[str, CatalogModel]

    def post_init(self):

        # dict for catalog query objects
        self.catq_objects = {}
        self.debug = self.logger.verbose > 1

    def init_extcats_query(self, catalog, **kwargs) -> "CatalogQuery":
        """
        Return the extcats.CatalogQuery object corresponding to the desired
        catalog. Repeated requests to the same catalog will not cause new duplicated
        CatalogQuery instances to be created.
        """

        # check if you have already init this peculiar query
        if not (catq := self.catq_objects.get(catalog)):
            catq = self.get_extcats_query(catalog, **kwargs)
            self.catq_objects[catalog] = catq
        return catq

    def run(self, datapoint: DataPoint) -> T2UnitResult:
        """
        :returns: example of a match in SDSS but not in NED:

        {
            'SDSS_spec': {
                'z': 0.08820018172264099,
                'bptclass': 2.0,
                'subclass': '',
                'dist2transient': 1.841666956181802e-09}
            },
            'NED': False
        }

        Note that, when a match is found, the distance of the lightcurve object
        to the catalog counterpart is also returned as the 'dist2transient' key.
        """

        try:
            transient_ra = datapoint["body"]["ra"]
            transient_dec = datapoint["body"]["dec"]
        except KeyError:
            return T2RunState.MISSING_INFO

        if self.debug:
            self.logger.debug(
                "Transient position (ra, dec): {transient_ra:.4f}, {transient_dec:.4f} deg"
            )

        # initialize the catalog quer(ies). Use instance variable to aviod duplicates
        out_dict: Dict[str, Any] = {}
        for catalog, cat_opts in self.catalogs.items():

            src, dist = None, None
            if self.debug:
                self.logger.debug(
                    f"Loading catalog {catalog} using options: {str(cat_opts)}"
                )

            # how do you want to support the catalog?
            if cat_opts.use == "extcats":

                # get the catalog query object and do the query
                catq = self.init_extcats_query(catalog, **cat_opts.catq_kwargs)

                src, dist = catq.findclosest(
                    transient_ra,
                    transient_dec,
                    cat_opts.rs_arcsec,
                    pre_filter=cat_opts.pre_filter,
                    post_filter=cat_opts.post_filter,
                )

            elif cat_opts.use == "catsHTM":

                # catshtm needs coordinates in radians
                transient_coords = SkyCoord(transient_ra, transient_dec, unit="deg")
                srcs, colnames, colunits = self.catshtm.cone_search(
                    catalog,
                    transient_coords.ra.rad,
                    transient_coords.dec.rad,
                    cat_opts.rs_arcsec,
                )

                if len(srcs) > 0:

                    # format to astropy Table
                    srcs_tab = Table(asarray(srcs), names=colnames)

                    # find out how ra/dec are called in the catalog
                    catq_kwargs = cat_opts.catq_kwargs
                    if catq_kwargs is None:
                        ra_key, dec_key = "ra", "dec"
                    else:
                        ra_key, dec_key = (
                            catq_kwargs.get("ra_key", "ra"),
                            catq_kwargs.get("dec_key", "dec"),
                        )

                    # get the closest source and its distance (catsHTM stuff is in radians)
                    srcs_tab[ra_key] = degrees(srcs_tab[ra_key])
                    srcs_tab[dec_key] = degrees(srcs_tab[dec_key])
                    src, dist = get_closest(
                        transient_coords.ra.degree,
                        transient_coords.dec.degree,
                        srcs_tab,
                        ra_key,
                        dec_key,
                    )
            else:
                raise ValueError(
                    f"use option can not be {cat_opts.use} for catalog {catalog}. "
                    f"valid are 'extcats' or 'catsHTM'"
                )

            if src is not None:
                if self.debug:
                    self.logger.debug(
                        f"found counterpart {dist:.2f} arcsec away from transient"
                    )
                # if you found a cp add the required field from the catalog:
                # if keys_to_append argument is given or if it is equal to 'all'
                # then take all the columns in the catalog. Otherwise only add the
                # requested ones.
                out_dict[catalog] = {"dist2transient": dist}

                keys_to_append = set(
                    cat_opts.keys_to_append if cat_opts.keys_to_append else src.colnames
                )
                if cat_opts.use == "extcats":
                    keys_to_append.difference_update({"_id", "pos"})

                if keys_to_append:
                    to_add = {}
                    for field in keys_to_append:
                        try:
                            val = src[field].tolist()
                        except AttributeError:
                            val = src[field]
                        except KeyError:
                            continue
                        to_add[field] = val
                    out_dict[catalog].update(to_add)
            else:
                if self.debug:
                    self.logger.debug(
                        f"no match found in catalog {catalog} within "
                        f"{cat_opts.rs_arcsec:.2f} arcsec from transient"
                    )
                out_dict[catalog] = False

        # return the info as dictionary
        return out_dict
