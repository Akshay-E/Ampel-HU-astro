#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                ampel/contrib/hu/t2/T2KilonovaEval.py
# License:             BSD-3-Clause
# Author:              jnordin@physik.hu-berlin.de
# Date:                29.03.2023
# Last Modified Date:  29.03.2023
# Last Modified By:    jnordin@physik.hu-berlin.de

import numpy as np
from typing import Any, Literal, Union
from collections.abc import Sequence
from astropy.coordinates import Distance, SkyCoord
from astropy.cosmology import Planck15

from ampel.types import UBson
from ampel.struct.UnitResult import UnitResult
from ampel.view.LightCurve import LightCurve
from ampel.view.T2DocView import T2DocView
from ampel.abstract.AbsTiedLightCurveT2Unit import AbsTiedLightCurveT2Unit
from ampel.model.StateT2Dependency import StateT2Dependency

class T2KilonovaEval(AbsTiedLightCurveT2Unit):
    """
    Evaluate whether a transient fulfills criteria for being a potential
    kilonova-like event.

    Could include evaluations based on (if present):
    - Lightcurve directly.
    - Redshift / distance to core.
    - SNguess.
    - Healpix map probability.
    - Sncosmo fits.
    - Parsnip fits.
    - Possis fits
    
    Will combine evaluation into a "kilonovaness" grade. 
    

    """

    # Which units should this be changed to
    t2_dependency: Sequence[StateT2Dependency[Literal[
    	"T2DigestRedshifts", 
    	"T2RunPossis", 
    	"T2PropagateStockInfo"
    	]]]


    # Evaluation sections
    
    # Distance
    max_redshift: float = 0.05     # Max 
    min_redshift: float = 0.001
    min_dist: float = 1.5      # Min arcsec distance (remove core variables)
    max_dist: float = 50       # Max arcsec distance 
    max_kpc_dist: float = 999  # Max distance in kpc (using redshift)
    max_redshift_uncertainty: float = 999

    # Lightcurve (using redshift, so that needs to be there)
    min_absmag: float = -20 
    max_absmag: float = -12  #  
    min_ndet: int = 1
    min_ndet_postul: int = 0  # and if it has this minimum nr of detection after the last significant (max_maglim) UL.
    min_age: float = 0
    max_age: float = 3.
    # Min age of detection history
    # range of peak magnitudes for submission
    min_peak_mag: float = 20
    max_peak_mag: float = 16
    # Reported detections in at least this many filters
    min_n_filters: int = 1
    # Require a detection in one of these filters (e.g. ZTF I-band more often spurious)
    det_filterids: list[int] = [1, 2, 3]   # default to any of them
    # Below are copied from filter - not sure is needed, but kept for legacy
    # Minimal galactic latitide
    min_gal_lat: float = 14
    # reject alert if ssdistnr smaller than this value for any pp
    ssdistnr_max: float = 1
    # reject alert if PS1 star for any pp
    ps1_sgveto_rad: float = 1
    ps1_sgveto_sgth: float = 0.8
    # Minimal median RB.
    rb_minmed: float = 0.3
    # Minimal median RB.
    drb_minmed: float = 0.995
    # Limiting magnitude to consider upper limits as 'significant'
    maglim_min: float = 19.5
    # A limiting magnitude max this time ago
    maglim_maxago: float = 2.5

    # Cut to apply to all the photopoints in the light curve.
    # This will affect most operations, i.e. evaluating the position,
    # computing number of detections ecc.
    lc_filters: list[dict[str, Any]] = [
        {"attribute": "sharpnr", "operator": ">=", "value": -10.15},
        {"attribute": "magfromlim", "operator": ">", "value": 0},
    ]

    def inspect_ampelz(self, t2res: dict[str, Any]) -> None | dict[str, Any]:
        """
        Check whether Ampel Z data (from T2DigestRedshifts) fulfill criteria.
        """
        info = {'pass':0}
        
        if not t2res['ampel_z']:
            # No match
            return info
            
        info['ampel_z'] = t2res['ampel_z']
        info['ampel_z_precision'] = t2res['group_z_precision']
        info['ampel_dist'] = t2res['ampel_dist']
        # Add score
        if (self.min_redshift < info["ampel_z"] < self.max_redshift):
            info['pass'] += 1
        if (self.min_dist < info["ampel_dist"] < self.max_dist):
            info['pass'] += 1

        # Calculate physical distance
        info['dst_kpc'] = (
            info["ampel_dist"] *
                    Planck15.kpc_proper_per_arcmin(info["ampel_z"]).value / 60.0
                )
        if info['dst_kpc'] < self.max_kpc_dist:
            info['pass'] += 1
        
        # Return collected info
        return info

    def inspect_possis(self, t2res: dict[str, Any]) -> None | dict[str, Any]:
        """
        Check whether a fit to T2RunPossis models look good.
        """
        info = {'pass':0, 'model':t2res['model_name']}
        if not t2res['z'] or not t2res['sncosmo_result']['success']:
            return info	
        
        info['possis_abspeak'] = t2res['fit_metrics']['restpeak_model_absmag_B']
        info['possis_obspeak'] = t2res['fit_metrics']['obspeak_model_B']
        info['possis_chisq'] = t2res['sncosmo_result']['chisq']
        info['possis_ndof'] = t2res['sncosmo_result']['ndof']
        
        if info['possis_ndof']<0:
            return info
        
        if (self.min_absmag < info["possis_abspeak"] < self.max_absmag):
            info['pass'] += 1
        
        return info


    def inspect_lc(self, lc: LightCurve) -> None | dict[str, Any]:
        """
        Verify whether the transient lightcurve fulfill criteria for submission.

        """

        # apply cut on history: consider photophoints which are sharp enough
        pps = lc.get_photopoints(filters=self.lc_filters)
        assert pps is not None
        info: dict[str, Any] = {}

        # cut on number of detection
        if len(pps) < self.min_ndet:
            self.logger.info(
                'Rejected', extra={'det': len(pps)}
            )
            return None
        info["detections"] = len(pps)

        # cut on age
        jds = [pp["body"]["jd"] for pp in pps]
        most_recent_detection, first_detection = max(jds), min(jds)
        age = most_recent_detection - first_detection
        if age > self.max_age or age < self.min_age:
            self.logger.info('Rejected', extra={'age': age})
            return None
        info["age"] = age

        # cut on number of detection after last SIGNIFICANT UL
        ulims = lc.get_upperlimits(
            filters={
                "attribute": "diffmaglim",
                "operator": ">=",
                "value": self.maglim_min,
            }
        )

        if ulims and len(ulims) > 0:
            last_ulim_jd = sorted([x["body"]["jd"] for x in ulims])[-1]
            pps_after_ndet = lc.get_photopoints(
                filters=self.lc_filters + [{"attribute": "jd", "operator": ">=", "value": last_ulim_jd}]
            )
            # Check if there are enough positive detection after the last significant UL
            if (
                pps_after_ndet is not None and
                len(pps_after_ndet) < self.min_ndet_postul
            ):
                self.logger.info(
                    "not enough consecutive detections after last significant UL.",
                    extra={"NDet": len(pps), "lastUlimJD": last_ulim_jd},
                )
                return None
            # Check that there is a recent ul
            if (most_recent_detection - last_ulim_jd) > self.maglim_maxago:
                self.logger.info(
                    "No recent UL.",
                    extra={
                        "lastDet": most_recent_detection,
                        "lastUlimJD": last_ulim_jd,
                    },
                )
                return None
            info["last_UL"] = most_recent_detection - last_ulim_jd
        else:
            self.logger.info("no UL")
            return None

        # cut on number of filters
        used_filters = set([pp["body"]["fid"] for pp in pps])
        if len(used_filters) < self.min_n_filters:
            self.logger.info(
                "Rejected", extra={'nbr_filt': len(used_filters)}
            )
            return None
        # cut on which filters used
        if used_filters.isdisjoint(self.det_filterids):
            self.logger.info(
                "Rejected (wrong filter det)", extra={'det_filters': used_filters}
            )
            return None

        # cut on range of peak magnitude
        mags = [pp["body"]["magpsf"] for pp in pps]
        peak_mag = min(mags)
        if peak_mag > self.min_peak_mag or peak_mag < self.max_peak_mag:
            self.logger.info(
                "Rejected", extra={'peak_mag': peak_mag}
            )
            return None
        info["peak_mag"] = peak_mag

        # For rapidly declining sources the latest magnitude is probably more relevant
        latest_pps = lc.get_photopoints(
            filters={
                "attribute": "jd",
                "operator": "==",
                "value": most_recent_detection,
            }
        )
        if latest_pps:
            if not len(latest_pps) == 1:
                raise ValueError("Have assumed a unique last photopoint")
            info["latest_mag"] = latest_pps[0]["body"]["magpsf"]

        # TODO: cut based on the mag rise per day (see submitRapid)

        # cut on galactic coordinates
        if pos := lc.get_pos(ret="mean", filters=self.lc_filters):
            ra, dec = pos
        else:
            raise ValueError("Light curve contains no points")
        coordinates = SkyCoord(ra, dec, unit="deg")
        b = coordinates.galactic.b.deg
        if abs(b) < self.min_gal_lat:
            self.logger.info(
                "Rejected (galactic plane)", extra={'gal_lat_b': b}
            )
            return None
        info["ra"] = ra
        info["dec"] = dec

        # cut on distance to closest solar system object
        # TODO: how to make this check: ('0.0' in list(phot["ssdistnr"])
        ssdist = np.array([pp["body"]["ssdistnr"] for pp in pps
            if "ssdistnr" in pp['body'].keys() and pp["body"]["ssdistnr"] is not None])
        close_to_sso = np.logical_and(ssdist < self.ssdistnr_max, ssdist > 0)

        # TODO: Note that this discards a transient if it was ever close to a ss object!
        if np.any(close_to_sso):
            self.logger.info(
                "Rejected (close to solar system object)",
                extra={"ssdistnr": ssdist.tolist()},
            )
            return None

        # check PS1 sg for the full alert history
        # Note that we for this check do *not* use the lightcurve filter criteria
        # TODO: Evaluate whether we should use the filters, and do a check for sufficient number of datapoints remaining
        if psdata := lc.get_tuples("distpsnr1", "sgscore1"):
            distpsnr1, sgscore1 = zip(*psdata)
            is_ps1_star = np.logical_and(
                np.array(distpsnr1) < self.ps1_sgveto_rad,
                np.array(sgscore1) > self.ps1_sgveto_sgth,
            )
            if np.any(is_ps1_star):
                self.logger.info(
                    "Rejected (PS1 SG cut)",
                    extra={"distpsnr1": distpsnr1, "sgscore1": sgscore1},
                )
                return None
        else:
            self.logger.info("No PS1 check as no data found.")

        # cut on median RB and DRB score
        rbs = [pp["body"]["rb"] for pp in pps]
        if np.median(rbs) < self.rb_minmed:
            self.logger.info(
                "Rejected (RB)",
                extra={"median_rd": np.median(rbs)},
            )
            return None
        elif (len(rbs) == 0) and self.rb_minmed > 0:
            self.logger.info("Rejected (No rb info)")
            return None
        info["rb"] = np.median(rbs)

        # drb might not exist
        drbs = [pp["body"]["drb"] for pp in pps if "drb" in pp["body"]]
        if len(drbs) > 0 and np.median(drbs) < self.drb_minmed:
            self.logger.info(
                "Rejected (dRB)",
                extra={"median_drd": np.median(drbs)},
            )
            return None
        elif (len(drbs) == 0) and self.drb_minmed > 0:
            self.logger.info("Rejected (No drb info)")
            return None

        info["drb"] = np.median(drbs)

        # Transient passed pure LC criteria
        self.logger.info("Passed T2infantCatalogEval", extra=info)
        return info


    # MANDATORY
    def process(self, light_curve: LightCurve, t2_views: Sequence[T2DocView]) -> UBson | UnitResult:
        """

        Evaluate whether a transient passes thresholds for being a nearby (young) transient.

        Parameters
        -----------
        light_curve: "ampel.view.LightCurve" instance.
        See the LightCurve docstring for more info.

        t2_views: List of T2Views (assumed to be the result of a CatalogMatch)

        Returns
        -------
        dict

        Containing transient info, and in particular the 'action' key. This will be set to true
        for transients passing all selection criteria.

        """

        kilonovaness: int = 0
        info = {'possis':[]}
        
        # Check t2 ouputs
        for t2_view in t2_views:
            self.logger.info('Parsing t2 results from {}'.format(t2_view.unit))
            t2_res = res[-1] if isinstance(res := t2_view.get_payload(), list) else res
            # Redshift
            if t2_view.unit == 'T2DigestRedshifts':
                zinfo = self.inspect_ampelz(t2_res)
                info.update(zinfo)
                kilonovaness += zinfo['pass']
            # Fit to kilonova model
            if t2_view.unit == 'T2RunPossis':
                pinfo = self.inspect_possis(t2_res)
                info['possis'].append(pinfo)   # Could be multiple possis fits
                kilonovaness += pinfo['pass']
                if 'possis_abspeak' in info.keys():
                    if info['possis_chisq']>zinfo['possis_chisq']:
                        info.update( pinfo )
                else:
                    info.update( pinfo )
                
            # Propagate map info
            if t2_view.unit == 'T2PropagateStockInfo':
                info.update( t2_res )   # Could there be multiple maps associated? E.g. after updates? TODO
        


        # Check whether the lightcurve passes selection criteria
        # TODO: add kilonovaness criteria
        lc_info = self.inspect_lc(light_curve)
        if lc_info:
           info.update(lc_info)


        # iii. Check absolute magnitude - again (but directly from lightcurve)
        if (z := info.get('ampel_z')) and (obsmag := info.get("peak_mag")):
            sndist = Distance(z=z, cosmology=Planck15)
            info["absmag"] = obsmag - sndist.distmod.value
            if (self.min_absmag < info['absmag'] < self.max_absmag):
                kilonovaness += 1

        # Categorize
        if kilonovaness > 3:
            info['is_gold'] = True
        info['kilonovaness'] = kilonovaness

        return info
