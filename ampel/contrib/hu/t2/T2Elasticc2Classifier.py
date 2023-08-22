#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                ampel/contrib/hu/t2/T2Elasticc2Classification.py
# License:             BSD-3-Clause
# Author:              jnordin@physik.hu-berlin.de
# Date:                25.08.2022
# Last Modified Date:  13.12.2022
# Last Modified By:    jnordin@physik.hu-berlin.de

from abc import abstractmethod
from typing import Any, Iterable, Literal, TypedDict
import numpy as np

import joblib
import copy

from ampel.types import UBson
from ampel.model.UnitModel import UnitModel
from ampel.struct.UnitResult import UnitResult
from ampel.abstract.AbsStateT2Unit import AbsStateT2Unit
from ampel.abstract.AbsTabulatedT2Unit import AbsTabulatedT2Unit
from ampel.content.T1Document import T1Document
from ampel.content.DataPoint import DataPoint
from ampel.struct.JournalAttributes import JournalAttributes

from ampel.contrib.hu.t2.T2ElasticcRedshiftSampler import get_elasticc_redshift_samples
from ampel.contrib.hu.t2.T2RunParsnip import run_parsnip_zsample
from ampel.contrib.hu.t2.T2TabulatorRiseDecline import T2TabulatorRiseDeclineBase, FitFailed, BaseLightCurveFeatures

# Parsnip models - avoid importing these if running in the light mode?
import parsnip
import lcdata

# METHODDATA
parsnip_taxonomy = {
    'SNII':  2224, 'SNIa':  2222, 'SNibc': 2223, 'SNIbc': 2223,
    'TDE':  2243, 'CART': 2245, 'ILOT': 2244, 'Mdwarf-flare': 2233,
    'PISN': 2246, 'KN': 2232, 'SLSN-I': 2242, 'SLSN': 2242,
    'SNIa91bg': 2226, 'SNIax': 2225, 'dwarf-nova': 2234, 'uLens': 2235,
    }

class ParsnipModelFiles(TypedDict):
    model: str
    classifier: str

class XgbMultiModelFiles(TypedDict):
    path: str
    classes: list[int]

class BaseElasticc2Classifier(AbsStateT2Unit, AbsTabulatedT2Unit, T2TabulatorRiseDeclineBase, BaseLightCurveFeatures):
    """
    Base class for carrying out operations which yield one (or more) Elasticc Taxonomy classifications.
    Classifications carried out by classifier method, which is assumed to be
    implemented in child.

    Base feature extraction structure:
    - 1a. Extract alert ID.
    - 1b. Extract host props (a la the RedshiftSampler)
    - 1c. Extract RiseDecline features.

    A series of base binary trees.
    - 2a. Run galactic vs nongalactic tree.
    - 2b. Run agn vs [kn+sn+long] IF significant nongalactic prob, otherwise set all of these to zero.
    - 2c. Run ulens vs [periodic+mdwarf+dwarf] IF significant galactic prob, otherwise set all of these to zero.
    - 2d. Run periodic star subclassification IF significant galactic prob, otherwise set all of these to zero.

    If there are few detection + short lightcurve:
    - 3a. Run kn vs [sn+long]
    - 3b. Run [mdwarf, dwarf-nova] x [periodic+mdwarf/dwarf-nova]
    [kn,mdwarf and dwarf-nova prob set to zero if the condition is not fulfilled.]

    If there are multiple detections:
    - 4a. Run parsnip for sn+long
    (- 4b. Periodic star subclassification could be here if we do not always run it.
    Note that the limits for sec 3 and 4 might not be the same. There are KN with up to 10 detections (even if very rare) and the parsnip model can work with less data (they use an internal algorithm).

    Adding priors:
    - 5. Accept a classification dict. Return this with rescaled probabilities based on some priors.
    # Lingering questions:

    Classifiers are expected to be one of:
    - xgboost binary classifier dumped jusing joblib, together with columns to use:
        {'columns':['ndet','t_lc',...],'model':...}
    - xgboost multi classifier...
    - parsnip
    Each are configured by label:path

    - The order of how classes are separate can be changed. For example could ulens events be picked out later, inside the low/high ndet forks. Same thing for AGN. Would this improve anything?
    - Should ulens be considered part of the periodic star multiclasser?
    - Should we also try to subclassify periodic stars with very few detections? Or just return the group id? A: At least for some types it works well with few det, so try.
    - What about the special "inclusive" KN channel?
    """


    # Setting for report to construct
    broker_name: str = 'AMPEL'
    broker_version: str = 'v0.8'
    classifier_name: str
    classifier_version: str

    ## Paths to classifiers to load.
    paths_xgbbinary: dict[str,str] = {
        'kn_vs_nonrec': '/home/jnordin/Downloads/model_kn_vs_other_non_recurring'
    }
    paths_xgbmulti: dict[str,XgbMultiModelFiles] = {
    }
    paths_parsnip: dict[str,ParsnipModelFiles] = {

    }

    # Parameters controlling host galaxy information
    # How many redshifts samples to return ? Implemented are 1,3 or 5.
    nbr_samples: int = 3
    # Check for final/spectroscopic redshifts, and use if present
    use_final_z: bool = False
    # Which bands should be used to estimate the galaxy color?
    use_galcol: list[tuple[str, str]] = [('u','i'), ('u','g')]
    # Max sample redshift
    sample_redshift_max: float = 3.            # Models, like parsnip, might not be able to process large redshifts
                                                  # Adjust redshifts to this level.  

    # Parameters controlling feature extraction
    max_ndet: int = 200000

    # Parameters controlling any parsnip fit
    # The parsnip model zeropoint will by default be set to that of
    # the input lightcurve. This can be adjusted by zp offset.
    # Offset can e.g. appear if training performed with wrong zeropoint...
    parsnip_zeropoint_offset: float = 0

    # Include features and ML results in t2_record
    return_features: bool = False

    result_adapter: None | UnitModel = None

    def read_class_models(self) -> None:
        self.class_xgbbinary = {
            label : joblib.load(path)
                for label, path in self.paths_xgbbinary.items()
        }
        self.class_xgbmulti: dict[str,Any] = {
            label : {**joblib.load(pathdir['path']), 'classes': pathdir['classes']}
                for label, pathdir in self.paths_xgbmulti.items()
        }
        self.class_parsnip = {
            label: {
                'classifier': parsnip.Classifier.load(paths['classifier']),
                'model':     parsnip.load_model(paths['model'], threads=1) }
                            for label, paths in self.paths_parsnip.items()
        }

    def post_init(self) -> None:
        self.init_lightcurve_extractor()
        self.read_class_models()

    def get_xgb_class(self, classlabel, features):
        """
        Return the classification for the labeled xgb model
        Classify based on features ordered according to columns
        """

        return self.class_xgbbinary[classlabel]['model'].predict_proba(
            np.array( [ features.get(col, np.nan)
                    for col in self.class_xgbbinary[classlabel]['columns'] ] ).reshape(1,-1)
        )

    def get_multixgb_class(self, classlabel, features):
        """
        Return the classification for the labeled xgb model
        Classify based on features ordered according to columns
        """

        if self.class_xgbmulti[classlabel]['model'].objective=="multi:softmax":
            # Single type classifier
            modelguess = self.class_xgbmulti[classlabel]['model'].predict(
                np.array( [ features.get(col, np.nan)
                    for col in self.class_xgbmulti[classlabel]['columns'] ] ).reshape(1,-1)
                    )
            pvals = [np.zeros(len(self.class_xgbmulti[classlabel]['classes']))]
            pvals[0][modelguess] = 1
        elif self.class_xgbmulti[classlabel]['model'].objective=="multi:softprob":
            # Multiple
            pvals = self.class_xgbmulti[classlabel]['model'].predict_proba(
                np.array( [ features.get(col, np.nan)
                        for col in self.class_xgbmulti[classlabel]['columns'] ] ).reshape(1,-1)
            )

        return {
            self.class_xgbmulti[classlabel]['classes'][k]:float(prob)
                for k, prob in enumerate(list(pvals[0]))
            }


    def get_parsnip_class(self, classlabel, features, lctable) -> dict[str,Any]:
        """
        Return the classification for the labeled parsnip model
        Classify based on features ordered according to columns
        
        Also limit to max model redshift.
        """
        
        if max(features['z_samples'])>=self.sample_redshift_max:
            zs, zw, maxweight = [],[], 0
            for z, w in zip(features['z_samples'], features['z_weights']):
                if z<self.sample_redshift_max:
                    zs.append(z)
                    zw.append(w)
                else:
                    maxweight += w
            zs.append(self.sample_redshift_max)
            zw.append(maxweight)                                
        else:                
            zs, zw = features['z_samples'], features['z_weights']
        
        pdict = run_parsnip_zsample(lctable,
            {'label':classlabel, 'z':zs, 'z_weights':zw},
            self.class_parsnip[classlabel]['model'],
            self.class_parsnip[classlabel]['classifier'],
            self.parsnip_zeropoint_offset )
        if pdict.get('Failed',None) is not None:
            return pdict
        return pdict


    @abstractmethod
    def classify(self, base_class_dict, features, lc_table) -> tuple[list[dict], dict]:
        """
        Based on the provided features, extend the base_class dict into
        one or more elasticc2 classification reports.

        Return a tuple with:
        - a list of classification dictionaries
        - a general dict to be stored in db.
        """
        ...



    def process(self,
        compound: T1Document,
        datapoints: Iterable[DataPoint]
        ) -> UnitResult:
        """

        Extract and combine results.

        Parameters
        -----------

        t2_records: List of T2Records from (here) TabulatorRiseDecline.

        Returns
        -------
        dict
        """
        dps = list(datapoints)

        # Construct the base reply. We have some magic here to fill
        class_report: dict[str,UBson] = {
            'brokerName': self.broker_name,
            'brokerVersion': self.broker_version,
            'classifierName': self.classifier_name,
            'classifierParams': self.classifier_version,
        }
        # use an alias variable to inform mypy that classifications is always a list
        classifications : list[dict] = []
        class_report['classifications'] = classifications

        ## 1a+b: extract alert information

        # Get alert info from T1Document if present
        for metarecord in compound['meta']:
            if isinstance(alert_id := metarecord.get('alert'), int):
                class_report['alertId'] = alert_id
                class_report['brokerIngestTimestamp'] = metarecord['ts'] * 1000
            if isinstance(alert_ts := metarecord.get('alert_ts'), int):
                class_report['elasticcPublishTimestamp'] = alert_ts

        # Extract information from the diaObject
        for dp in reversed(dps):
            if 'diaSourceId' in dp['body'].keys():
                class_report['diaSourceId'] = dp['body']['diaSourceId']
                break
        # Get the last diaSource id in datapoints
        features: dict[str,Any] = {}
        for dp in reversed(dps):
            if 'mwebv' in dp['body'].keys():
                features.update( get_elasticc_redshift_samples(dp['body'],
                        self.nbr_samples, self.use_galcol, self.use_final_z) )
                features['mwebv'] = dp['body']['mwebv']
                break


        ### 1c: Extract features
        # Convert input datapoints to standardized Astropy Table
        # Using standard tabulators
        flux_table = self.get_flux_table(datapoints)

        # Cut the flux table if requested
        if self.max_ndet>0 and len(flux_table)>self.max_ndet:
            flux_table = self.cut_flux_table(flux_table)

        try:
            # Calculate RiseDecline
            features.update( self.compute_stats(flux_table) )
        except FitFailed:
            # Continue, but likely cant say much without these
            pass
        
        try:
            # Calculate light_curve features
            features.update(self.extract_lightcurve_features(flux_table))
        except ValueError:
            self.logger.info('lightcurve extraction fail')
            pass
        # Create averaged values
        avgfeat = self.average_filtervalues(features)
        features.update(avgfeat)

        # Fix galaxy color mismatch: xgboost assumes galcol corresponds to
        # u-i color, while galcol here is a dict with potentially differnet
        # colors. Fix this.
        if features.get('galaxy_color') is not None:
            for col, val in features['galaxy_color'].items():
                features['galaxy_color_'+col] = val
            features['galaxy_color'] = features.get('galaxy_color_u-i',None)
        # Similarly we need homogeneous treatment of redshifts
        zkind = {'HOSTGAL_ZQUANT':1,'HOSTGAL2_ZQUANT':2,'HOSTGAL_ZSPEC':3,'HOSTGAL2_ZSPEC':3,'default':0}
        if features['z_source'] == 'default':
            features['z_source'] = 0
            features['z'] = 0
        else:
            # Assuming three samples - this is what was used in feature training
            features['z'] = features['z_samples'][1]
            features['z_source'] = zkind[features['z_source']]
        # Correct redshift 

        # TODO: Check for minimal data to attempt classifcation?
        # Otherwise, prepare and report classification: {'classId':300, probability: 1}

        # Run the classifiers, tie together to one or more reports
        (classification_reports, probabilities) = self.classify(class_report, features, flux_table)

        if self.return_features:
            t2body = {
                "reports": classification_reports,
                "features": features,
                "probabilities": probabilities
            }
        else:
            t2body = {
                "reports": classification_reports,
            }

        # Prepare t2 document
        return UnitResult(
            body=t2body,
            adapter=self.result_adapter,
            # record the link the stock journal so we can tell which states have been reported
            journal=JournalAttributes(extra={"link": compound["link"]})
        )




def add_elasticc2_zprior(classification_dict: dict, z: float):
        """
        Adjust probabilities based on redshift prior.
        """
        # Elasticc redshift priors.
        # Warning: Based on normalized distributions of training sample, no rate info taken into account.
        # Bins based on z 0.1 bins starting at -0.09 and extending to 3.4
        # Probabilities assumed to add up to 1000 for each redshift bin.
        # Cutting out FAST galactic objects (not included in this run, but compare T2ElasticcReport.)
        zmap = {
            2222: {0: 12, 1: 19, 2: 26, 3: 39, 4: 60, 5: 92, 6: 134, 7: 181, 8: 217, 9: 225, 10: 200, 11: 150, 12: 93, 13: 48, 14: 23, 15: 11, 16: 5, 17: 2, 18: 0, 19: 0, 20: 0, 21: 0, 22: 0, 23: 0, 24: 0, 25: 0, 26: 0, 27: 0, 28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0},
            2223: {0: 49, 1: 60, 2: 73, 3: 89, 4: 105, 5: 119, 6: 127, 7: 128, 8: 121, 9: 107, 10: 90, 11: 72, 12: 53, 13: 36, 14: 25, 15: 18, 16: 13, 17: 8, 18: 3, 19: 1, 20: 0, 21: 0, 22: 0, 23: 0, 24: 0, 25: 0, 26: 0, 27: 0, 28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0},
            2224: {0: 37, 1: 47, 2: 59, 3: 75, 4: 94, 5: 112, 6: 126, 7: 131, 8: 127, 9: 117, 10: 105, 11: 95, 12: 86, 13: 77, 14: 70, 15: 66, 16: 63, 17: 59, 18: 48, 19: 33, 20: 21, 21: 11, 22: 5, 23: 2, 24: 0, 25: 0, 26: 0, 27: 0, 28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0},
            2225: {0: 49, 1: 64, 2: 77, 3: 95, 4: 116, 5: 136, 6: 145, 7: 137, 8: 111, 9: 77, 10: 46, 11: 23, 12: 9, 13: 3, 14: 1, 15: 1, 16: 1, 17: 2, 18: 1, 19: 0, 20: 0, 21: 0, 22: 0, 23: 0, 24: 0, 25: 0, 26: 0, 27: 0, 28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0},
            2226: {0: 61, 1: 78, 2: 100, 3: 127, 4: 151, 5: 156, 6: 132, 7: 88, 8: 45, 9: 17, 10: 6, 11: 3, 12: 2, 13: 2, 14: 1, 15: 0, 16: 0, 17: 0, 18: 0, 19: 0, 20: 0, 21: 0, 22: 0, 23: 0, 24: 0, 25: 0, 26: 0, 27: 0, 28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0},
            2232: {0: 354, 1: 293, 2: 234, 3: 166, 4: 98, 5: 45, 6: 15, 7: 3, 8: 0, 9: 0, 10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0, 16: 0, 17: 0, 18: 0, 19: 0, 20: 0, 21: 0, 22: 0, 23: 0, 24: 0, 25: 0, 26: 0, 27: 0, 28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0},
            2242: {0: 0, 1: 1, 2: 2, 3: 3, 4: 5, 5: 11, 6: 20, 7: 35, 8: 55, 9: 80, 10: 112, 11: 153, 12: 215, 13: 307, 14: 421, 15: 536, 16: 645, 17: 746, 18: 827, 19: 879, 20: 911, 21: 936, 22: 958, 23: 976, 24: 988, 25: 995, 26: 998, 27: 999, 28: 999, 29: 999, 30: 999, 31: 999, 32: 999, 33: 999, 34: 999, 35: 999},
            2243: {0: 37, 1: 46, 2: 56, 3: 69, 4: 84, 5: 100, 6: 113, 7: 123, 8: 133, 9: 141, 10: 145, 11: 142, 12: 130, 13: 108, 14: 83, 15: 63, 16: 50, 17: 43, 18: 38, 19: 34, 20: 30, 21: 25, 22: 20, 23: 14, 24: 9, 25: 4, 26: 1, 27: 0, 28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0},
            2244: {0: 293, 1: 257, 2: 223, 3: 173, 4: 114, 5: 59, 6: 23, 7: 6, 8: 1, 9: 0, 10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0, 16: 0, 17: 0, 18: 0, 19: 0, 20: 0, 21: 0, 22: 0, 23: 0, 24: 0, 25: 0, 26: 0, 27: 0, 28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0},
            2245: {0: 110, 1: 124, 2: 135, 3: 143, 4: 143, 5: 127, 6: 97, 7: 62, 8: 33, 9: 16, 10: 8, 11: 6, 12: 6, 13: 5, 14: 5, 15: 6, 16: 7, 17: 6, 18: 3, 19: 1, 20: 0, 21: 0, 22: 0, 23: 0, 24: 0, 25: 0, 26: 0, 27: 0, 28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0},
            2246: {0: 0, 1: 7, 2: 10, 3: 16, 4: 24, 5: 38, 6: 62, 7: 100, 8: 152, 9: 215, 10: 284, 11: 352, 12: 401, 13: 409, 14: 368, 15: 296, 16: 211, 17: 131, 18: 76, 19: 49, 20: 36, 21: 26, 22: 15, 23: 6, 24: 1, 25: 0, 26: 0, 27: 0, 28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0}
            }

        # Out of range
        if  z>3.409 or z<0:
            return classification_dict
        zbin = int( (z + 0.09) / 0.1 )

        postprob = 0.
        preprob = 0.
        for classprob in classification_dict['classifications']:
            if classprob['classId'] in zmap.keys():
                preprob += classprob['probability']
                classprob['probability'] *= float(zmap[classprob['classId']][zbin])/1000
                postprob += classprob['probability']
        if postprob==0:
            postprob = 1.0e-15
        probnorm = preprob / postprob
        for classprob in classification_dict['classifications']:
            if classprob['classId'] in zmap.keys():
                classprob['probability'] *= probnorm

        return classification_dict


def add_bts_rateprior(classification_dict: dict):
        """
        Adjust probabilities based BTS reported rates.

        """
        # Parsnip type priors
        # Based on BTS _observed_ rate distributions, i.e. no knowledge of elasticc
        # or LSST simulation properties.
        # Probabilities assumed to add up to 1000
        btsmap = {
                    2222: 663, 2223: 58, 2224: 168, 2225: 10, 2226: 10,
                    2232: 10, 2233: 10, 2234: 10, 2235: 10,
                    2242: 14, 2243: 10, 2244: 10, 2245: 10, 2246: 10
                    }

        postprob = 0.
        preprob = 0.
        for classprob in classification_dict['classifications']:
            if classprob['classId'] in btsmap.keys():
                preprob += classprob['probability']
                classprob['probability'] *= float(btsmap[classprob['classId']])/1000
                postprob += classprob['probability']
        if postprob==0:
            postprob = 1.0e-15
        probnorm = preprob / postprob
        for classprob in classification_dict['classifications']:
            if classprob['classId'] in btsmap.keys():
                classprob['probability'] *= probnorm

        return classification_dict


def add_elasticc2_galcolprior(classification_dict: dict, host_ug: float | None, mwebv: float):
        """
        Adjust probabilities based on host galaxy color.
        """

        # Priors based on the host galaxy u-g color (from elasticc_galcol notebook)
        galcol_prior: dict[int, list[float]] = {
            2222: [0.6102884945155177, 0.39269590167347995],
            2223: [0.9490882652856778, 0.5687292278916529],
            2224: [0.8752024059665746, 0.5297978836976114],
            2225: [0.6945950080255028, 0.36977359393920906],
            2226: [1.8926892171924607, 0.4252978932935133],
            2232: [1.3089799059907905, 0.3303618787483199],
            2242: [0.37962520970022234, 0.3723189361658543],
            2243: [0.8753566579007913, 0.7194828408852172],
            2244: [1.1458356003495815, 0.5093055923613805],
            2245: [1.1681864134021618, 0.5451610139053239],
            2246: [0.3426406749881123, 0.4465663913955291],
            }
        # Correlation between galaxy color and mwebv
        galcol_mwebv_slope =  1.1582821


        if host_ug is None:
            return classification_dict
        # Correct for mwebv dependence
        host_ug = host_ug-galcol_mwebv_slope*mwebv

        postprob = 0.
        preprob = 0.
        for classprob in classification_dict['classifications']:
            if classprob['classId'] in galcol_prior.keys():
                preprob += classprob['probability']
                classprob['probability'] *= np.exp(
        -(host_ug-galcol_prior[classprob['classId']][0])**2/(2.*galcol_prior[classprob['classId']][1]**2)
                )
                postprob += classprob['probability']
        if postprob==0:
            postprob = 1.0e-15
        probnorm = preprob / postprob
        for classprob in classification_dict['classifications']:
            if classprob['classId'] in galcol_prior.keys():
                classprob['probability'] *= probnorm
        return classification_dict



class T2Elasticc2Classifier(BaseElasticc2Classifier):
    """

    Feature extraction, submission in Base.

    Classification structure:
    - 1. Run Multi RF
    - 2. Possibly run parsnip (extragalactic + fit success). If so, get
         complementing RF.
    - 3. Add postversions.
    - 4. Check for ping conditions.
    - 5. Run all model w parsnip data
    
    If all works...
    - Try running parsnip, if so use that all model
    - Else use xgb model w/o parsnip?

    """

    # Setting for report to construct
    classifier_name: str = 'ElasticcMonster'
    classifier_version: str = 'v230819'

    # May reports to be generated
    reports: int = 99 #

    # fast: do not run parsnip (or multixgb if that is timeconsuming)
    # extragalactic: run parsnip if extragalctic prob>1%
    parsnip_mode: Literal["full", "fast", "extragalactic"] = "extragalactic"

    # Ping limit (SNIa)
    pinglim: float = 0.5
    pingtrange: list[float] = [15., 60.]
    pingmaxmag: float = 22.

    def classify(self, base_class_dict, features, flux_table) -> tuple[list[dict], dict]:
        """

        """

        # 1.  MultiXGB classifier
        multixgb_class =  copy.deepcopy(base_class_dict)
        multixgb_prob = self.get_multixgb_class('all', features)
        multixgb_class['classifications'].extend( [
                {'classId': fitclass, 'probability': float(fitprop)}
                    for fitclass, fitprop in multixgb_prob.items()
            ]
        )
        multixgb_class['classifierName'] += 'All'
        output_classes = [multixgb_class]
        prob = {'allxgb': {str(k):v for k,v in multixgb_prob.items()}  }
        if self.reports<2:
            return (output_classes, prob)

        # Create post version of all
        all_post_class = copy.deepcopy(multixgb_class)
        all_post_class['classifierName'] += 'Post'
        # Z prior not quite logical, and z is used by parsnip/xgb - skip?
        # all_post_class = add_elasticc2_zprior(all_post_class, features['z'])
        all_post_class = add_bts_rateprior(all_post_class)
        all_post_class = add_elasticc2_galcolprior(all_post_class,
                    features.get("hostgal_mag_u-g"), features.get("mwebv") )
        output_classes.append( all_post_class )
        if self.reports<3:
            return (output_classes, prob)


        ## 2. Parsnip Probe part.
        probe_class = copy.deepcopy(base_class_dict)
        probe_class['classifierName'] += 'Probe'
        # Run the necessary trees
        multivarstardwarf_prob = self.get_multixgb_class('stars_ulens_mdwarf_nova', features)
        # returns [prob_neg, prob_pos]: px[1] provides *pos* pred.
        pgal = self.get_xgb_class('gal_vs_nongal', features)[0]
        pagn = self.get_xgb_class('agn_vs_knsnlong', features)[0]
        pkn = self.get_xgb_class('kn_vs_snlong', features)[0]

        # Run parsnip
        if self.parsnip_mode in ['full','extragalactic']:
            if features['ndet']<0:
                parsnip_class: dict[str, Any] = {'Failed':'few det'}
            elif self.parsnip_mode=='extragalactic' and pgal[0]<0.01:
                parsnip_class = {'Failed':'galactic'}
            else:
                # attempt to run parsnip
                parsnip_class = self.get_parsnip_class('snlong', features, flux_table)
        else:
            parsnip_class = {'Failed': 'notrun'}

        # Start building the taxonomy
        # AGN
        probe_class['classifications'].extend( [
                {'classId': 2332, 'probability': float(pgal[0]*pagn[1])},
        ] )
        # KN
        probe_class['classifications'].extend( [
                    {'classId': 2232, 'probability': float(pgal[0]*pagn[0]*pkn[1])},
                ] )
        # Stars
        probe_class['classifications'].extend( [
                {'classId': fitclass, 'probability': float(pgal[1]*fitprop)}
                    for fitclass, fitprop in multivarstardwarf_prob.items()
            ]
        )
        # SNe + LONG
        if 'Failed' in parsnip_class.keys():
            # Parsnip did not work, so use the binary
            psn = self.get_xgb_class('sn_vs_long', features)[0]
            probe_class['classifications'].extend( [
                    {'classId': 2220, 'probability': float(pgal[0]*pagn[0]*pkn[0]*psn[1])},
                    {'classId': 2240, 'probability': float(pgal[0]*pagn[0]*pkn[0]*psn[0])},
                ] )
        else:
            probe_class['classifications'].extend([
                {'classId': parsnip_taxonomy[fitclass], 'probability': float(pgal[0]*pagn[0]*pkn[0]*fitprop)}
                    for fitclass, fitprop in parsnip_class['classification'].items()
            ] )
        output_classes.append(probe_class)
        prob.update( {'binary':[float(pgal[1]), float(pagn[1]), float(pkn[1]) ],
                'gal_recurrants': {str(k):v for k,v in multivarstardwarf_prob.items()},
                'parsnip': parsnip_class } )
        if self.reports<4:
            return (output_classes, prob)

        # Create post version
        probe_post_class = copy.deepcopy(probe_class)
        probe_post_class['classifierName'] += 'Post'
        # Z prior not quite logical, and z is used by parsnip/xgb - skip?
        # probe_post_class = add_elasticc2_zprior(probe_post_class, features['z'])
        probe_post_class = add_bts_rateprior(probe_post_class)
        probe_post_class = add_elasticc2_galcolprior(probe_post_class,
                    features.get("hostgal_mag_u-g"), features.get("mwebv") )
        output_classes.append( probe_post_class )
        if self.reports<5:
            return (output_classes, prob)

        # 4. Create PING version.
        snprob = [ classp['probability'] for classp in multixgb_class['classifications'] if classp['classId']==2222 ]
        snprob.extend( [ classp['probability'] for classp in probe_class['classifications'] if classp['classId']==2222 ] )
        # One should be defined
        if max(snprob)>self.pinglim:
            # Check other features
            if (self.pingtrange[0] <= features.get('t_lc',0) <= self.pingtrange[1]
                and self.pingmaxmag>features.get('mag_last',99)):
                ping_class = copy.deepcopy(base_class_dict)
                ping_class['classifierName'] += 'Ping'
                ping_class['classifications'].extend( [
                        {'classId': 2222, 'probability': 1.0},
                        ])
                output_classes.append(ping_class)
        if self.reports<6:
            return (output_classes, prob)

        # 5. Create pure "all" version
        if 'prediction' in parsnip_class.keys():
            # Add parsnip features to features
            features.update( {'parsnip_'+featname : featval for featname, featval in  parsnip_class['prediction'].items() } )
            features['parsnip_modelchidof'] = features['parsnip_model_chisq'] / features['parsnip_model_dof']
            # Create new class setup
            allincl_class =  copy.deepcopy(base_class_dict)
            allincl_prob = self.get_multixgb_class('all_incl', features)
            allincl_class['classifications'].extend( [
                    {'classId': fitclass, 'probability': float(fitprop)}
                        for fitclass, fitprop in allincl_prob.items()
                        ]
                )
            allincl_class['classifierName'] += 'AllInclusive'
            output_classes.append(allincl_class)
            prob['allincl'] = {str(k):v for k,v in allincl_prob.items()}
        if self.reports<7:
            return (output_classes, prob)



        # Verification
        for report in output_classes:
            testsum = sum([d['probability'] for d in report['classifications']])
            if np.abs(testsum-1)>0.01:
                self.logger.info('prob ne 1',extra=report)


        return (output_classes, prob)
