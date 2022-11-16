#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t2/T2BayesianBlocks.py
# License           : BSD-3-Clause
# Author            : Eleni
# Date              : 28.04.2021
# Last Modified Date: 16.11.2022
# Last Modified By  : Simeon

import os
from typing import Dict, List, Optional, Sequence, Any
import numpy as np
import matplotlib.pyplot as plt
import more_itertools as mit

from ampel.types import UBson
from ampel.struct.UnitResult import UnitResult
from ampel.view.LightCurve import LightCurve
import matplotlib.pyplot as plt
import pandas as pd
from astropy.stats import bayesian_blocks

from ampel.abstract.AbsLightCurveT2Unit import AbsLightCurveT2Unit

from ampel.model.PlotProperties import PlotProperties
from ampel.ztf.util.ZTFIdMapper import to_ztf_id
from ampel.plot.create import create_plot_record
import uncertainties.unumpy as unumpy
from nltk import flatten
import itertools
import math
from scipy.signal import find_peaks
from sklearn.metrics import mean_squared_error


class T2BayesianBlocks(AbsLightCurveT2Unit):
    """ """

    # Min number of detections remaining in the target range for subsequent analysis
    min_det: int = 3

    # Rejection sigma
    rej_sigma: float = 3  # A lower number will more aggressively reject data

    # Plot
    plot: bool = True

    # Use Npoints per observed datapoint
    Npoints: bool = False

    # Type of data we will process, valid ztf_alert, ztf_fp, wise
    data_type: str = "wise"

    debug: bool = False

    debug_dir: Optional[str]

    filter: Sequence[str]

    # Work with fluxes instead of magnitude
    flux: bool = False
    #
    plot_props: Optional[PlotProperties]
    # Color of filters for the plots

    PlotColor: Sequence[str] = ["red", "red", "blue"]

    def baseline(self, baye_block, flux):
        if flux:

            baseline = baye_block["mag"][baye_block["mag"].idxmin()]
            baseline_sigma = baye_block["mag.err"][baye_block["mag"].idxmin()]

            if baye_block["measurements_nu"][baye_block["mag"].idxmin()] == 1:
                baye_block.loc[baye_block["mag"].idxmin(), "level"] = "baseline"
                baye_block.loc[
                    baye_block.index[
                        baye_block["mag"]
                        == baye_block.sort_values(by=["mag"]).iloc[1]["mag"]
                    ].tolist()[0],
                    "level",
                ] = "baseline"
                value = unumpy.uarray(
                    np.array(baye_block[baye_block["level"] == "baseline"]["mag"]),
                    np.array(baye_block[baye_block["level"] == "baseline"]["mag.err"]),
                )
                baseline = np.mean(value).nominal_value
                baseline_sigma = np.mean(value).std_dev
        else:
            baseline = baye_block["mag"][baye_block["mag"].idxmax()]
            baseline_sigma = baye_block["mag.err"][baye_block["mag"].idxmax()]

            if baye_block["measurements_nu"][baye_block["mag"].idxmax()] == 1:
                baye_block["level"][baye_block["mag"].idxmax()] = "baseline"
                baye_block["level"][
                    baye_block.index[
                        baye_block["mag"]
                        == baye_block.sort_values(by=["mag"]).iloc[-2]["mag"]
                    ].tolist()[0]
                ] = "baseline"
                value = unumpy.uarray(
                    np.array(baye_block[baye_block["level"] == "baseline"]["mag"]),
                    np.array(baye_block[baye_block["level"] == "baseline"]["mag.err"]),
                )
                baseline = np.mean(value).nominal_value
                baseline_sigma = np.mean(value).std_dev

        if np.isnan(baseline_sigma):
            baseline_sigma = baye_block.sort_values(by="mag.err", ascending=False)[
                "mag.err"
            ].iloc[0]
        return (baseline, baseline_sigma)

    def baye_block_levels(self, baye_block, baseline, baseline_sigma):
        for nu, mag in enumerate(baye_block["mag"]):
            idx = baye_block.index.tolist()[nu]
            if np.isnan(baye_block["mag.err"][idx]):
                if abs((baseline - mag) / baseline_sigma) < self.rej_sigma:
                    baye_block.loc[idx, "level"] = "baseline"
                else:
                    baye_block.loc[idx, "level"] = "excess"
            elif baye_block["level"][idx] != "baseline":
                if (
                    baseline + (self.rej_sigma * baseline_sigma)
                    >= mag
                    >= baseline - (self.rej_sigma * baseline_sigma)
                ) and (
                    baseline + (self.rej_sigma * baseline_sigma)
                    >= mag - baye_block["mag.err"][idx]
                ):
                    baye_block.loc[idx, "level"] = "baseline"
                else:
                    baye_block.loc[idx, "level"] = "excess"
        return baye_block

    def idx_of_excess_regions(self, excess_region):
        length = 1
        excess_regions = []
        for nu in range(1, len(excess_region) + 1):
            if (
                nu == len(excess_region)
                or excess_region.index[nu] - excess_region.index[nu - 1] != 1
            ):
                if length == 1:
                    excess_regions.append([excess_region.index[nu - length]])
                elif length == len(excess_region):
                    excess_regions.append(
                        np.arange(
                            excess_region.index[0], excess_region.index[nu - 1] + 1
                        )
                    )
                else:
                    excess_regions.append(
                        np.arange(
                            excess_region.index[nu - length],
                            excess_region.index[nu - 1] + 1,
                        )
                    )
                length = 1
            else:
                length += 1
        return excess_regions

    def outliers(self, excess_regions, df, baye_block, measurements_nu):
        for nu, value in enumerate(excess_regions):
            if len(value) == 1:
                # if measurements_nu[value[0]] == 1.0:
                if measurements_nu[value[0]] < 5:
                    if (
                        baye_block["Npoints"][value[0]] == 1
                        and baye_block["sigma_from_baseline"][value[0]] > 5.0
                    ):
                        df.loc[
                            df.index[
                                df["jd"].between(
                                    baye_block["jd_measurement_start"][value[0]],
                                    baye_block["jd_measurement_end"][value[0]],
                                    inclusive=True,
                                )
                            ].tolist()[0],
                            "Outlier",
                        ] = True
                        baye_block.loc[value[0], "level"] = "outlier"
        return baye_block

    def description(self, excess_regions, measurements_nu):
        # 0: The excess region has one baye block with one measurement, 1: The excess region has one baye block with multiple measurements, 2: The excess region has multiple baye blocks
        description = []
        for nu, value in enumerate(excess_regions):
            if len(value) == 1:
                if measurements_nu[value[0]] == 1.0:
                    description.append([0])
                else:
                    description.append([1])
            else:
                description.append([2])
        return description

    def coincide_peak_block(self, output_per_filter):
        # It returns 1: bayesian regions of different filters coincide, -1: bayesian regions of different filters do not coincide

        coincide_region = 0
        # we select the first filter to later compare it to the others
        base_filter = list(output_per_filter.keys())[0]

        if output_per_filter[base_filter]["nu_of_excess_regions"][0] == 0:
            return coincide_region
        else:
            if self.flux:
                idx = np.argmax(output_per_filter[base_filter]["max_mag_excess_region"])
            else:
                idx = np.argmin(output_per_filter[base_filter]["max_mag_excess_region"])
            start_region = output_per_filter[base_filter]["jd_excess_regions"][idx][0]
            end_region = output_per_filter[base_filter]["jd_excess_regions"][idx][-1]

            # now we select the other filters
            for compare_filter in list(output_per_filter.keys())[1 : len(self.filter)]:

                # as the i-band is spotty in the case of ZTF, we skip it
                if self.data_type in ["ztf_alert", "ztf_fp"]:
                    if compare_filter in ["ZTF_i", "ztf_i", "ztfi"]:
                        continue

                if (
                    int(output_per_filter[compare_filter]["nu_of_excess_regions"][0])
                    != 0
                ):
                    if self.flux:
                        idx = np.argmax(
                            output_per_filter[compare_filter]["max_mag_excess_region"]
                        )
                    else:
                        idx = np.argmin(
                            output_per_filter[compare_filter]["max_mag_excess_region"]
                        )
                    if (
                        (
                            output_per_filter[compare_filter]["jd_excess_regions"][idx][
                                -1
                            ]
                            >= start_region
                            >= output_per_filter[compare_filter]["jd_excess_regions"][
                                idx
                            ][0]
                        )
                        or (
                            output_per_filter[compare_filter]["jd_excess_regions"][idx][
                                -1
                            ]
                            >= end_region
                            >= output_per_filter[compare_filter]["jd_excess_regions"][
                                idx
                            ][0]
                        )
                        or (
                            start_region
                            <= output_per_filter[compare_filter]["jd_excess_regions"][
                                idx
                            ][0]
                            <= end_region
                        )
                    ):
                        coincide_region += 1

        return coincide_region

    def process(self, light_curve: LightCurve) -> UBson | UnitResult:
        """ """

        assert self.data_type in ["ztf_alert", "ztf_fp", "wise"]

        if self.data_type in ["ztf_alert", "ztf_fp"]:
            self.ztfid = to_ztf_id(light_curve.stock_id)
            self.PlotColor = ["green", "red", "orange"]
        ################################
        ######## Bayesian blocks #######
        output_per_filter = {}
        if self.plot:
            fig = plt.figure()
            ax = fig.add_subplot(len(self.filter) + 1, 1, 1)

        for fid, passband in enumerate(self.filter, 1):
            baye_block = pd.DataFrame(
                columns=[
                    "jd_start",
                    "jd_end",
                    "jd_measurement_start",
                    "jd_measurement_end",
                    "mag",
                    "mag.err",
                    "measurements_nu",
                    "sigma_from_old_baseline",
                    "sigma_from_baseline",
                    "mag_edge",
                ]
            )

            output = {
                "nu_of_excess_regions": ([]),
                "nu_of_excess_blocks": ([]),
                "nu_of_baseline_blocks": ([]),
                "jd_excess_regions": ([]),
                "mag_edge_excess": ([]),
                "max_mag_excess_region": ([]),
                "max_jd_excess_region": ([]),
                "max_sigma_excess_region": ([]),
                "max_baye_block_timescale": ([]),
                "baseline": ([]),
                "baseline_sigma": ([]),
                "baseline_rms": ([]),
                "jd_baseline_regions": ([]),
                "jd_outlier": ([]),
                "mag_edge_baseline": ([]),
                "sigma_from_baseline": ([]),
                "sigma_from_baseline_excess": ([]),
                "significance_of_variability_excess": ([], []),
                "significance_of_fluctuation": ([]),
                "max_mag": ([]),
                "significance": ([]),
                "strength_sjoert": ([]),
                "significance_after_peak": ([]),
                "strength_after_peak": ([]),
                "description": ([]),
            }  # 0: The excess region has one baye block with one measurement, 1: The excess region has one baye block with multiple measurements, 2: The excess region has multiple baye blocks

            if self.data_type == "ztf_fp":
                if self.flux:
                    phot_tuple = light_curve.get_ntuples(
                        ["jd", "flux_Jy", "flux_err_Jy"],
                        {"attribute": "fid", "operator": "==", "value": fid},
                    )

                else:
                    phot_tuple = light_curve.get_ntuples(
                        ["jd", "magpsf", "sigmapsf"],
                        {"attribute": "fid", "operator": "==", "value": fid},
                    )
            elif self.data_type == "ztf_alert":
                if self.flux:
                    raise ValueError("Not implemented yet")
                else:
                    phot_tuple = light_curve.get_ntuples(
                        ["jd", "magpsf", "sigmapsf"],
                        {"attribute": "fid", "operator": "==", "value": fid},
                    )
            elif self.data_type == "wise":
                if self.flux:
                    if self.Npoints:
                        phot_tuple = light_curve.get_ntuples(
                            ["jd", "mean_flux", "flux_rms", "flux_density_Npoints"],
                            [
                                {
                                    "attribute": "filter",
                                    "operator": "==",
                                    "value": passband,
                                },
                                {
                                    "attribute": "flux_ul",
                                    "operator": "==",
                                    "value": "False",
                                },
                            ],
                        )
                    else:
                        phot_tuple = light_curve.get_ntuples(
                            ["jd", "mean_flux", "flux_rms"],
                            [
                                {
                                    "attribute": "filter",
                                    "operator": "==",
                                    "value": passband,
                                },
                                {
                                    "attribute": "flux_ul",
                                    "operator": "==",
                                    "value": "False",
                                },
                            ],
                        )
                else:
                    if self.Npoints:
                        phot_tuple = light_curve.get_ntuples(
                            ["jd", "magpsf", "sigmapsf", "mag_Npoints"],
                            [
                                {
                                    "attribute": "filter",
                                    "operator": "==",
                                    "value": passband,
                                },
                                {
                                    "attribute": "mag_ul",
                                    "operator": "==",
                                    "value": "False",
                                },
                            ],
                        )
                    else:
                        phot_tuple = light_curve.get_ntuples(
                            ["jd", "magpsf", "sigmapsf"],
                            [
                                {
                                    "attribute": "filter",
                                    "operator": "==",
                                    "value": passband,
                                },
                                {
                                    "attribute": "mag_ul",
                                    "operator": "==",
                                    "value": "False",
                                },
                            ],
                        )

            output_per_filter[passband] = []
            if phot_tuple is None or len(phot_tuple) <= 1:
                output["nu_of_excess_regions"].append(0)
                output["nu_of_excess_blocks"].append(0)
                output["nu_of_baseline_blocks"].append(0)
                output["jd_excess_regions"].append(0)
                output["mag_edge_excess"].append(0)
                output["max_mag_excess_region"].append(0)
                output["max_jd_excess_region"].append(0)
                output["max_sigma_excess_region"].append(0)
                output["max_baye_block_timescale"].append(0)
                output["baseline"].append(0)
                output["baseline_sigma"].append(0)
                output["baseline_rms"].append(0)
                output["jd_baseline_regions"].append(0)
                output["jd_outlier"] = None
                output["mag_edge_baseline"].append(0)
                output["sigma_from_baseline"].append(0)
                output["sigma_from_baseline_excess"].append(0)
                output["significance_of_variability_excess"][0].append(0)
                output["significance_of_variability_excess"][1].append(0)
                output["significance_of_fluctuation"].append(0)
                output["max_mag"].append(0)
                output["significance"].append(0)
                output["strength_sjoert"].append(0)
                output["description"].append(None)
                output_per_filter[str(passband)] = dict(output)
                continue

            if self.Npoints:
                df = pd.DataFrame(
                    phot_tuple, columns=["jd", "mag", "mag.err", "Npoints"]
                )
            else:
                df = pd.DataFrame(phot_tuple, columns=["jd", "mag", "mag.err"])

            # Now we use a rolling window as extreme outlier rejection for ZTF data
            if self.data_type in ["ztf_alert", "ztf_fp"]:
                df["median"] = df["mag"].rolling(10).median()
                df["std"] = df["mag"].rolling(10).std()
                df = df[
                    (df.mag <= df["median"] + 3 * df["std"])
                    & (df.mag >= df["median"] - 3 * df["std"])
                ]

            df = df.sort_values(by=["jd"], ignore_index=True)
            df["Outlier"] = False

            if self.data_type == "wise":
                ncp_prior = 1.32 + 0.577 * math.log10(len(df))
            elif self.data_type == "ztf_fp":
                ncp_prior = 10 * math.log10(len(df))

            """
            in case multiple measurements have the same
            jd (there are cases where df['jd'].unique() contains no
            unique values, but np.unique(df['jd'].values) does, for
            whichever reason). We simply use the first of the measurements
            with the same jd
            """
            unique_jd, unique_jd_idx = np.unique(df["jd"].values, return_index=True)

            edges = bayesian_blocks(
                unique_jd,
                df["mag"].values[unique_jd_idx],
                sigma=df["mag.err"].values[unique_jd_idx],
                ncp_prior=ncp_prior,
                fitness="measures",
            )

            for i in range(1, len(edges)):
                baye_block_all = df.loc[
                    df["jd"].between(edges[i - 1], edges[i], inclusive="both")
                ]

                all_value_per_block = unumpy.uarray(
                    np.array(baye_block_all["mag"]), np.array(baye_block_all["mag.err"])
                )

                if self.data_type == "wise":
                    mag_err = np.mean(all_value_per_block).std_dev
                elif self.data_type == "ztf_fp":
                    mag_err = mean_squared_error(
                        unumpy.nominal_values(all_value_per_block),
                        [np.mean(all_value_per_block).nominal_value]
                        * len(all_value_per_block),
                        squared=False,
                    )

                to_append = pd.DataFrame(
                    {
                        "jd_start": edges[i - 1],
                        "jd_end": edges[i],
                        "jd_measurement_start": min(baye_block_all["jd"]),
                        "jd_measurement_end": max(baye_block_all["jd"]),
                        "mag": np.mean(all_value_per_block).nominal_value,
                        "mag.err": np.mean(all_value_per_block).std_dev,
                        "mag.err": mag_err,
                        "measurements_nu": len(baye_block_all),
                        "mag_edge": baye_block_all["mag"][
                            baye_block_all["jd"].idxmax()
                        ],
                    },
                    index=[0],
                )

                if self.Npoints:
                    to_append["Npoints"] = [np.array(baye_block_all["Npoints"])]

                baye_block = pd.concat([baye_block, to_append], ignore_index=True)

            baye_block["level"] = None
            baye_block = baye_block.astype(
                {
                    "jd_start": "float64",
                    "jd_end": "float64",
                    "jd_measurement_start": "float64",
                    "jd_measurement_end": "float64",
                    "mag": "float64",
                    "mag.err": "float64",
                    "measurements_nu": "float64",
                    "sigma_from_old_baseline": "float64",
                    "sigma_from_baseline": "float64",
                    "mag_edge": "float64",
                }
            )

            #######################################
            ######### Find the baseline ###########

            (baseline, baseline_sigma) = self.baseline(baye_block, self.flux)

            for sigma_discr in ["sigma_from_old_baseline", "sigma_from_baseline"]:
                # Recalculate levels
                baye_block[baye_block["level"] != "baseline"] = self.baye_block_levels(
                    baye_block[baye_block["level"] != "baseline"],
                    baseline,
                    baseline_sigma,
                )
                ####### Calculate rms of the baseline
                value = unumpy.uarray(
                    np.array(baye_block[baye_block["level"] == "baseline"]["mag"]),
                    np.array(baye_block[baye_block["level"] == "baseline"]["mag.err"]),
                )
                baseline_region = baye_block[baye_block["level"] == "baseline"]
                baseline_values = []
                baseline_error_values = []
                for i in baseline_region.index:
                    baseline_values.append(
                        df[
                            df["jd"].between(
                                baseline_region["jd_measurement_start"][i],
                                baseline_region["jd_measurement_end"][i],
                                inclusive="both",
                            )
                        ]["mag"].values
                    )
                    baseline_error_values.append(
                        df[
                            df["jd"].between(
                                baseline_region["jd_measurement_start"][i],
                                baseline_region["jd_measurement_end"][i],
                                inclusive="both",
                            )
                        ]["mag.err"].values
                    )

                value = unumpy.uarray(
                    list(itertools.chain(*baseline_values)),
                    list(itertools.chain(*baseline_error_values)),
                )
                baseline = np.mean(value).nominal_value
                baseline_sigma = np.mean(value).std_dev
                baseline_rms = mean_squared_error(
                    list(itertools.chain(*baseline_values)),
                    [baseline] * len(list(itertools.chain(*baseline_values))),
                    squared=False,
                )
                baye_block[str(sigma_discr)] = abs(
                    (baseline - baye_block["mag"]) / baseline_sigma
                )

            #######################################
            ########## Excess region  #############
            excess_region = baye_block[baye_block["level"] == "excess"]
            if not excess_region.empty:
                # Find the excess regions (baye block that are accumulated to a region)

                excess_regions_idx = [
                    list(group)
                    for group in mit.consecutive_groups(excess_region.index.tolist())
                ]
                # Assign levels as outlier
                if self.Npoints == True:
                    baye_block = self.outliers(
                        excess_regions_idx,
                        df,
                        baye_block,
                        excess_region["measurements_nu"],
                    )
                    # Calculate again the excess regions
                    excess_regions_idx = [
                        list(group)
                        for group in mit.consecutive_groups(
                            baye_block[baye_block["level"] == "excess"].index.tolist()
                        )
                    ]
                    if not baye_block[baye_block["level"] == "outlier"].empty:
                        outlier = [
                            baye_block[baye_block["level"] == "outlier"][
                                "jd_measurement_start"
                            ].values,
                            baye_block[baye_block["level"] == "outlier"][
                                "jd_measurement_end"
                            ].values,
                        ]
                        output["jd_outlier"] = np.array(outlier).T.tolist()
                    else:
                        output["jd_outlier"] = None
                else:
                    output["jd_outlier"] = None
                output["description"] = self.description(
                    excess_regions_idx, excess_region["measurements_nu"]
                )

            excess_region = baye_block[baye_block["level"] == "excess"]
            baseline_region = baye_block[baye_block["level"] != "excess"]
            ##########################
            everything_except_excess_values = []
            if not excess_region.empty:
                if self.flux:
                    global_peak_idx = (
                        baye_block["mag"].loc[flatten(excess_regions_idx)].idxmax()
                    )
                else:
                    global_peak_idx = (
                        baye_block["mag"].loc[flatten(excess_regions_idx)].idxmin()
                    )
                for idx in excess_regions_idx:
                    if global_peak_idx in idx:
                        everything_except_excess_values.append(
                            df[
                                (
                                    (
                                        df["jd"]
                                        < baye_block["jd_measurement_start"].loc[idx[0]]
                                    )
                                    & (df["Outlier"] == False)
                                )
                                | (
                                    (
                                        df["jd"]
                                        > baye_block["jd_measurement_end"].loc[idx[-1]]
                                    )
                                    & (df["Outlier"] == False)
                                )
                            ]["mag"].values
                        )

                        everything_except_excess_rms = mean_squared_error(
                            list(everything_except_excess_values)[0],
                            [baseline] * len(list(everything_except_excess_values)[0]),
                            squared=False,
                        )
            else:
                everything_except_excess_rms = baseline_rms

            baseline_regions_idx = [
                list(group)
                for group in mit.consecutive_groups(baseline_region.index.tolist())
            ]
            output["sigma_from_baseline"].append(
                baye_block["sigma_from_baseline"].values.tolist()
            )
            output["baseline"].append(baseline)
            output["baseline_sigma"].append(baseline_sigma)
            # Add baseline_rms, calculate max magnitude, flux from df['mag']
            # strength: Delta flux/rms, significance: rms/baseline_sigma
            output["baseline_rms"].append(baseline_rms)
            output["significance"].append(everything_except_excess_rms / baseline_sigma)
            if self.flux == True:
                output["max_mag"].append(max(df[df["Outlier"] == False]["mag"].values))
                output["strength_sjoert"].append(
                    (max(df[df["Outlier"] == False]["mag"].values) - baseline)
                    / everything_except_excess_rms
                )
            else:
                output["max_mag"].append(min(df[df["Outlier"] == False]["mag"].values))
                output["strength_sjoert"].append(
                    (min(df[df["Outlier"] == False]["mag"].values) - baseline)
                    / everything_except_excess_rms
                )

            for idx in baseline_regions_idx:
                if baye_block["level"][idx[-1]] == "outlier":
                    if len(idx) != 1:
                        output["nu_of_baseline_blocks"].append(len(idx) - 1)
                        output["jd_baseline_regions"].append(
                            [
                                baye_block["jd_measurement_start"][idx[0]],
                                baye_block["jd_measurement_end"][idx[-1] - 1],
                            ]
                        )
                        output["mag_edge_baseline"].append(
                            [baye_block["mag_edge"][idx[-1] - 1]]
                        )
                elif baye_block["level"][idx[0]] == "outlier":
                    if len(idx) != 1:
                        output["nu_of_baseline_blocks"].append(len(idx) - 1)
                        output["jd_baseline_regions"].append(
                            [
                                baye_block["jd_measurement_start"][idx[0] + 1],
                                baye_block["jd_measurement_end"][idx[-1]],
                            ]
                        )
                        output["mag_edge_baseline"].append(
                            [baye_block["mag_edge"][idx[-1]]]
                        )
                else:
                    output["nu_of_baseline_blocks"].append(len(idx))
                    output["jd_baseline_regions"].append(
                        [
                            baye_block["jd_measurement_start"][idx[0]],
                            baye_block["jd_measurement_end"][idx[-1]],
                        ]
                    )
                    output["mag_edge_baseline"].append(
                        [baye_block["mag_edge"][idx[-1]]]
                    )

            if not excess_region.empty:
                output["nu_of_excess_regions"].append(len(excess_regions_idx))
                significance_of_fluctuation_before_peak = []
                significance_of_fluctuation_after_peak = []
                for idx in excess_regions_idx:
                    output["nu_of_excess_blocks"].append(len(idx))
                    output["jd_excess_regions"].append(
                        [
                            baye_block["jd_measurement_start"][idx[0]],
                            baye_block["jd_measurement_end"][idx[-1]],
                        ]
                    )
                    output["mag_edge_excess"].append([baye_block["mag_edge"][idx[-1]]])
                    output["max_sigma_excess_region"].append(
                        [max(baye_block["sigma_from_baseline"][idx[0] : idx[-1] + 1])]
                    )
                    output["sigma_from_baseline_excess"].append(
                        baye_block["sigma_from_baseline"][
                            idx[0] : idx[-1] + 1
                        ].values.tolist()
                    )
                    each_excess_max_idx = baye_block["mag"].loc[idx].idxmax()
                    if global_peak_idx == each_excess_max_idx:
                        #      else: #Inside the excess region with the highest intensity
                        # Calculate the local peaks inside the excess region of the highest intensity
                        local_peaks, _ = np.array(
                            find_peaks(
                                np.concatenate(
                                    (
                                        [min(baye_block["mag"].loc[idx].values)],
                                        baye_block["mag"].loc[idx].values,
                                        [min(baye_block["mag"].loc[idx].values)],
                                    )
                                )
                            ),
                            dtype=object,
                        )
                        local_peaks = local_peaks - 1
                        for peak in local_peaks:
                            if idx[peak] != global_peak_idx:
                                if idx[peak] < global_peak_idx:
                                    significance_of_fluctuation_before_peak.append(
                                        (
                                            baye_block["mag"].loc[global_peak_idx]
                                            - baye_block["mag"].loc[idx[peak]]
                                        )
                                        / math.sqrt(
                                            sum(
                                                np.array(baye_block["mag.err"].values)
                                                ** 2
                                            )
                                        )
                                    )
                                else:
                                    significance_of_fluctuation_after_peak.append(
                                        (
                                            baye_block["mag"].loc[global_peak_idx]
                                            - baye_block["mag"].loc[idx[peak]]
                                        )
                                        / math.sqrt(
                                            sum(
                                                np.array(baye_block["mag.err"].values)
                                                ** 2
                                            )
                                        )
                                    )
                        output["significance_of_fluctuation"].append(
                            significance_of_fluctuation_before_peak
                        )
                        output["significance_of_fluctuation"].append(
                            significance_of_fluctuation_after_peak
                        )
                        # Calculate the significance of each bayesian block, inside the excess region of the highest intensity
                        if len([i for i in idx if (i > global_peak_idx)]) >= 2:
                            after_peak_excess_values = df[
                                (
                                    (
                                        df["jd"]
                                        >= baye_block["jd_measurement_start"].loc[
                                            [i for i in idx if (i > global_peak_idx)][0]
                                        ]
                                    )
                                    & (df["Outlier"] == False)
                                )
                                & (
                                    (
                                        df["jd"]
                                        <= baye_block["jd_measurement_end"].loc[
                                            [i for i in idx if (i > global_peak_idx)][
                                                -1
                                            ]
                                        ]
                                    )
                                    & (df["Outlier"] == False)
                                )
                            ]["mag"].values
                            after_peak_excess_error = df[
                                (
                                    (
                                        df["jd"]
                                        >= baye_block["jd_measurement_start"].loc[
                                            [i for i in idx if (i > global_peak_idx)][0]
                                        ]
                                    )
                                    & (df["Outlier"] == False)
                                )
                                & (
                                    (
                                        df["jd"]
                                        <= baye_block["jd_measurement_end"].loc[
                                            [i for i in idx if (i > global_peak_idx)][
                                                -1
                                            ]
                                        ]
                                    )
                                    & (df["Outlier"] == False)
                                )
                            ]["mag.err"].values
                            after_peak_excess_rms = mean_squared_error(
                                list(after_peak_excess_values),
                                [np.mean(after_peak_excess_values)]
                                * len(after_peak_excess_values),
                                squared=False,
                            )
                            after_peak_excess_sigma = mean_squared_error(
                                list(after_peak_excess_values),
                                [np.mean(after_peak_excess_values)]
                                * len(after_peak_excess_values),
                                squared=True,
                            )

                            output["significance_after_peak"].append(
                                after_peak_excess_rms / after_peak_excess_sigma
                            )
                            #      output['significance_after_peak'].append(after_peak_excess_rms/baseline_sigma)
                            output["strength_after_peak"].append(
                                (
                                    baye_block["mag"].loc[global_peak_idx]
                                    - baye_block["mag"].loc[
                                        [i for i in idx if (i > global_peak_idx)][0]
                                    ]
                                )
                                / after_peak_excess_rms
                            )

                        for baye_excess_idx in idx:
                            if baye_excess_idx < global_peak_idx:
                                output["significance_of_variability_excess"][0].append(
                                    (
                                        baye_block["mag"].loc[global_peak_idx]
                                        - baye_block["mag"].loc[baye_excess_idx]
                                    )
                                    / math.sqrt(
                                        sum(np.array(baye_block["mag.err"].values) ** 2)
                                    )
                                )
                            elif baye_excess_idx > global_peak_idx:
                                output["significance_of_variability_excess"][1].append(
                                    (
                                        baye_block["mag"].loc[global_peak_idx]
                                        - baye_block["mag"].loc[baye_excess_idx]
                                    )
                                    / math.sqrt(
                                        sum(np.array(baye_block["mag.err"].values) ** 2)
                                    )
                                )

                    if self.flux == True:
                        max_idx = df["mag"][
                            df["jd"].between(
                                baye_block["jd_measurement_start"][idx[0]],
                                baye_block["jd_measurement_end"][idx[-1]],
                            )
                        ].idxmax()
                    else:
                        max_idx = df["mag"][
                            df["jd"].between(
                                baye_block["jd_measurement_start"][idx[0]],
                                baye_block["jd_measurement_end"][idx[-1]],
                            )
                        ].idxmin()
                    output["max_mag_excess_region"].append([df["mag"][max_idx]])
                    output["max_jd_excess_region"].append([df["jd"][max_idx]])

                if self.flux == True:
                    output["max_baye_block_timescale"].append(
                        float(
                            (
                                excess_region["jd_measurement_end"]
                                - excess_region["jd_measurement_start"]
                            )[
                                flatten(
                                    excess_region[
                                        excess_region["mag"]
                                        == max(excess_region["mag"])
                                    ].index.tolist()
                                )
                            ].to_string(
                                index=False
                            )
                        )
                    )
                else:
                    output["max_baye_block_timescale"].append(
                        float(
                            (
                                excess_region["jd_measurement_end"]
                                - excess_region["jd_measurement_start"]
                            )[
                                flatten(
                                    excess_region[
                                        excess_region["mag"]
                                        == min(excess_region["mag"])
                                    ].index.tolist()
                                )
                            ].to_string(
                                index=False
                            )
                        )
                    )
            else:
                output["nu_of_excess_regions"].append(0)
                output["nu_of_excess_blocks"].append(0)
                output["jd_excess_regions"].append(0)
                output["mag_edge_excess"].append(None)
                output["max_mag_excess_region"].append(0)
                output["max_jd_excess_region"].append(0)
                output["max_sigma_excess_region"].append(0)
                output["sigma_from_baseline_excess"].append(0)
                output["significance_of_variability_excess"][0].append(0)
                output["significance_of_variability_excess"][1].append(0)
                output["significance_of_fluctuation"].append(0)
                output["significance_of_fluctuation"].append(0)
                output["max_baye_block_timescale"].append(0)
                output["description"].append(None)

            output_per_filter[str(passband)] = dict(output)

            ##############################################
            ################## Plots #####################
            if self.plot:
                df["mjd"] = df["jd"] - 2400000.5
                ax.errorbar(
                    df[df["Outlier"] == False]["mjd"],
                    df[df["Outlier"] == False]["mag"],
                    yerr=df[df["Outlier"] == False]["mag.err"],
                    label=passband,
                    fmt="s",
                    color=self.PlotColor[fid - 1],
                    ecolor="lightgray",
                    markeredgecolor="black",
                    elinewidth=3,
                    capsize=0,
                )
                ax.errorbar(
                    df[df["Outlier"] == True]["mjd"],
                    df[df["Outlier"] == True]["mag"],
                    yerr=df[df["Outlier"] == True]["mag.err"],
                    label="Outlier",
                    fmt="o",
                    color="lightgray",
                    ecolor="lightgray",
                    markeredgecolor="black",
                    elinewidth=3,
                    capsize=0,
                )

                for block in output["jd_excess_regions"]:
                    if block != 0:
                        ax.axvline(
                            x=(block[0] - 40) - 2400000.5,
                            label="T2 guess",
                            color=self.PlotColor[fid - 1],
                        )
                        ax.axvline(
                            x=(block[-1] + 40) - 2400000.5,
                            color=self.PlotColor[fid - 1],
                        )
                        ax.axvspan(
                            (block[0] - 40) - 2400000.5,
                            (block[-1] + 40) - 2400000.5,
                            alpha=0.05,
                            color=self.PlotColor[fid - 1],
                        )
                    else:
                        ax.axhline(
                            y=output["baseline"],
                            label="Baseline",
                            color=self.PlotColor[fid - 1],
                        )

                for sp in ["left", "bottom", "top", "right"]:
                    ax.spines[sp].set_linewidth(3)

                ax.set_title(light_curve.stock_id, fontsize=25)
                ax.legend(
                    bbox_to_anchor=(1.05, 1),
                    loc=2,
                    borderaxespad=0.0,
                    prop={"size": 20},
                )
                plt.tick_params(axis="both", which="major", pad=12, length=10, width=3)
                plt.xticks(fontsize=20)
                plt.yticks(fontsize=20)

                locals()[str("ax") + str(passband)] = fig.add_subplot(
                    len(self.filter) + 1, 1, fid + 1, sharex=ax
                )

                if self.debug:
                    alpha = 0.07
                else:
                    alpha = 1

                locals()[str("ax") + str(passband)] = plt.errorbar(
                    df[df["Outlier"] == False]["mjd"],
                    df[df["Outlier"] == False]["mag"],
                    yerr=df[df["Outlier"] == False]["mag.err"],
                    label=passband,
                    fmt="s",
                    color=self.PlotColor[fid - 1],
                    alpha=alpha,
                    ecolor="lightgray",
                    markeredgecolor="black",
                    elinewidth=3,
                    capsize=0,
                )
                locals()[str("ax") + str(passband)] = plt.errorbar(
                    df[df["Outlier"] == True]["mjd"],
                    df[df["Outlier"] == True]["mag"],
                    yerr=df[df["Outlier"] == True]["mag.err"],
                    label="Outlier",
                    fmt="o",
                    color="lightgray",
                    ecolor="lightgray",
                    markeredgecolor="black",
                    elinewidth=3,
                    capsize=0,
                )
                baye_block["mjd_start"] = baye_block["jd_start"] - 2400000.5
                baye_block["mjd_end"] = baye_block["jd_end"] - 2400000.5

                for nu, value in enumerate(baye_block["mag"]):
                    linestyle = "solid"
                    linewidth = 1.5
                    if baye_block["level"][nu] == "excess":
                        color_block = self.PlotColor[fid - 1]
                        if self.data_type == "ztf_fp":
                            linestyle = "dashdot"
                    elif baye_block["level"][nu] == "baseline":
                        color_block = self.PlotColor[fid - 1]
                        if self.data_type == "ztf_fp":
                            linewidth = 3
                    else:
                        color_block = "lightgray"
                        if self.data_type == "ztf_fp":
                            linestyle = "dashdot"

                    locals()[str("ax") + str(passband)] = plt.hlines(
                        value,
                        baye_block["mjd_start"][nu],
                        baye_block["mjd_end"][nu],
                        color=color_block,
                        linestyles=linestyle,
                        linewidth=linewidth,
                    )
                    locals()[str("ax") + str(passband)] = plt.hlines(
                        value + baye_block["mag.err"][nu],
                        baye_block["mjd_start"][nu],
                        baye_block["mjd_end"][nu],
                        color=color_block,
                        linestyles="dashed",
                    )
                    locals()[str("ax") + str(passband)] = plt.hlines(
                        value - baye_block["mag.err"][nu],
                        baye_block["mjd_start"][nu],
                        baye_block["mjd_end"][nu],
                        color=color_block,
                        linestyles="dashed",
                    )

                if self.flux == False:
                    plt.gca().invert_yaxis()

                locals()[str("ax") + str(passband)] = plt.gca()
                for sp in ["left", "bottom", "top", "right"]:
                    locals()[str("ax") + str(passband)].spines[sp].set_linewidth(3)

                plt.tick_params(axis="both", which="major", pad=22, length=10, width=3)
                plt.xticks(fontsize=20)
                plt.yticks(fontsize=20)

        if self.plot:
            if self.flux == False and "ax" in locals():
                ax.invert_yaxis()

            if self.flux == True:
                fig.supylabel("Difference flux", fontsize=30)
            else:
                fig.supylabel("Difference magnitude", fontsize=30)

            fig.supxlabel("MJD", fontsize=30)

            if self.plot or self.debug:
                output_per_filter["Plots"] = [
                    create_plot_record(
                        fig,
                        self.plot_props,
                        extra={"band": "All", "stock": light_curve.stock_id},  # type: ignore
                        #     logger = self.logger
                    )
                ]

                if self.debug and self.debug_dir != None:
                    if self.data_type in ["ztf_alert", "ztf_fp"]:
                        object_id = self.ztfid
                    else:
                        object_id = light_curve.stock_id

                    if not os.path.exists(self.debug_dir):
                        os.makedirs(self.debug_dir)

                    fig.savefig(os.path.join(self.debug_dir, f"{object_id}.pdf"))
                    output_per_filter.pop("Plots")
                plt.close()

            fig.tight_layout()

        ##################### Further check if the bayesian excess regions of different filter coincide (Maybe move to T2) ##########################
        #################### !!! Works only for 2 filters !!!! ##########################
        coincide_peak_block = self.coincide_peak_block(output_per_filter)
        # include if you want to see how many excess regions are coincide
        #        if coincide_region >= 1:
        #            output_per_filter['coincide_regions'] = 1
        #        else:
        #            output_per_filter['coincide_regions'] = -1
        if coincide_peak_block >= 1:
            output_per_filter["coincide_peak_block"] = 1
        else:
            output_per_filter["coincide_peak_block"] = -1

        ###########################################################################################################################
        output_per_filter["start_excess"] = None
        output_per_filter["size_excess"] = 0

        for keys in list(output_per_filter.keys())[0 : len(self.filter)]:
            if output_per_filter[keys]["nu_of_excess_regions"][0] != 0:
                idxmax = np.argmax(output_per_filter[keys]["max_sigma_excess_region"])
                output_per_filter["start_excess"] = output_per_filter[keys][
                    "jd_excess_regions"
                ][idxmax][0]
                output_per_filter["size_excess"] = (
                    output_per_filter[keys]["jd_excess_regions"][idxmax][-1]
                    - output_per_filter[keys]["jd_excess_regions"][idxmax][0]
                )

        t2_output: dict[str, UBson] = output_per_filter

        if self.debug and self.data_type in ["ztf_alert", "ztf_fp"]:
            for fil in self.filter:
                print(f"{fil}\n")
                print(f"# excess regions: {t2_output[fil]['nu_of_excess_regions']}")
                print(f"# excess blocks: {t2_output[fil]['nu_of_excess_blocks']}")
                print("--------")
            print(
                f"coincident regions between g and r: {t2_output['coincide_peak_block']}"
            )

        return t2_output
