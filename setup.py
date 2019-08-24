
from setuptools import setup

setup(name='ampel-contrib-hu',
      version='0.5.0',
      packages=['ampel.contrib.hu',
                'ampel.contrib.hu.t0',
                'ampel.contrib.hu.t2',
                'ampel.contrib.hu.t3'],
      package_data = {'': ['*.json']},
      entry_points = {
          'ampel.channels' : [
              'hu = ampel.contrib.hu.channels:load_channels',
          ],
          'ampel.target_sources' : [
              'TargetSourceListener = ampel.contrib.hu.TargetSourceListener:TargetSourceListener',
              'TargetSourceListenerSlack = ampel.contrib.hu.TargetSourceListenerSlack:TargetSourceListenerSlack',
          ],
          'ampel.t2.configs' : [
              'hu = ampel.contrib.hu.channels:load_t2_run_configs',
          ],
          'ampel.t0.units' : [
              'DecentFilter = ampel.contrib.hu.t0.DecentFilter:DecentFilter',
              'SimpleDecentFilter = ampel.contrib.hu.t0.SimpleDecentFilter:SimpleDecentFilter',	#This was just for testing at NERSC 
              'XShooterFilter = ampel.contrib.hu.t0.XShooterFilter:XShooterFilter',
              'TransientInClusterFilter = ampel.contrib.hu.t0.TransientInClusterFilter:TransientInClusterFilter',
              'LensedTransientFilter = ampel.contrib.hu.t0.LensedTransientFilter:LensedTransientFilter',
              'RandFilter = ampel.contrib.hu.t0.RandFilter:RandFilter',
              'ToOFilter = ampel.contrib.hu.t0.ToOFilter:ToOFilter',
              'SEDmTargetFilter = ampel.contrib.hu.t0.SEDmTargetFilter:SEDmTargetFilter',
              'NoFilter = ampel.contrib.hu.t0.NoFilter:NoFilter',
              'TransientInEllipticalFilter = ampel.contrib.hu.t0.TransientInEllipticalFilter:TransientInEllipticalFilter',
          ],
          'ampel.t2.units' : [
              'SNCOSMO = ampel.contrib.hu.t2.T2SNCosmo:T2SNCosmo',
              'CATALOGMATCH = ampel.contrib.hu.t2.T2CatalogMatch:T2CatalogMatch',
              'LCQUALITY = ampel.contrib.hu.t2.T2LCQuality:T2LCQuality',
              'MARSHALMONITOR = ampel.contrib.hu.t2.T2MarshalMonitor:T2MarshalMonitor',
          ],
          'ampel.t3.jobs' : [
              'hu = ampel.contrib.hu.channels:load_t3_jobs',
          ],
          'ampel.t3.units' : [
              'TransientInfoPrinter = ampel.contrib.hu.t3.TransientInfoPrinter:TransientInfoPrinter',
              'TransientViewDumper = ampel.contrib.hu.t3.TransientViewDumper:TransientViewDumper',
              'ChannelSummaryPublisher = ampel.contrib.hu.t3.ChannelSummaryPublisher:ChannelSummaryPublisher',
              'TransientWebPublisher = ampel.contrib.hu.t3.TransientWebPublisher:TransientWebPublisher',
              'SlackSummaryPublisher = ampel.contrib.hu.t3.SlackSummaryPublisher:SlackSummaryPublisher',
              'SlackAlertPublisher = ampel.contrib.hu.t3.SlackAlertPublisher:SlackAlertPublisher',
              'SkyPortalPublisher = ampel.contrib.hu.t3.SkyPortalPublisher:SkyPortalPublisher',
              'MarshalPublisher = ampel.contrib.hu.t3.MarshalPublisher:MarshalPublisher',
              'MarshalMonitor = ampel.contrib.hu.t3.T3MarshalMonitor:T3MarshalMonitor',
              'RapidBase = ampel.contrib.hu.t3.RapidBase:RapidBase',
              'RapidSedm = ampel.contrib.hu.t3.RapidSedm:RapidSedm',
              'TNSTalker = ampel.contrib.hu.t3.TNSTalker:TNSTalker',
              'TNSMatcher = ampel.contrib.hu.t3.aiotns:TNSMatcher'
          ],
          'ampel.resources' : [
              'extcats = ampel.contrib.hu.resources:extcatsURI',
              'catsHTM = ampel.contrib.hu.resources:catsHTMURI',
              'desycloud = ampel.contrib.hu.resources:desyCloudURI',
          ],
          'console_scripts' : [
              'catshtmd = ampel.contrib.hu.catshtm_server:run'
          ]
      }
)
