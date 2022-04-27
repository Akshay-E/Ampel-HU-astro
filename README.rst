Branch containing specific tutorial/demo notebooks based on 0.8.2

Install guidelines
==================

Sample instructions for creating a conda environment

- pip3 install ipython jupyter extcats sfdmap iminuit sncosmo light-curve-python
- conda create -n ampelTutorial python=3.9
- conda activate ampelTutorial
- pip3 install git+https://github.com/AmpelProject/Ampel-ipython.git
- git clone https://github.com/AmpelProject/Ampel-HU-astro.git 
- cd Ampel-HU-astro/
- git checkout AmpelTutorial
- cd ..
- ampel config build -out ampel_conf.yaml 

The last command will create an Ampel yaml configuration file, which will be required when running any AMPEL context units.

Access tokens
=============

Access data either from the ZTF alert archive or from the live AMPEL instance requires an access token. This is created through a github auth - contact maintainers to get added to the project.
