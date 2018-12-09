# Streaming Mini-Apps
Mini Apps for [Pilot-Streaming](https://github.com/radical-cybertools/pilot-streaming)


Generell Dependencies:

conda install -c conda-forge pandas boto3 tomopy pykafka pyspark paramiko dask distributed 


Supported Data Productions:

* KMeans: random number of points around a given number of cluster centroids
* Lightsource: reconstruction using TomoPy
* Lightsource: CMS Beamline with Peakfinder
     
     * Data Source: https://github.com/CFN-softbio/cms_generators.git
     * Data Processing: https://github.com/CFN-softbio/SciStreams/blob/master-dask/SciStreams/start_pipeline_loop_dask.py
     * Dependencies:
         * conda install -c conda-forge tomopy
         * conda install -c lightsource2-tag databroker scikit-beam
         * conda install statsmodels
