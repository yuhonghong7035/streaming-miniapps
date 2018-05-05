# Streaming Mini-Apps
Mini Apps for [Pilot-Streaming](thub.com/radical-cybertools/pilot-streaming)


Supported Data Productions:

* KMeans: random number of points around a given number of cluster centroids
* Lightsource: reconstruction using TomoPy
* Lightsource: CMS Beamline with Peakfinder
     
     * Data Source: https://github.com/CFN-softbio/cms_generators.git
     * Data Processing: https://github.com/CFN-softbio/SciStreams/blob/master-dask/SciStreams/start_pipeline_loop_dask.py
     * Dependencies:
         * conda install -c lightsource2-tag databroker scikit-beam
         * conda install statsmodels