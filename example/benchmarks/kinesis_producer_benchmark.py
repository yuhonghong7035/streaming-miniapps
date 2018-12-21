# System Libraries
import sys, os
sys.path.append("..")
import pandas as pd
import numpy as np
import ast
import pykafka
import mass.kafka
import math

## logging
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger().setLevel(logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("tornado.application").setLevel(logging.CRITICAL)
logging.getLogger("distributed.utils").setLevel(logging.CRITICAL)

import boto3
boto3.setup_default_session(profile_name='dev')  

# Pilot-Streaming
import pilot.streaming
import uuid 
import time

for num_repeats in range(10):
    for num_producer_nodes in [1,2,4]:
        
        
        #dask_details={'master_url': 'tcp://c251-101:8786'}
        
        for num_broker_nodes in [1]: #,2,4
            for num_partitions in [1,2,4,8,16,32,64,128,256]:
                #for application in ["kmeans-5000", "kmeansstatic-5000", "kmeansstatic-10000", "kmeansstatic-20000", "light"]: "synthetic-1024",
                for application_scenario in  [#synthetic-131072",
                                              #"synthetic-262144", 
                                              "synthetic-524288", 
                                              "synthetic-1048576"]: 
                    
                    dask_pilot_description = {
                         "resource":"slurm+ssh://login1.wrangler.tacc.utexas.edu",
                         "working_directory": os.path.join('/work/01131/tg804093/wrangler/', "work"),
                         "number_cores": 48*num_producer_nodes,
                         "project": "TG-MCB090174",
                         "queue": "normal",
                         "walltime": 300,
                         "type":"dask"
                    }
                    dask_pilot = pilot.streaming.PilotComputeService.create_pilot(dask_pilot_description)
                    dask_pilot.wait()
                    dask_details=dask_pilot.get_details()
                    pilot_compute_description = {
                                                    "resource":"kinesis://awscloud.com",
                                                    "number_cores": num_partitions,
                                                    "type":"kinesis"
                                                }
                    kinesis_pilot = pilot.streaming.PilotComputeService.create_pilot(pilot_compute_description)
                    kinesis_pilot.wait()
                    kinesis_details=kinesis_pilot.get_details()
                    
                    time.sleep(30)
                    #for npt_exponent in range(0, math.floor(math.log2(num_partitions))+1): 
                    for number_parallel_tasks in [1,2,4,8,16,32]:
                        #number_parallel_tasks=2**npt_exponent
                        number_parallel_tasks = num_producer_nodes*number_parallel_tasks
                        number_points_per_message = 5000
                        message_size = 1
                        if application_scenario.startswith("kmeans"):
                            number_points_per_message = int(application_scenario.split("-")[1])
                            application = application_scenario.split("-")[0]
                        elif application_scenario.startswith("synthetic"):
                            application = application_scenario.split("-")[0]
                            message_size = application_scenario.split("-")[1]
                        else:
                            application = application_scenario
                        
                        number_messages=48
                        print("Run Application: %s, Number Messages: %d, Message Size: %s"%\
                              (application, number_messages, message_size))
                        run_id = str(uuid.uuid1())
                        miniapp=mass.kafka.MiniApp(
                                             dask_scheduler=dask_details['master_url'],
                                             resource_url=kinesis_details["master_url"],
                                             broker_service="kinesis",
                                             number_parallel_tasks=number_parallel_tasks,
                                             number_clusters=10, # kmeans
                                             number_points_per_cluster=10000, # kmeans
                                             number_points_per_message=number_points_per_message, # kmeans
                                             number_dim=3, # kmeans
                                             number_messages=number_messages, # light, synthetic
                                             message_size = message_size,
                                             number_produces=1,
                                             number_partitions=num_broker_nodes*num_partitions,
                                             topic_name="test-"+run_id,
                                             application_type = application
                                             )
                        miniapp.run()
                    try:
                        kinesis_pilot.cancel()
                        dask_pilot.cancel()
                        os.system("cd /home/01131/tg804093/ & /home/01131/tg804093/clean.sh")
                        time.sleep(60)
                    except:
                        pass
        try:
                     time.sleep(60)
        except: 
            pass