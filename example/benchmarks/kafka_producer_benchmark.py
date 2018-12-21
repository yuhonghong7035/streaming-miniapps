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


# Pilot-Streaming
import pilot.streaming
import uuid 
import time

for num_repeats in range(10):
    for num_producer_nodes in [1,2,4]:
        
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
        #dask_details={'master_url': 'tcp://c251-101:8786'}
        
        for num_broker_nodes in [1,2,4]: #,2,4
            for num_partitions_per_node in [4,8,16,32]:
                number_partitions=num_broker_nodes*num_partitions_per_node
                #for application in ["kmeans-5000", "kmeansstatic-5000", "kmeansstatic-10000", "kmeansstatic-20000", "light"]:
                for application_scenario in ["synthetic-1024", "synthetic-131072",
                                             "synthetic-262144", "synthetic-524288", "synthetic-1048576"]:    #"kmeans-5000", "synthetic-32768", "synthetic-16384", 
                    kafka_pilot_description1 = {
                        "resource":"slurm+ssh://login1.wrangler.tacc.utexas.edu",
                        "working_directory": os.path.join('/work/01131/tg804093/wrangler/', "work"),
                        "number_cores": 48*num_broker_nodes,
                        "project": "TG-MCB090174",
                        "queue": "normal",
                        "walltime": 300,
                        "type":"kafka"
                    }
                    kafka_pilot = pilot.streaming.PilotComputeService.create_pilot(kafka_pilot_description1)
                    kafka_pilot.wait()
                    kafka_details = kafka_pilot.get_details()
                    
                    time.sleep(120)
                    for npt_exponent in range(0, math.floor(math.log2(number_partitions))+1): # number_partitions to 2^6=64 [1,2,4,8,16,32,64]:
                        number_parallel_tasks=2**npt_exponent
                        number_parallel_tasks = num_producer_nodes*number_parallel_tasks
                        number_points_per_message = 5000
                        message_size = -1

                        if application_scenario.startswith("kmeans"):
                            number_points_per_message = int(application_scenario.split("-")[1])
                            application = application_scenario.split("-")[0]
                        elif application_scenario.startswith("synthetic"):
                            application = application_scenario.split("-")[0]
                            message_size = application_scenario.split("-")[1]
                        else:
                            application = application_scenario
                            number_points_per_message = 1
                        
                        number_messages=48
                        print("Run Application: %s, Number Messages: %d, Message Size: %s"%(application, number_messages, message_size))
                        run_id = str(uuid.uuid1())
                        miniapp=mass.kafka.MiniApp(
                                                     dask_scheduler=dask_details['master_url'],
                                                     resource_url=kafka_details["master_url"],
                                                     broker_service="kafka",
                                                     number_parallel_tasks=number_parallel_tasks,
                                                     #number_clusters=10, # kmeans
                                                     #number_points_per_cluster=10000, # kmeans
                                                     #number_points_per_message=number_points_per_message, # kmeans
                                                     #number_dim=3, # kmeans
                                                     number_messages=number_messages, # light, synthetic
                                                     message_size = message_size,
                                                     number_produces=1,
                                                     number_partitions=num_broker_nodes*num_partitions_per_node,
                                                     topic_name="test-"+run_id,
                                                     application_type = application
                                                    )
                        miniapp.run()
                    try:
                        kafka_pilot.cancel()
                        os.system("/home/01131/tg804093/clean_kafka.sh")
                    except:
                        pass
        try:
            dask_pilot.cancel()
            os.system("/home/01131/tg804093/clean.sh")
            time.sleep(120)
        except: 
            pass