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
import subprocess


def run_benchmark_kafka(num_broker_nodes, num_producer_nodes, num_partitions, application_scenario, number_parallel_tasks):
    
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
    
    # Wait for start of pilots
    kafka_pilot.wait()
    dask_pilot.wait()
    
    kafka_details = kafka_pilot.get_details()
    dask_details=dask_pilot.get_details()
    time.sleep(120)
    
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
    
    number_points_per_message = 5000
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
                                number_partitions=int(num_broker_nodes)*int(num_partitions),
                                topic_name="test-"+run_id,
                                application_type = application
                               )
    miniapp.run()
    try:
        kafka_pilot.cancel()
        dask_pilot.cancel()
        #time.sleep(60)
        os.system("/home/01131/tg804093/clean_kafka.sh")

    except:
        pass
    print("END run_benchmark_kafka")
    #sys.exit(0)
    os._exit(0)
       
    

def run_benchmark_kinesis(num_broker_nodes, num_producer_nodes, num_partitions, application_scenario, number_parallel_tasks):
    
    number_parallel_tasks = int(num_producer_nodes) * int(number_parallel_tasks)
    dask_pilot_description = {
        "resource": "slurm+ssh://login1.wrangler.tacc.utexas.edu",
        "working_directory": os.path.join('/work/01131/tg804093/wrangler/', "work"),
        "number_cores": 48 * num_producer_nodes,
        "project": "TG-MCB090174",
        "queue": "normal",
        "walltime": 300,
        "type": "dask"
    }
    dask_pilot = pilot.streaming.PilotComputeService.create_pilot(dask_pilot_description)
    
    pilot_compute_description = {
        "resource": "kinesis://awscloud.com",
        "number_cores": num_partitions,
        "type": "kinesis"
    }
    kinesis_pilot = pilot.streaming.PilotComputeService.create_pilot(pilot_compute_description)
    
    # Wait for both Pilots to startup
    dask_pilot.wait()
    kinesis_pilot.wait()
    
    dask_details = dask_pilot.get_details()
    kinesis_details = kinesis_pilot.get_details()
    
    time.sleep(20)
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
    number_messages = 48
    print("Run Application: %s, Number Messages: %d, Message Size: %s" % \
          (application, number_messages, message_size))
    run_id = str(uuid.uuid1())
    
    miniapp = mass.kafka.MiniApp(
        dask_scheduler=dask_details['master_url'],
        resource_url=kinesis_details["master_url"],
        broker_service="kinesis",
        number_parallel_tasks=number_parallel_tasks,
        number_clusters=10,  # kmeans
        number_points_per_cluster=10000,  # kmeans
        number_points_per_message=number_points_per_message,  # kmeans
        number_dim=3,  # kmeans
        number_messages=number_messages,  # light, synthetic
        message_size=message_size,
        number_produces=1,
        number_partitions=int(num_broker_nodes) * int(num_partitions),
        topic_name="test-" + run_id,
        application_type=application
    )
    miniapp.run()
    try:
        kinesis_pilot.cancel()
        dask_pilot.cancel()
        os.system("cd /home/01131/tg804093/ & /home/01131/tg804093/clean.sh")
        time.sleep(60)
    except:
        pass

def run_benchmark_in_background(num_broker_nodes, num_producer_nodes, num_partitions, application_scenario, number_parallel_tasks,broker):
    cmd = "python producer_benchmark.py %s %s %s %s %s %s"%(num_broker_nodes, num_producer_nodes, num_partitions, application_scenario, number_parallel_tasks, broker)
    print("Run command: %s"%cmd)
    try:
        benchmark_process = subprocess.Popen(cmd, shell=True)
        benchmark_process.wait()
        print("Finished Wait for Process")
    except:
        pass
    return


if __name__ == "__main__":
    if len(sys.argv)==7:
        # run in background
        print("Run One Benchmark Scenario")
        broker=sys.argv[6]
        if broker=="kinesis":
            print("Run Kinesis Experiment")
            run_benchmark_kinesis(num_broker_nodes=sys.argv[1],
                          num_producer_nodes=sys.argv[2],
                          num_partitions=sys.argv[3],
                          application_scenario=sys.argv[4],
                          number_parallel_tasks=sys.argv[5]
                          )
        elif broker=="kafka":            
            print("Run Kafka Experiment")
            run_benchmark_kafka(num_broker_nodes=sys.argv[1],
                          num_producer_nodes=sys.argv[2],
                          num_partitions=sys.argv[3],
                          application_scenario=sys.argv[4],
                          number_parallel_tasks=sys.argv[5]
                          )

    else:
        print("Run Benchmark Loop")
        for num_repeats in range(10):
            for num_producer_nodes in [1,2]:
                for num_broker_nodes in [1]: #,2,4
                    for num_partitions in [1,2,4,8,16,32,64]:
                        #for application in ["kmeans-5000", "kmeansstatic-5000", "kmeansstatic-10000", "kmeansstatic-20000", "light"]: "synthetic-1024",
                        for application_scenario in  ["synthetic-1024",
                                                      #"synthetic-16384",
                                                      "synthetic-131072",
                                                      #"synthetic-262144",
                                                      "synthetic-524288",
                                                      "synthetic-1048576"]:
                            for number_parallel_tasks in [1,2,4,8,16,32,64]:
                                print("Start new Benchmark Run")
                                
                                # for npt_exponent in range(0, math.floor(math.log2(num_partitions))+1):
                                # number_parallel_tasks=2**npt_exponent
                                run_benchmark_in_background(num_broker_nodes=num_broker_nodes,
                                                            num_producer_nodes=num_producer_nodes,
                                                            num_partitions=num_partitions,
                                                            application_scenario=application_scenario,
                                                            number_parallel_tasks=number_parallel_tasks,
                                                            broker="kafka")
                                
                                print("Finished Benchmark run")
                                os.system("cd /home/01131/tg804093/ & /home/01131/tg804093/clean.sh")
                                time.sleep(60)
                                #run_benchmark_kafka(num_broker_nodes=num_broker_nodes,
                                #                    num_producer_nodes=num_producer_nodes,
                                #                    num_partitions=num_partitions,
                                #                    application_scenario=application_scenario,
                                #                    number_parallel_tasks=number_parallel_tasks
                                #)