#!/usr/bin/env python

# noinspection PyUnresolvedReferences
import signal
from time import sleep

import graphitesend
import os
from dotenv import load_dotenv, find_dotenv
from pyVmomi import vim
from pyVim.connect import SmartConnectNoSSL, Disconnect
import atexit


load_dotenv(find_dotenv())

graphite = graphitesend.init(
    prefix=os.getenv("GRAPHITE_PREFIX", ""),
    graphite_server=os.getenv("GRAPHITE_SERVER", "graphite"),
    group=os.getenv("GRAPHITE_GROUP", ""),
    system_name=""
)


def signal_handling(signum, frame):
    global terminated
    terminated = True


terminated = False
signal.signal(signal.SIGINT, signal_handling)


def run_esxi_metric_polling_loop(delay=10):
    esxi_connection = get_esxi_host_connection()

    print("running main loop")
    while not terminated:
        metrics = get_esxi_metrics(esxi_connection)
        graphite.send_list(metrics)
        sleep(delay)


def get_esxi_host_connection():

    # Connect to the host without SSL signing
    try:
        si = SmartConnectNoSSL(
            host=os.getenv("ESXI_HOST"),
            user=os.getenv("ESXI_USER"),
            pwd=os.getenv("ESXI_PASSWORD"),
            port=int(os.getenv("ESXI_PORT", 443)))
        atexit.register(Disconnect, si)
        return si
    except IOError:
        raise SystemExit("Unable to connect to host with supplied info.")


def get_esxi_metrics(esxi_server_connection):
    content = esxi_server_connection.RetrieveContent()
    perf_manager = content.perfManager

    # create a mapping from counter_ids to their full metric name
    counter_info = {}
    for c in perf_manager.perfCounter:
        metric_name = c.groupInfo.key + "." + c.nameInfo.key + "." + c.rollupType
        counter_info[c.key] = metric_name

    virtual_machines = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)

    metrics = []
    for vm in virtual_machines.view:

        # Get all available metric IDs for this VM
        counter_ids = [m.counterId for m in perf_manager.QueryAvailablePerfMetric(entity=vm)]

        # Using the IDs form a list of MetricId
        # objects for building the Query Spec
        metric_ids = [vim.PerformanceManager.MetricId(counterId=c, instance="*") for c in counter_ids]

        # Build the specification to be used
        # for querying the performance manager
        spec = vim.PerformanceManager.QuerySpec(maxSample=1, entity=vm, metricId=metric_ids)
        # Query the performance manager
        # based on the metrics created above
        result = perf_manager.QueryStats(querySpec=[spec])

        # Loop through the results and print the output

        # store in a string, this property involves a lookup
        vm_name = vm.summary.config.name
        # this library is doing something really fucked up but I'm just going to leave this be
        for r in result:
            for val in result[0].value:
                metrics.append((vm_name + "." + counter_info[val.id.counterId], float(str(val.value[0]))))

    return metrics


if __name__ == "__main__":
    run_esxi_metric_polling_loop()
