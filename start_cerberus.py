#!/usr/bin/env python

import os
import sys
import yaml
import time
import json
import signal
import logging
import optparse
import pyfiglet
import functools
import importlib
import multiprocessing
from itertools import repeat
from datetime import datetime
from collections import defaultdict
import cerberus.server.server as server
import cerberus.inspect.inspect as inspect
import cerberus.invoke.command as runcommand
import cerberus.kubernetes.client as cerberus_kubecli
import cerberus.slack.slack_client as slackcli
import cerberus.prometheus.client as promcli
import cerberus.database.client as dbcli


def smap(f):
    return f()


# define Python user-defined exceptions
class EndedByUserException(Exception):
    "Raised when the user ends a process"
    pass


def handler(sig, frame):
    raise EndedByUserException("End process with user kill")


def init_worker():
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


# Publish the cerberus status
def publish_cerberus_status(status):
    with open("/tmp/cerberus_status", "w+") as file:
        file.write(str(status))


def print_final_status_json(iterations, cerberus_status, exit_status_code):
    status_json = {"iterations": iterations, "cluster_health": cerberus_status, "exit_status": exit_status_code}

    with open("final_cerberus_info.json", "w") as file:
        file.write(str(status_json))

    logging.info("Final status information written to final_cerberus_info.json")


# Create a json file of operation timings
def record_time(time_tracker):
    if time_tracker:
        average = defaultdict(float)
        for check in time_tracker["Iteration 1"]:
            iterations = 0
            for values in time_tracker.values():
                if check in values:
                    average[check] += values[check]
                    iterations += 1
            average[check] /= iterations
        time_tracker["Average"] = average
    with open("./time_tracker.json", "w+") as file:
        json.dump(time_tracker, file, indent=4, separators=(",", ": "))


# Main function
def main(cfg):
    # Start cerberus
    print(pyfiglet.figlet_format("cerberus"))
    logging.info("Starting ceberus")

    # Parse and read the config
    if os.path.isfile(cfg):
        with open(cfg, "r") as f:
            config = yaml.full_load(f)
        distribution = config["cerberus"].get("distribution", "openshift").lower()
        kubeconfig_path = os.path.expanduser(config["cerberus"].get("kubeconfig_path", ""))
        port = config["cerberus"].get("port", 8080)
        watch_nodes = config["cerberus"].get("watch_nodes", False)
        watch_cluster_operators = config["cerberus"].get("watch_cluster_operators", False)
        watch_namespaces = config["cerberus"].get("watch_namespaces", [])
        watch_namespaces_ignore_pattern = config["cerberus"].get("watch_namespaces_ignore_pattern", [])
        watch_terminating_namespaces = config["cerberus"].get("watch_terminating_namespaces", True)
        watch_url_routes = config["cerberus"].get("watch_url_routes", [])
        watch_master_schedulable = config["cerberus"].get("watch_master_schedulable", {})
        cerberus_publish_status = config["cerberus"].get("cerberus_publish_status", False)
        inspect_components = config["cerberus"].get("inspect_components", False)
        slack_integration = config["cerberus"].get("slack_integration", False)
        prometheus_url = config["cerberus"].get("prometheus_url", "")
        prometheus_bearer_token = config["cerberus"].get("prometheus_bearer_token", "")
        custom_checks = config["cerberus"].get("custom_checks", [])
        iterations = config["tunings"].get("iterations", 0)
        sleep_time = config["tunings"].get("sleep_time", 0)
        cmd_timeout = config["tunings"].get("timeout", 60)
        request_chunk_size = config["tunings"].get("kube_api_request_chunk_size", 250)
        daemon_mode = config["tunings"].get("daemon_mode", False)
        cores_usage_percentage = config["tunings"].get("cores_usage_percentage", 0.5)
        if "database" in config.keys():
            database_path = config["database"].get("database_path", "/tmp/cerberus.db")
            reuse_database = config["database"].get("reuse_database", False)
        else:
            database_path = "/tmp/cerberus.db"
            reuse_database = False
        # Initialize custom checks vars
        custom_checks_status = True
        custom_checks_fail_messages = []

        # Initialize clients and set kube api request chunk size
        if not os.path.isfile(kubeconfig_path):
            logging.error("Proper kubeconfig not set, please set proper kubeconfig path")
            print_final_status_json(-1, "Unknown", 1)
            sys.exit(1)
        try:
            os.environ["KUBECONFIG"] = str(kubeconfig_path)
            # krkn-lib-kubernetes init
            cerberus_kubecli.initialize_globals(kubeconfig_path, request_chunk_size, cmd_timeout)
        except Exception:
            cerberus_kubecli.initialize_globals(None, request_chunk_size, cmd_timeout)

        if "openshift-sdn" in watch_namespaces:
            sdn_namespace = cerberus_kubecli.check_sdn_namespace()
            watch_namespaces = [namespace.replace("openshift-sdn", sdn_namespace) for namespace in watch_namespaces]

        # Check if all the namespaces under watch_namespaces are valid
        watch_namespaces = cerberus_kubecli.check_namespaces(watch_namespaces)

        # Cluster info
        # Need to get clusterversion
        cv = ""
        cerberus_host_url = cerberus_kubecli.get_host()
        logging.info("Server URL: %s" % cerberus_host_url)

        # Run http server using a separate thread if cerberus is asked
        # to publish the status. It is served by the http server.
        if cerberus_publish_status:
            if not 0 <= port <= 65535:
                logging.info("Using port 8080 as %s isn't a valid port number" % (port))
                port = 8080
            address = ("0.0.0.0", port)
            server_address = address[0]
            port = address[1]
            logging.info("Publishing cerberus status at http://%s:%s" % (server_address, port))
            server.start_server(address)

        dbcli.set_db_path(database_path)
        if not os.path.isfile(database_path) or not reuse_database:
            dbcli.create_db()
            dbcli.create_table()

        # Create slack WebCleint when slack intergation has been enabled
        if slack_integration:
            slack_integration = slackcli.initialize_slack_client()

        # Run inspection only when the distribution is openshift
        if distribution == "openshift" and inspect_components:
            logging.info("Detailed inspection of failed components has been enabled")
            inspect.delete_inspect_directory()

        # Use cluster_info to get the api server url
        api_server_url = cerberus_host_url + "/healthz"

        # Counter for if api server is not ok
        api_fail_count = 0

        # Variables used for multiprocessing
        multiprocessing.set_start_method("fork")
        pool = multiprocessing.Pool(int(cores_usage_percentage * multiprocessing.cpu_count()), init_worker)
        manager = multiprocessing.Manager()
        pods_tracker = manager.dict()

        # Track time taken for different checks in each iteration
        global time_tracker
        time_tracker = {}

        # Initialize the start iteration to 0
        iteration = 0

        # Initialize the prometheus client
        promcli.initialize_prom_client(distribution, prometheus_url, prometheus_bearer_token)

        # Prometheus query to alert on high apiserver latencies
        apiserver_latency_query = r"""ALERTS{alertname="KubeAPILatencyHigh", severity="warning"}"""
        # Prometheus query to alert when etcd fync duration is high
        etcd_leader_changes_query = r"""ALERTS{alertname="etcdHighNumberOfLeaderChanges", severity="warning"}"""  # noqa

        # Set the number of iterations to loop to infinity if daemon mode is
        # enabled or else set it to the provided iterations count in the config

        if daemon_mode:
            logging.info("Daemon mode enabled, cerberus will monitor forever")
            logging.info("Ignoring the iterations set\n")
            iterations = float("inf")
        else:
            iterations = int(iterations)

        # Need to set start to cerberus_status
        cerberus_status = True
        # Loop to run the components status checks starts here
        while int(iteration) < iterations:

            try:
                # Initialize a dict to store the operations timings per iteration
                iter_track_time = manager.dict()

                # Capture the start time
                iteration_start_time = time.time()

                iteration += 1
                # Read the config for info when slack integration is enabled
                if slack_integration:
                    weekday = runcommand.invoke("date '+%A'")[:-1]
                    watcher_slack_member_ID = config["cerberus"]["watcher_slack_ID"].get(weekday, None)
                    slack_team_alias = config["cerberus"].get("slack_team_alias", None)
                    slackcli.slack_tagging(watcher_slack_member_ID, slack_team_alias)

                    if iteration == 1:
                        slackcli.slack_report_cerberus_start(cv, weekday, watcher_slack_member_ID)

                # Collect the initial creation_timestamp and restart_count of all the pods in all
                # the namespaces in watch_namespaces
                if iteration == 1:

                    pool.starmap(
                        cerberus_kubecli.namespace_sleep_tracker,
                        zip(watch_namespaces, repeat(pods_tracker), repeat(watch_namespaces_ignore_pattern)),
                    )

                # Execute the functions to check api_server_status, master_schedulable_status,
                # watch_nodes, watch_cluster_operators parallely
                (
                    (server_status),
                    (schedulable_masters),
                    (watch_nodes_status, failed_nodes),
                    (watch_cluster_operators_status, failed_operators),
                    (failed_routes),
                    (terminating_namespaces),
                ) = pool.map(
                    smap,
                    [
                        functools.partial(cerberus_kubecli.is_url_available, api_server_url),
                        functools.partial(
                            cerberus_kubecli.process_master_taint, watch_master_schedulable, iteration, iter_track_time
                        ),
                        functools.partial(cerberus_kubecli.process_nodes, watch_nodes, iteration, iter_track_time),
                        functools.partial(
                            cerberus_kubecli.process_cluster_operator,
                            distribution,
                            watch_cluster_operators,
                            iteration,
                            iter_track_time,
                        ),
                        functools.partial(cerberus_kubecli.process_routes, watch_url_routes, iter_track_time),
                        functools.partial(
                            cerberus_kubecli.monitor_namespaces_status,
                            watch_namespaces,
                            watch_terminating_namespaces,
                            iteration,
                            iter_track_time,
                        ),
                    ],
                )
                # Increment api_fail_count if api server url is not ok
                if not server_status:
                    api_fail_count += 1
                else:
                    api_fail_count = 0

                # Initialize a shared_memory of type dict to share data between different processes
                failed_pods_components = manager.dict()
                failed_pod_containers = manager.dict()

                # Monitor all the namespaces parallely
                watch_namespaces_start_time = time.time()
                pool.starmap(
                    cerberus_kubecli.process_namespace,
                    zip(
                        repeat(iteration),
                        watch_namespaces,
                        repeat(failed_pods_components),
                        repeat(failed_pod_containers),
                        repeat(watch_namespaces_ignore_pattern),
                    ),
                )

                watch_namespaces_status = False if failed_pods_components else True
                iter_track_time["watch_namespaces"] = time.time() - watch_namespaces_start_time

                # Check for the number of hits
                if cerberus_publish_status:
                    logging.info("HTTP requests served: %s \n" % (server.SimpleHTTPRequestHandler.requests_served))

                if schedulable_masters:
                    logging.warning(
                        "Iteration %s: Masters without NoSchedule taint: %s\n" % (iteration, schedulable_masters)
                    )

                # Logging the failed components
                if not watch_nodes_status:
                    logging.info("Iteration %s: Failed nodes" % (iteration))
                    logging.info("%s\n" % (failed_nodes))
                    dbcli.insert(datetime.now(), time.time(), 1, "not ready", failed_nodes, "node")

                if not watch_cluster_operators_status and distribution == "openshift" and watch_cluster_operators:
                    logging.info("Iteration %s: Failed operators" % (iteration))
                    logging.info("%s\n" % (failed_operators))
                    dbcli.insert(datetime.now(), time.time(), 1, "degraded", failed_operators, "cluster operator")

                    # Run inspection only when the inspect_components is set
                    if inspect_components:
                        # Collect detailed logs for all operators with degraded states parallely
                        pool.map(inspect.inspect_operator, failed_operators)
                        logging.info("")
                elif distribution == "kubernetes" and inspect_components:
                    logging.info("Skipping the failed components inspection as " "it's specific to OpenShift")

                if not server_status:
                    logging.info(
                        "Iteration %s: Api Server is not healthy as reported by %s\n" % (iteration, api_server_url)
                    )
                    dbcli.insert(
                        datetime.now(), time.time(), api_fail_count, "unavailable", list(api_server_url), "api server"
                    )

                if not watch_namespaces_status:
                    logging.info("Iteration %s: Failed pods and components" % (iteration))
                    for namespace, failures in failed_pods_components.items():
                        logging.info("%s: %s", namespace, failures)

                        for pod, containers in failed_pod_containers[namespace].items():
                            logging.info("Failed containers in %s: %s", pod, containers)

                        component = namespace.split("-")
                        if component[0] == "openshift":
                            component = "-".join(component[1:])
                        else:
                            component = "-".join(component)
                        dbcli.insert(datetime.now(), time.time(), 1, "pod crash", failures, component)
                    logging.info("")

                watch_teminating_ns = True
                if terminating_namespaces:
                    watch_teminating_ns = False
                    logging.info("Iteration %s: Terminating namespaces %s" % (iteration, str(terminating_namespaces)))

                # Logging the failed checking of routes
                watch_routes_status = True
                if failed_routes:
                    watch_routes_status = False
                    logging.info("Iteration %s: Failed route monitoring" % iteration)
                    for route in failed_routes:
                        logging.info("Route url: %s" % route)
                    logging.info("")
                    dbcli.insert(datetime.now(), time.time(), 1, "unavailable", failed_routes, "route")

                # Aggregate the status and publish it
                cerberus_status = (
                    watch_nodes_status
                    and watch_namespaces_status
                    and watch_cluster_operators_status
                    and server_status
                    and watch_routes_status
                    and watch_teminating_ns
                )

                if distribution == "openshift":
                    watch_csrs_start_time = time.time()
                    csrs = cerberus_kubecli.get_csrs()
                    pending_csr = []
                    for csr in csrs["items"]:
                        # find csr status
                        if "conditions" in csr["status"]:
                            if "Approved" not in csr["status"]["conditions"][0]["type"]:
                                pending_csr.append(csr["metadata"]["name"])
                        else:
                            pending_csr.append(csr["metadata"]["name"])
                    if pending_csr:
                        logging.warning("There are CSR's that are currently not approved")
                        logging.warning("Csr's that are not approved: " + str(pending_csr))
                    iter_track_time["watch_csrs"] = time.time() - watch_csrs_start_time

                if custom_checks:
                    if iteration == 1:
                        custom_checks_imports = []
                        for check in custom_checks:
                            my_check = ".".join(check.replace("/", ".").split(".")[:-1])
                            my_check_module = importlib.import_module(my_check)
                            custom_checks_imports.append(my_check_module)
                    custom_checks_fail_messages = []
                    custom_checks_status = True
                    for check in custom_checks_imports:
                        check_returns = check.main()
                        if type(check_returns) == bool:
                            custom_checks_status = custom_checks_status and check_returns
                        elif type(check_returns) == dict:
                            status = check_returns["status"]
                            message = check_returns["message"]
                            custom_checks_status = custom_checks_status and status
                            custom_checks_fail_messages.append(message)
                    cerberus_status = cerberus_status and custom_checks_status

                if cerberus_publish_status:
                    publish_cerberus_status(cerberus_status)

                # Report failures in a slack channel
                if (
                    not watch_nodes_status
                    or not watch_namespaces_status
                    or not watch_cluster_operators_status
                    or not custom_checks_status
                ):
                    if slack_integration:
                        slackcli.slack_logging(
                            cv,
                            iteration,
                            watch_nodes_status,
                            failed_nodes,
                            watch_cluster_operators_status,
                            failed_operators,
                            watch_namespaces_status,
                            failed_pods_components,
                            custom_checks_status,
                            custom_checks_fail_messages,
                        )

                # Run inspection only when the distribution is openshift
                if distribution == "openshift" and inspect_components:
                    # Collect detailed logs for all the namespaces with failed
                    # components parallely
                    pool.map(inspect.inspect_component, failed_pods_components.keys())
                    logging.info("")
                elif distribution == "kubernetes" and inspect_components:
                    logging.info("Skipping the failed components inspection as " "it's specific to OpenShift")

                # Alert on high latencies
                metrics = promcli.process_prom_query(apiserver_latency_query)
                if metrics:
                    logging.warning(
                        "Kubernetes API server latency is high. "
                        "More than 99th percentile latency for given requests to the "
                        "kube-apiserver is above 1 second.\n"
                    )
                    logging.info("%s\n" % (metrics))

                # Alert on high etcd fync duration
                metrics = promcli.process_prom_query(etcd_leader_changes_query)
                if metrics:
                    logging.warning(
                        "Observed increase in number of etcd leader elections over the last "
                        "15 minutes. Frequent elections may be a sign of insufficient resources, "
                        "high network latency, or disruptions by other components and should be "
                        "investigated.\n"
                    )
                logging.info("%s\n" % (metrics))

                # Sleep for the specified duration
                logging.info("Sleeping for the specified duration: %s\n" % (sleep_time))
                time.sleep(float(sleep_time))

                sleep_tracker_start_time = time.time()

                # Track pod crashes/restarts during the sleep interval in all namespaces parallely
                multiprocessed_output = pool.starmap(
                    cerberus_kubecli.namespace_sleep_tracker,
                    zip(watch_namespaces, repeat(pods_tracker), repeat(watch_namespaces_ignore_pattern)),
                )

                crashed_restarted_pods = {}
                for item in multiprocessed_output:
                    crashed_restarted_pods.update(item)

                iter_track_time["sleep_tracker"] = time.time() - sleep_tracker_start_time

                if crashed_restarted_pods:
                    logging.info(
                        "Pods that were crashed/restarted during the sleep interval of " "iteration %s" % (iteration)
                    )
                    for namespace, pods in crashed_restarted_pods.items():
                        distinct_pods = set(pod[0] for pod in pods)
                        logging.info("%s: %s" % (namespace, distinct_pods))
                        component = namespace.split("-")
                        if component[0] == "openshift":
                            component = "-".join(component[1:])
                        else:
                            component = "-".join(component)
                        for pod in pods:
                            if pod[1] == "crash":
                                dbcli.insert(datetime.now(), time.time(), 1, "pod crash", [pod[0]], component)
                            elif pod[1] == "restart":
                                dbcli.insert(datetime.now(), time.time(), pod[2], "pod restart", [pod[0]], component)
                    logging.info("")

                # Capture total time taken by the iteration
                iter_track_time["entire_iteration"] = (time.time() - iteration_start_time) - sleep_time  # noqa

                time_tracker["Iteration " + str(iteration)] = iter_track_time.copy()

                # Print the captured timing for each operation
                logging.info("-------------------------- Iteration Stats ---------------------------")  # noqa
                for operation, timing in iter_track_time.items():
                    logging.info("Time taken to run %s in iteration %s: %s seconds" % (operation, iteration, timing))
                logging.info("----------------------------------------------------------------------\n")  # noqa

            except EndedByUserException:
                pool.terminate()
                pool.join()
                logging.info("Terminating cerberus monitoring by user")
                record_time(time_tracker)
                print_final_status_json(iteration, cerberus_status, 0)
                sys.exit(0)

            except KeyboardInterrupt:
                pool.terminate()
                pool.join()
                logging.info("Terminating cerberus monitoring")
                record_time(time_tracker)
                print_final_status_json(iteration, cerberus_status, 1)
                sys.exit(1)

            except Exception as e:
                logging.info("Encountered issues in cluster. Hence, setting the go/no-go " "signal to false")
                logging.info("Exception: %s\n" % (e))
                if cerberus_publish_status:
                    publish_cerberus_status(False)
                    cerberus_status = False

                continue

        else:
            logging.info("Completed watching for the specified number of iterations: %s" % (iterations))
            record_time(time_tracker)
            pool.close()
            pool.join()
            if cerberus_publish_status:
                print_final_status_json(iterations, cerberus_status, 0)
                sys.exit(0)
    else:
        logging.error("Could not find a config at %s, please check" % (cfg))
        print_final_status_json(-1, "Unknown", 1)

        sys.exit(1)


if __name__ == "__main__":
    init_worker()
    # Initialize the parser to read the config
    parser = optparse.OptionParser()
    parser.add_option(
        "-c",
        "--config",
        dest="cfg",
        help="config location",
        default="config/config.yaml",
    )
    parser.add_option(
        "-o",
        "--output",
        dest="output",
        help="output report location",
        default="cerberus.report",
    )
    (options, args) = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(options.output, mode="w"), logging.StreamHandler()],
    )
    if options.cfg is None:
        logging.error("Please check if you have passed the config")
        sys.exit(1)
    else:
        main(options.cfg)
