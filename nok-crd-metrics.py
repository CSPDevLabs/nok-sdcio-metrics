import os
import time
import logging
from threading import Thread
from jsonpath_ng.ext import parse
from prometheus_client import start_http_server, Gauge, CollectorRegistry
from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException
from http.server import BaseHTTPRequestHandler, HTTPServer # Import for health endpoint

# Generic Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("nok-crd-metrics")

# Global health status
HEALTH_STATUS = {"ok": True, "message": "All metrics are scraping successfully."}

class HealthCheckHandler(BaseHTTPRequestHandler):
    """
    A simple HTTP handler for the /healthy endpoint.
    """
    def do_GET(self):
        if self.path == '/healthy':
            if HEALTH_STATUS["ok"]:
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"OK")
            else:
                self.send_response(500)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(f"FAILED: {HEALTH_STATUS['message']}".encode('utf-8'))
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Not Found")

    def log_message(self, format, *args):
        # Suppress HTTP server access logs to keep main app logs clean
        pass

class GenericCrdExporter:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.app_filter = os.getenv("METRIC_APP_LABEL", "")

        try:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
                self.namespace = f.read().strip()
        except:
            self.namespace = os.getenv("NAMESPACE", "default")

        self.metrics = {}
        self.definitions = {}
        # Store a mapping from metric name to a set of active label combinations
        # Each combination is stored as a tuple of label values for hashability
        self.active_metric_labels = {} 

        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        self.custom_api = client.CustomObjectsApi()

    def resolve_path(self, item, path, is_label=False):
        """Extracts values. Labels stay strings, values become 1/0/float."""
        res_name = item.get('metadata', {}).get('name', 'unknown')
        try:
            is_length_query = False
            search_path = path
            if path.endswith('.length'):
                is_length_query = True
                search_path = path[:-7]

            from jsonpath_ng.ext import parse as ext_parse
            jsonpath_expr = ext_parse(search_path)
            matches = [match.value for match in jsonpath_expr.find(item)]

            if not matches:
                return "unknown" if is_label else 0

            val = matches[0]

            if is_label:
                return str(val)

            if is_length_query:
                return float(len(val)) if isinstance(val, list) else float(len(matches))

            if isinstance(val, bool):
                return 1.0 if val else 0.0

            val_str = str(val).strip().lower()
            if val_str in ['true', 'reachable', 'enabled', 'ready', 'ok']:
                return 1.0
            if val_str in ['false', 'unreachable', 'disabled', 'notready', 'failed']:
                return 0.0

            try:
                return float(val)
            except:
                return 0.0

        except Exception as e:
            logger.error(f"Path error {path} on {res_name}: {e}")
            return "error" if is_label else 0.0

    def wait_for_rbac(self):
        logger.info("Waiting for basic RBAC connectivity...")
        while True:
            try:
                self.custom_api.list_namespaced_custom_object(
                    group="metrics.dynamic.io",
                    version="v1alpha1",
                    namespace=self.namespace,
                    plural="metricdefinitions",
                    limit=1
                )
                logger.info("Basic RBAC permissions confirmed")
                return
            except ApiException as e:
                if e.status == 403:
                    logger.warning("ServiceAccount cannot list MetricDefinitions yet. Retrying...")
                    time.sleep(5)
                else:
                    raise

    def watch_definitions(self):
        """Watcher thread: Reconciles MetricDefinition CRDs."""
        logger.info(f"Watching MetricDefinitions in: {self.namespace}")
        w = watch.Watch()
        while True:
            try:
                for event in w.stream(
                    self.custom_api.list_namespaced_custom_object,
                    group="metrics.dynamic.io", version="v1alpha1",
                    namespace=self.namespace, plural="metricdefinitions",
                    label_selector=f"metrics-app={self.app_filter}" if self.app_filter else None
                ):
                    spec = event['object']['spec']
                    m_name = spec['metricName']

                    if event['type'] in ['ADDED', 'MODIFIED']:
                        label_keys = [lm['label'] for lm in spec['labelMappings']]
                        label_keys.extend(['resource_name', 'resource_namespace'])

                        if m_name not in self.metrics:
                            self.metrics[m_name] = Gauge(
                                m_name, spec.get('help', ''), label_keys,
                                registry=self.registry
                            )
                            # Initialize the set for tracking active labels for this new metric
                            self.active_metric_labels[m_name] = set() 
                        self.definitions[m_name] = spec
                        logger.info(f"Reconciled metric: {m_name}")

                    elif event['type'] == 'DELETED':
                        # When a metric definition is deleted, remove its gauge and clear associated labels
                        if m_name in self.metrics:
                            # The Prometheus client library doesn't offer a direct way to unregister a Gauge
                            # or clear all its series from the registry. The common approach is to
                            # stop updating it and let it eventually be garbage collected if no longer referenced.
                            # For a clean slate, we remove the Gauge from our internal tracking and clear its labels.
                            del self.metrics[m_name]
                            self.active_metric_labels.pop(m_name, None) # Clear all tracked labels for this metric
                        self.definitions.pop(m_name, None)
                        logger.info(f"Deleted metric definition: {m_name}")

            except Exception as e:
                logger.error(f"Watcher Error: {e}")
                HEALTH_STATUS["ok"] = False
                HEALTH_STATUS["message"] = f"MetricDefinition watcher failed: {e}"
                time.sleep(10)

    def scrape_loop(self):
        """Main loop: Scrapes resources and updates gauges."""
        start_http_server(8080, registry=self.registry)
        logger.info("Metrics server listening on port 8080 (Strict Mode)")

        health_server_port = 8081
        health_server = HTTPServer(('', health_server_port), HealthCheckHandler)
        Thread(target=health_server.serve_forever, daemon=True).start()
        logger.info(f"Health check server listening on port {health_server_port}")

        while True:
            # Create a temporary dictionary to store labels for currently scraped items in this cycle
            # This will be used to compare against previously active labels.
            current_scrape_labels_for_metrics = {m_name: set() for m_name in self.definitions.keys()}

            for m_name, spec in list(self.definitions.items()):
                try:
                    res = spec['resource']
                    items = self.custom_api.list_namespaced_custom_object(
                        group=res['group'], version=res['version'],
                        namespace=self.namespace, plural=res['plural']
                    )

                    gauge = self.metrics[m_name]
                    # Dynamically determine the full list of label keys for this metric
                    label_keys_for_gauge = [lm['label'] for lm in spec['labelMappings']]
                    label_keys_for_gauge.extend(['resource_name', 'resource_namespace'])

                    for item in items.get('items', []):
                        labels = {lm['label']: str(self.resolve_path(item, lm['path'], is_label=True))
                                for lm in spec['labelMappings']}

                        labels['resource_name'] = item['metadata']['name']
                        labels['resource_namespace'] = item['metadata']['namespace']

                        val = self.resolve_path(item, spec['valuePath'])
                        gauge.labels(**labels).set(float(val))
                        
                        # Add the tuple of label values to the current scrape set for this metric
                        # Ensure the order of values matches the order of keys for consistent tuple creation
                        label_values_tuple = tuple(labels[key] for key in label_keys_for_gauge)
                        current_scrape_labels_for_metrics[m_name].add(label_values_tuple)

                    # After scraping all items for this metric, identify and remove any stale metrics
                    # Only proceed if there were previously active labels for this metric
                    if m_name in self.active_metric_labels:
                        stale_labels_tuples = self.active_metric_labels[m_name] - current_scrape_labels_for_metrics[m_name]
                        for label_values_tuple in stale_labels_tuples:
                            # Reconstruct the dictionary of labels to pass to gauge.remove()
                            stale_label_dict = dict(zip(label_keys_for_gauge, label_values_tuple))
                            gauge.remove(**stale_label_dict)
                            logger.debug(f"Removed stale metric for {m_name} with labels: {stale_label_dict}")

                    # Update the active_metric_labels for the next cycle
                    self.active_metric_labels[m_name] = current_scrape_labels_for_metrics[m_name]

                except ApiException as e:
                    if e.status == 403:
                        logger.warning(
                            f"RBAC denied for {m_name}. Marking application as unhealthy. Will retry; assuming propagation delay."
                        )
                        HEALTH_STATUS["ok"] = False
                        HEALTH_STATUS["message"] = f"RBAC denied for metric '{m_name}'. Status 403."
                        time.sleep(5)
                        break
                    else:
                        logger.error(f"Scrape Error [{m_name}]: {e}")
                        HEALTH_STATUS["ok"] = False
                        HEALTH_STATUS["message"] = f"Scraping error for metric '{m_name}': {e}"
                        time.sleep(5)
                        break

                except Exception as e:
                    logger.error(f"Unexpected error during scrape for [{m_name}]: {e}")
                    HEALTH_STATUS["ok"] = False
                    HEALTH_STATUS["message"] = f"Unexpected error during scrape for '{m_name}': {e}"
                    time.sleep(5)
                    break

            time.sleep(30)


if __name__ == '__main__':
    app = GenericCrdExporter()
    app.wait_for_rbac()
    Thread(target=app.watch_definitions, daemon=True).start()
    app.scrape_loop()