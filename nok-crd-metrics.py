import os
import time
import logging
from threading import Thread
from jsonpath_ng.ext import parse
from prometheus_client import start_http_server, Gauge, CollectorRegistry
from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException
from http.server import BaseHTTPRequestHandler, HTTPServer 
# Import for advanced client configuration
from kubernetes.client import Configuration
from kubernetes.client.api_client import ApiClient
import urllib3 # To manage connection pools

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
        self.custom_api = None # Initialize to None, will be set by _init_kube_client
        self._init_kube_client() # Initial client setup        

    def _init_kube_client(self):
        """Initializes or re-initializes the Kubernetes client."""
        logger.info("Initializing Kubernetes client...")
        try:
            # Clear any existing client configuration to force a fresh load
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config.")
        except config.ConfigException:
            logger.warning("Could not load in-cluster config, trying kubeconfig.")
            config.load_kube_config()
            logger.info("Loaded kubeconfig.")
        except Exception as e:
            logger.error(f"Failed to load Kubernetes config or initialize client: {e}")
            HEALTH_STATUS["ok"] = False
            HEALTH_STATUS["message"] = f"Client initialization failed: {e}"
            raise # Re-raise to prevent app from starting without client            

        # Create a *new* Configuration object to ensure no cached settings
        # from previous client instances are carried over.
        new_config = Configuration()
        # Copy relevant settings from the loaded default config
        new_config.host = Configuration().host
        new_config.ssl_ca_cert = Configuration().ssl_ca_cert
        new_config.api_key = Configuration().api_key
        new_config.api_key_prefix = Configuration().api_key_prefix
        new_config.verify_ssl = Configuration().verify_ssl
        new_config.cert_file = Configuration().cert_file
        new_config.key_file = Configuration().key_file

        # Explicitly create a new ApiClient with a fresh connection pool
        # This is the key part to force new connections and DNS lookups
        if self.api_client:
            # Close existing connection pool if it exists
            logger.info("Closing existing Kubernetes API client connection pool.")
            self.api_client.rest_client.pool_manager.clear()
            self.api_client.close() # Close any open connections

        self.api_client = ApiClient(configuration=new_config)
        self.custom_api = client.CustomObjectsApi(api_client=self.api_client)
        logger.info("Kubernetes CustomObjectsApi client re-initialized with new connection pool.")

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
                        self.definitions[m_name] = spec
                        logger.info(f"Reconciled metric: {m_name}")

                    elif event['type'] == 'DELETED':
                        self.definitions.pop(m_name, None)
                        logger.info(f"Deleted metric definition: {m_name}")

            except Exception as e:
                logger.error(f"Watcher Error: {e}")
                # If watcher fails, it's a critical issue, mark as unhealthy
                HEALTH_STATUS["ok"] = False
                HEALTH_STATUS["message"] = f"MetricDefinition watcher failed: {e}"
                time.sleep(10)

    def scrape_loop(self):
        """Main loop: Scrapes resources and updates gauges."""
        # Start Prometheus metrics server on port 8080
        start_http_server(8080, registry=self.registry)
        logger.info("Metrics server listening on port 8080 (Strict Mode)")

        # Start health check server on a different port, e.g., 8081
        health_server_port = 8081
        health_server = HTTPServer(('', health_server_port), HealthCheckHandler)
        Thread(target=health_server.serve_forever, daemon=True).start()
        logger.info(f"Health check server listening on port {health_server_port}")

        while True:
            # Assume healthy at the start of each scrape cycle, unless an error occurs
            # This allows recovery if RBAC issues are transiently resolved.
            # If you want it to stay FAILED until restart, remove this line.
            # For this scenario, based on user's request, we'll keep it FAILED once RBAC issue occurs.
            # HEALTH_STATUS["ok"] = True
            # HEALTH_STATUS["message"] = "All metrics are scraping successfully."

            for m_name, spec in list(self.definitions.items()):
                try:
                    res = spec['resource']
                    items = self.custom_api.list_namespaced_custom_object(
                        group=res['group'], version=res['version'],
                        namespace=self.namespace, plural=res['plural']
                    )

                    gauge = self.metrics[m_name]
                    for item in items.get('items', []):
                        labels = {lm['label']: str(self.resolve_path(item, lm['path'], is_label=True))
                                for lm in spec['labelMappings']}

                        labels['resource_name'] = item['metadata']['name']
                        labels['resource_namespace'] = item['metadata']['namespace']

                        val = self.resolve_path(item, spec['valuePath'])
                        gauge.labels(**labels).set(float(val))

                except ApiException as e:
                    if e.status == 403:
                        logger.warning(
                            f"RBAC denied for {m_name}. Marking application as unhealthy. Will retry; assuming propagation delay."
                        )
                        HEALTH_STATUS["ok"] = False
                        HEALTH_STATUS["message"] = f"RBAC denied for metric '{m_name}'. Status 403."
                        # No need to continue scraping other metrics if RBAC is broken for one.
                        # The liveness probe will pick this up and restart.
                        time.sleep(5) # Still wait to avoid busy loop in case of rapid restarts
                        break # Exit the inner loop to re-evaluate health
                    else:
                        logger.error(f"Scrape Error [{m_name}]: {e}")
                        HEALTH_STATUS["ok"] = False
                        HEALTH_STATUS["message"] = f"Scraping error for metric '{m_name}': {e}"
                        time.sleep(5)
                        break # Exit the inner loop

                except Exception as e:
                    logger.error(f"Unexpected error during scrape for [{m_name}]: {e}")
                    HEALTH_STATUS["ok"] = False
                    HEALTH_STATUS["message"] = f"Unexpected error during scrape for '{m_name}': {e}"
                    time.sleep(5)
                    break # Exit the inner loop

            time.sleep(30)


if __name__ == '__main__':
    app = GenericCrdExporter()
    app.wait_for_rbac()
    Thread(target=app.watch_definitions, daemon=True).start()
    app.scrape_loop()