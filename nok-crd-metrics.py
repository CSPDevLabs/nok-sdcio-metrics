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
from kubernetes.client import Configuration # Keep this for type hinting if needed, but not for manual config creation
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
        self.api_client = None # Initialize to None, will be set by _init_kube_client
        self._init_kube_client() # Initial client setup        

    def _init_kube_client(self):
        """Initializes or re-initializes the Kubernetes client, forcing new connections."""
        logger.info("Initializing Kubernetes client...")
        try:
            # Clear any existing client configuration to force a fresh load
            # This loads the in-cluster config into the *default* Configuration object
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config.")

            # Explicitly create a new ApiClient with a fresh connection pool
            # This is the key part to force new connections and DNS lookups
            if self.api_client:
                # Close existing connection pool if it exists
                logger.info("Closing existing Kubernetes API client connection pool.")
                self.api_client.rest_client.pool_manager.clear()
                self.api_client.close() # Close any open connections

            # Create a new ApiClient using the *default* Configuration object
            # which was populated by config.load_incluster_config()
            self.api_client = ApiClient() # ApiClient() without args uses the default Configuration
            self.custom_api = client.CustomObjectsApi(api_client=self.api_client)
            logger.info("Kubernetes CustomObjectsApi client re-initialized with new connection pool.")

        except config.ConfigException:
            logger.warning("Could not load in-cluster config, trying kubeconfig.")
            # If falling back to kubeconfig, ensure similar aggressive re-initialization
            config.load_kube_config()
            logger.info("Loaded kubeconfig.")
            # Re-apply the same logic for kubeconfig if needed, or simplify
            # For in-cluster, this path is less likely to be hit.
            if self.api_client:
                logger.info("Closing existing Kubernetes API client connection pool (kubeconfig path).")
                self.api_client.rest_client.pool_manager.clear()
                self.api_client.close()
            self.api_client = ApiClient() # Default ApiClient will use loaded kubeconfig
            self.custom_api = client.CustomObjectsApi(api_client=self.api_client)
            logger.info("Kubernetes CustomObjectsApi client re-initialized with new connection pool (kubeconfig path).")

        except Exception as e:
            logger.error(f"Failed to load Kubernetes config or initialize client: {e}")
            HEALTH_STATUS["ok"] = False
            HEALTH_STATUS["message"] = f"Client initialization failed: {e}"
            raise # Re-raise to prevent app from starting without client

    # ... (rest of your GenericCrdExporter class remains the same) ...
    # Ensure all calls to self.custom_api are preceded by a check:
    # if not self.custom_api: self._init_kube_client()

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
                # Ensure client is initialized before making calls
                if not self.custom_api:
                    self._init_kube_client()

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
            except Exception as e:
                logger.error(f"General error during RBAC check: {e}")
                raise

    def watch_definitions(self):
        """Watcher thread: Reconciles MetricDefinition CRDs."""
        logger.info(f"Watching MetricDefinitions in: {self.namespace}")
        w = watch.Watch()
        while True:
            try:
                # Ensure client is initialized before making calls
                if not self.custom_api:
                    self._init_kube_client()

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

            except ApiException as e:
                if e.status == 403:
                    logger.warning(f"RBAC denied for MetricDefinition watcher (403). Re-initializing client and retrying...")
                    self._init_kube_client() # Force client re-initialization
                    HEALTH_STATUS["ok"] = False
                    HEALTH_STATUS["message"] = f"MetricDefinition watcher RBAC denied: {e}"
                    time.sleep(10)
                else:
                    logger.error(f"Watcher API Error: {e}")
                    HEALTH_STATUS["ok"] = False
                    HEALTH_STATUS["message"] = f"MetricDefinition watcher failed: {e}"
                    time.sleep(10)
            except Exception as e:
                logger.error(f"Watcher General Error: {e}")
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
            HEALTH_STATUS["ok"] = True
            HEALTH_STATUS["message"] = "All metrics are scraping successfully."

            for m_name, spec in list(self.definitions.items()):
                try:
                    res = spec['resource']
                    # Ensure client is initialized before making calls
                    if not self.custom_api:
                        self._init_kube_client()

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
                            f"RBAC denied for {m_name}. Marking application as unhealthy. Re-initializing client and retrying..."
                        )
                        self._init_kube_client() # Force client re-initialization
                        HEALTH_STATUS["ok"] = False
                        HEALTH_STATUS["message"] = f"RBAC denied for metric '{m_name}'. Status 403. Client re-initialized."
                        time.sleep(5)
                        break
                    else:
                        logger.error(f"Scrape API Error [{m_name}]: {e}")
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