import os
import time
import logging
from threading import Thread
from jsonpath_ng.ext import parse 
from prometheus_client import start_http_server, Gauge, CollectorRegistry
from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException

# Generic Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("nok-crd-metrics")

# Add an environment variable to filter CRDs


class GenericCrdExporter:
    def __init__(self):
        # 1. Create a CLEAN registry (removes default CPU/Mem/Python metrics)
        self.registry = CollectorRegistry()
        self.app_filter = os.getenv("METRIC_APP_LABEL", "")
        
        try:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
                self.namespace = f.read().strip()
        except:
            self.namespace = os.getenv("NAMESPACE", "default")

        self.metrics = {}      
        self.definitions = {}  
        
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

            # Use the extended parser for [?(@...)]
            from jsonpath_ng.ext import parse as ext_parse
            jsonpath_expr = ext_parse(search_path)
            matches = [match.value for match in jsonpath_expr.find(item)]
            
            if not matches:
                return "unknown" if is_label else 0

            # Unpack first match
            val = matches[0]

            # --- IF IT'S A LABEL, RETURN AS STRING ---
            if is_label:
                return str(val)

            # --- IF IT'S A VALUE, NORMALIZE TO NUMBER ---
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
                # Just a simple check to see if the service account can talk to the API
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
                            # Register gauge ONLY to our clean registry
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
                time.sleep(10)

    def scrape_loop(self):
        """Main loop: Scrapes resources and updates gauges."""
        # Pass the custom registry to the server
        start_http_server(8080, registry=self.registry)
        logger.info("Metrics server listening on port 8080 (Strict Mode)")

        while True:
            for m_name, spec in list(self.definitions.items()):
                try:
                    res = spec['resource']
                    items = self.custom_api.list_namespaced_custom_object(
                        group=res['group'], version=res['version'],
                        namespace=self.namespace, plural=res['plural']
                    )

                    gauge = self.metrics[m_name]
                    # Inside scrape_loop...
                    for item in items.get('items', []):
                        # Pass is_label=True here so 'type' doesn't become '0'
                        labels = {lm['label']: str(self.resolve_path(item, lm['path'], is_label=True)) 
                                for lm in spec['labelMappings']}
                        
                        labels['resource_name'] = item['metadata']['name']
                        labels['resource_namespace'] = item['metadata']['namespace']
                        
                        # This remains is_label=False (default) for the actual gauge value
                        val = self.resolve_path(item, spec['valuePath'])
                        gauge.labels(**labels).set(float(val))

                except ApiException as e:
                    if e.status == 403:
                        logger.warning(f"RBAC denied for {m_name}. Removing from cache to trigger re-sync.")
                        self.definitions.pop(m_name, None) # Drop it so we stop looping on it
                        time.sleep(5)
                        continue
                    else:
                        logger.error(f"Scrape Error [{m_name}]: {e}")
            
            time.sleep(30)

if __name__ == '__main__':
    app = GenericCrdExporter()
    app.wait_for_rbac()
    Thread(target=app.watch_definitions, daemon=True).start()
    app.scrape_loop()
