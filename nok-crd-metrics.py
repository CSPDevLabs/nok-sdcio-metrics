import os
import time
import logging
from threading import Thread
from jsonpath_ng import parse
from prometheus_client import start_http_server, Gauge, REGISTRY
from kubernetes import client, config, watch

# Generic Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("nok-crd-metrics")

class GenericCrdExporter:
    def __init__(self):
        # Auto-detect namespace or fallback to default
        try:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
                self.namespace = f.read().strip()
        except:
            self.namespace = os.getenv("NAMESPACE", "default")

        self.metrics = {}      # {metric_name: Gauge_Object}
        self.definitions = {}  # {metric_name: CRD_Spec}
        
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()
        
        self.custom_api = client.CustomObjectsApi()

    def resolve_path(self, item, path):
        """Extracts values from K8s objects using JSONPath."""
        try:
            # Handle special boolean logic for 'Ready' conditions
            if "status.conditions" in path and "Ready" in path:
                conds = item.get('status', {}).get('conditions', [])
                return 1 if any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in conds) else 0
            
            # Standard JSONPath
            jsonpath_expr = parse(path)
            matches = [match.value for match in jsonpath_expr.find(item)]
            if not matches: return 0 if "value" in path else "unknown"
            
            # Return length if path ends in .length, else first match
            return len(matches[0]) if path.endswith('.length') else matches[0]
        except Exception:
            return "unknown"

    def watch_definitions(self):
        """Watcher thread: Reconciles MetricDefinition CRDs in the local namespace."""
        logger.info(f"Watching MetricDefinitions in namespace: {self.namespace}")
        w = watch.Watch()
        while True:
            try:
                for event in w.stream(
                    self.custom_api.list_namespaced_custom_object,
                    group="metrics.dynamic.io", version="v1alpha1",
                    namespace=self.namespace, plural="metricdefinitions"
                ):
                    spec = event['object']['spec']
                    m_name = spec['metricName']
                    
                    if event['type'] in ['ADDED', 'MODIFIED']:
                        # Extract label keys + standard ones
                        label_keys = [lm['label'] for lm in spec['labelMappings']]
                        label_keys.extend(['resource_name', 'resource_namespace'])
                        
                        if m_name not in self.metrics:
                            self.metrics[m_name] = Gauge(m_name, spec.get('help', ''), label_keys)
                        
                        self.definitions[m_name] = spec
                        logger.info(f"Reconciled metric: {m_name}")

                    elif event['type'] == 'DELETED':
                        self.definitions.pop(m_name, None)
                        logger.info(f"Deleted metric definition: {m_name}")

            except client.exceptions.ApiException as e:
                if e.status == 403:
                    logger.error("RBAC ERROR: App lacks permission to list MetricDefinitions.")
                else:
                    logger.error(f"API Error in Watcher: {e}")
                time.sleep(10)

    def scrape_loop(self):
        """Main loop: Scrapes the resources defined by the CRDs."""
        start_http_server(8080)
        logger.info("Metrics server listening on port 8080")

        while True:
            # Iterate over a copy to avoid dictionary size changes during loop
            active_defs = list(self.definitions.items())
            
            for m_name, spec in active_defs:
                try:
                    res = spec['resource']
                    # Scrape resources (limited to the app's namespace for security)
                    items = self.custom_api.list_namespaced_custom_object(
                        group=res['group'], version=res['version'],
                        namespace=self.namespace, plural=res['plural']
                    )

                    gauge = self.metrics[m_name]
                    for item in items.get('items', []):
                        # Map labels dynamically
                        labels = {lm['label']: str(self.resolve_path(item, lm['path'])) 
                                 for lm in spec['labelMappings']}
                        labels['resource_name'] = item['metadata']['name']
                        labels['resource_namespace'] = item['metadata']['namespace']
                        
                        # Set value
                        val = self.resolve_path(item, spec['valuePath'])
                        try:
                            gauge.labels(**labels).set(float(val))
                        except (ValueError, TypeError):
                            gauge.labels(**labels).set(0)

                except client.exceptions.ApiException as e:
                    if e.status == 403:
                        logger.error(f"RBAC ERROR: Cannot list {res['plural']}. Check Role permissions.")
                    else:
                        logger.error(f"Error scraping {m_name}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error scraping {m_name}: {e}")

            time.sleep(30)

if __name__ == '__main__':
    app = GenericCrdExporter()
    # Start Watcher in background
    Thread(target=app.watch_definitions, daemon=True).start()
    # Run Scraper
    app.scrape_loop()
