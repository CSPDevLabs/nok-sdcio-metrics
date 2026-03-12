import os
import time
import logging
from threading import Thread
from jsonpath_ng import parse
from prometheus_client import start_http_server, Gauge, CollectorRegistry
from kubernetes import client, config, watch

# Generic Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("nok-crd-metrics")

class GenericCrdExporter:
    def __init__(self):
        # 1. Create a CLEAN registry (removes default CPU/Mem/Python metrics)
        self.registry = CollectorRegistry()
        
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

    def resolve_path(self, item, path):
        """Extracts values from K8s objects using JSONPath."""
        try:
            # Handle Ready/ConfigReady Conditions
            if "status.conditions" in path:
                conds = item.get('status', {}).get('conditions', [])
                target_type = "ConfigReady" if "ConfigReady" in path else "Ready"
                return 1 if any(c.get('type') == target_type and c.get('status') == 'True' for c in conds) else 0
            
            # Handle .length suffix manually
            is_length_query = False
            search_path = path
            if path.endswith('.length'):
                is_length_query = True
                search_path = path[:-7]

            jsonpath_expr = parse(search_path)
            matches = [match.value for match in jsonpath_expr.find(item)]
            
            if not matches:
                return 0 if is_length_query else "unknown"

            val = matches[0] # Take first match
            if is_length_query:
                return len(val) if isinstance(val, list) else 1
            return val
        except Exception:
            return 0 if "length" in path else "unknown"

    def watch_definitions(self):
        """Watcher thread: Reconciles MetricDefinition CRDs."""
        logger.info(f"Watching MetricDefinitions in: {self.namespace}")
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
                    for item in items.get('items', []):
                        labels = {lm['label']: str(self.resolve_path(item, lm['path'])) 
                                 for lm in spec['labelMappings']}
                        labels['resource_name'] = item['metadata']['name']
                        labels['resource_namespace'] = item['metadata']['namespace']
                        
                        val = self.resolve_path(item, spec['valuePath'])
                        try:
                            gauge.labels(**labels).set(float(val))
                        except:
                            gauge.labels(**labels).set(0)
                except Exception as e:
                    logger.error(f"Scrape Error [{m_name}]: {e}")
            
            time.sleep(30)

if __name__ == '__main__':
    app = GenericCrdExporter()
    Thread(target=app.watch_definitions, daemon=True).start()
    app.scrape_loop()
