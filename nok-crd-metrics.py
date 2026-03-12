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
        """Extracts values using JSONPath with support for filters and length."""
        res_name = item.get('metadata', {}).get('name', 'unknown')
        try:
            is_length_query = False
            search_path = path
            if path.endswith('.length'):
                is_length_query = True
                search_path = path[:-7]

            jsonpath_expr = parse(search_path)
            matches = [match.value for match in jsonpath_expr.find(item)]
            
            # --- DEBUG LOG 1: What did JSONPath find? ---
            # logger.info(f"[DEBUG] Resource: {res_name} | Path: {path} | Matches: {matches}")

            if not matches:
                return 0 if is_length_query else "unknown"

            if is_length_query:
                # If match[0] is a list, count its items, else count the matches found
                val = len(matches[0]) if isinstance(matches[0], list) else len(matches)
                return val

            # Unpack the first match
            val = matches[0] 
            
            # --- DEBUG LOG 2: After unpacking ---
            logger.info(f"[DEBUG] Resource: {res_name} | Path: {path} | Raw Value: {val} | Type: {type(val)}")

            # Handle Booleans (K8s sometimes returns actual bools, not strings)
            if isinstance(val, bool):
                return 1 if val else 0
            
            # Handle Strings
            val_str = str(val).strip().lower()
            if val_str in ['true', 'reachable', 'enabled', 'ready', 'ok']:
                return 1
            if val_str in ['false', 'unreachable', 'disabled', 'notready', 'failed']:
                return 0

            # Numeric fallback
            try:
                return float(val)
            except (ValueError, TypeError):
                return 0
                
        except Exception as e:
            logger.error(f"Path error {path} on {res_name}: {e}")
            return 0



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
