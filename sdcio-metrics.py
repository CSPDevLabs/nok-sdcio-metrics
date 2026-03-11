import time
from prometheus_client import start_http_server, Gauge, REGISTRY, PROCESS_COLLECTOR, GC_COLLECTOR, PLATFORM_COLLECTOR
from kubernetes import client, config

# 1. Remove default Python/Process metrics to keep it clean
REGISTRY.unregister(PROCESS_COLLECTOR)
REGISTRY.unregister(GC_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)

# Define Prometheus metrics
# Value will now be the actual count of deviation entries
DEVIATION_COUNT = Gauge(
    'sdcio_deviation_count', 
    'Number of deviations found in the spec per target', 
    ['target_name', 'namespace', 'deviation_type']
)

def monitor_deviations():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

    custom_api = client.CustomObjectsApi()
    
    print("Starting sdcio-metrics exporter on port 8080...")
    # metrics_url: http://localhost:8080/metrics
    start_http_server(8080)

    while True:
        try:
            resource = custom_api.list_namespaced_custom_object(
                group="config.sdcio.dev",
                version="v1alpha1",
                namespace="nok-bng",
                plural="deviations"
            )
            
            # Clear old metrics to handle deleted or renamed deviations correctly
            DEVIATION_COUNT.clear()

            for item in resource.get('items', []):
                metadata = item.get('metadata', {})
                spec = item.get('spec', {})
                
                # Extract labels
                target = metadata.get('labels', {}).get('config.sdcio.dev/targetName', metadata.get('name'))
                d_type = spec.get('deviationType', 'unknown')
                namespace = metadata.get('namespace', 'nok-bng')
                
                # 2. Extract the count from spec.deviations list
                # Defaults to 0 if the deviations key is missing or empty
                deviations_list = spec.get('deviations', [])
                count = len(deviations_list)
                
                DEVIATION_COUNT.labels(
                    target_name=target, 
                    namespace=namespace, 
                    deviation_type=d_type
                ).set(count)

            time.sleep(30)
            
        except Exception as e:
            print(f"Error fetching deviations: {e}")
            time.sleep(10)

if __name__ == '__main__':
    monitor_deviations()
