import time
from prometheus_client import start_http_server, Gauge, REGISTRY, PROCESS_COLLECTOR, GC_COLLECTOR, PLATFORM_COLLECTOR
from kubernetes import client, config

# 1. Remove default metrics
REGISTRY.unregister(PROCESS_COLLECTOR)
REGISTRY.unregister(GC_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)

# Deviation Metric
DEVIATION_COUNT = Gauge(
    'sdcio_deviation_count', 
    'Number of deviations found in the spec per target', 
    ['target_name', 'namespace', 'name', 'deviation_type']
)

# Target Status Metric
TARGET_CONFIG_READY = Gauge(
    'sdcio_target_config_ready',
    'Target ConfigReady status (1 for True, 0 for False)',
    ['name', 'namespace', 'address', 'vendor']
)

def monitor_resources():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

    custom_api = client.CustomObjectsApi()
    print("Starting sdcio-metrics exporter on port 8080...")
    start_http_server(8080)

    while True:
        try:
            # --- PROCESS DEVIATIONS ---
            deviations = custom_api.list_namespaced_custom_object(
                group="config.sdcio.dev", version="v1alpha1",
                namespace="nok-bng", plural="deviations"
            )
            DEVIATION_COUNT.clear()
            for item in deviations.get('items', []):
                meta = item.get('metadata', {})
                spec = item.get('spec', {})
                
                target = meta.get('labels', {}).get('config.sdcio.dev/targetName', meta.get('name'))
                d_type = spec.get('deviationType', 'unknown')
                name = meta.get('name', 'unknown')
                ns = meta.get('namespace', 'unknown')
                
                count = len(spec.get('deviations', []))
                DEVIATION_COUNT.labels(target_name=target, namespace=ns, name=name, deviation_type=d_type).set(count)

            # --- PROCESS TARGETS ---
            targets = custom_api.list_namespaced_custom_object(
                group="inv.sdcio.dev", version="v1alpha1",
                namespace="nok-bng", plural="targets"
            )
            TARGET_CONFIG_READY.clear()
            for item in targets.get('items', []):
                meta = item.get('metadata', {})
                spec = item.get('spec', {})
                status = item.get('status', {})
                
                name = meta.get('name', 'unknown')
                ns = meta.get('namespace', 'unknown')
                address = spec.get('address', 'unknown')
                vendor = meta.get('labels', {}).get('vendor', 'unknown')

                # Check for ConfigReady status
                is_ready = 0
                conditions = status.get('conditions', [])
                for c in conditions:
                    if c.get('type') == 'ConfigReady' and c.get('status') == 'True':
                        is_ready = 1
                        break
                
                TARGET_CONFIG_READY.labels(name=name, namespace=ns, address=address, vendor=vendor).set(is_ready)

            time.sleep(30)
            
        except Exception as e:
            print(f"Error fetching resources: {e}")
            time.sleep(10)

if __name__ == '__main__':
    monitor_resources()
