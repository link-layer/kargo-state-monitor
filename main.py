"""
Kargo State Monitor

A service that monitors Kubernetes resources and records their states in Redis.
Watches: Namespaces, Deployments (ContainerApps), Secrets, and PVCs (Volumes).
"""

from collections import deque
from dataclasses import dataclass
from typing import Literal

import os
import sys
import json
import time
import signal
import logging
import datetime
import threading
import redis
import kubernetes

REDIS_URL = os.getenv('REDIS_URL')

if not REDIS_URL:
    print("REDIS_URL not set. Exit(1)")
    sys.exit(1)

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

KEY_PREFIX = 'kargo-sm'
HEALTH_KEY = 'kargo-sm:cluster:health'
HEALTH_CHECK_INTERVAL = 3  # sec

NAMESPACE_PREFIX = 'k-ns-'
APP_PREFIX = 'k-app-'
SECRET_PREFIX = 'k-secret-'
PULL_SECRET_PREFIX = 'k-pull-secret-'
PVC_PREFIX = 'k-vol-'


class RState:
    CREATING = 'creating'
    UPDATING = 'updating'
    DELETING = 'deleting'
    ACTIVE = 'active'
    STOPPED = 'stopped'
    ERROR = 'error'


# Hash keys per resource type
# Namespaces: kargo-sm:namespaces -> { nsid: state_json }
# Apps:       kargo-sm:{nsid}:apps -> { app_id: state_json }
# Secrets:    kargo-sm:{nsid}:secrets -> { secret_id: state_json }
# Volumes:    kargo-sm:{nsid}:volumes -> { pvc_id: state_json }


logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('state-monitor')

watches = []
stop_event = threading.Event()
redis_client = None
redis_available = True

QUEUE_FLUSH_INTERVAL = 5  # sec


@dataclass
class QueuedOperation:
    op_type: Literal['store', 'delete']
    resource_type: str
    resource_id: str
    nsid: str | None
    state: dict | None  # None for delete operations


pending_operations: deque[QueuedOperation] = deque()
pending_lock = threading.Lock()

NAMESPACE_PREFIX_LEN = len(NAMESPACE_PREFIX)
SECRET_PREFIX_LEN = len(SECRET_PREFIX)
PULL_SECRET_PREFIX_LEN = len(PULL_SECRET_PREFIX)
PVC_PREFIX_LEN = len(PVC_PREFIX)
APP_PREFIX_LEN = len(APP_PREFIX)


def init_redis() -> redis.Redis:
    global redis_client
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    logger.info(f"Connected to Redis")
    return redis_client


def init_kubernetes():
    try:
        kubernetes.config.load_incluster_config()
        logger.info("Loaded in-cluster Kubernetes config")
    except kubernetes.config.ConfigException:
        try:
            kubernetes.config.load_kube_config()
            logger.info("Loaded kubeconfig from file")
        except kubernetes.config.ConfigException as e:
            logger.error(f"Failed to load Kubernetes config: {e}")
            sys.exit(1)

    api_client = kubernetes.client.ApiClient()
    core_v1 = kubernetes.client.CoreV1Api(api_client)
    apps_v1 = kubernetes.client.AppsV1Api(api_client)

    return api_client, core_v1, apps_v1


def extract_namespace_id(name: str) -> str | None:
    if name.startswith(NAMESPACE_PREFIX):
        return name[NAMESPACE_PREFIX_LEN:]
    return None


def extract_app_id(name: str) -> str | None:
    if name.startswith(APP_PREFIX):
        return name[APP_PREFIX_LEN:]
    return None


def extract_secret_id(name: str) -> str | None:
    if name.startswith(PULL_SECRET_PREFIX):
        return name[PULL_SECRET_PREFIX_LEN:]
    elif name.startswith(SECRET_PREFIX):
        return name[SECRET_PREFIX_LEN:]
    return None


def extract_volume_id(name: str) -> str | None:
    if name.startswith(PVC_PREFIX):
        return name[PVC_PREFIX_LEN:]
    return None


def get_deployment_state(obj) -> dict:
    conditions = {c.type: c for c in (obj.status.conditions or [])}
    ready_replicas = obj.status.ready_replicas or 0
    desired_replicas = obj.spec.replicas or 0
    available_replicas = obj.status.available_replicas or 0

    if 'ReplicaFailure' in conditions and conditions['ReplicaFailure'].status == 'True':
        return {
            'status': RState.ERROR,
            'message': conditions['ReplicaFailure'].message,
            'ready_replicas': ready_replicas,
            'desired_replicas': desired_replicas,
        }

    if ready_replicas == 0 and desired_replicas > 0:
        status = RState.CREATING
    elif ready_replicas == desired_replicas and desired_replicas > 0:
        status = RState.ACTIVE
    elif desired_replicas == 0:
        status = RState.STOPPED
    elif conditions.get('Progressing') and conditions['Progressing'].status == 'True':
        status = RState.UPDATING
    elif ready_replicas < desired_replicas:
        status = RState.ERROR
    else:
        status = RState.ERROR

    return {
        'status': status,
        'ready_replicas': ready_replicas,
        'desired_replicas': desired_replicas,
        'available_replicas': available_replicas,
    }


def get_namespace_state(obj) -> dict:
    phase = obj.status.phase

    if phase == 'Terminating':
        return {'status': RState.DELETING}
    elif phase == 'Active':
        return {'status': RState.ACTIVE}

    return {'status': RState.ERROR, 'phase': phase}


def get_secret_state(obj) -> dict:
    return {'status': RState.ACTIVE, 'type': obj.type}


def get_pvc_state(obj) -> dict:
    phase = obj.status.phase
    capacity = obj.status.capacity or {}

    if phase == 'Pending':
        return {'status': RState.CREATING, 'phase': phase}
    elif phase == 'Bound':
        return {
            'status': RState.ACTIVE,
            'phase': phase,
            'capacity': capacity.get('storage', 'unknown'),
        }
    elif phase == 'Lost':
        return {'status': RState.ERROR, 'phase': phase, 'message': 'Volume lost'}

    return {'status': RState.ERROR, 'phase': phase}


def get_hash_key(resource_type: str, nsid: str | None) -> str:
    """
    Get the Redis hash key for a resource type.
    Namespaces use a global hash, other resources use per-namespace hashes.
    """
    if resource_type == 'namespace':
        return f"{KEY_PREFIX}:namespaces"
    return f"{KEY_PREFIX}:{nsid}:{resource_type}s"


def queue_operation(op: QueuedOperation):
    """
    Add operation to the pending queue.
    """
    with pending_lock:
        # Remove any existing operation for the same resource
        key = (op.resource_type, op.resource_id, op.nsid)
        filtered = [
            p for p in pending_operations if (p.resource_type, p.resource_id, p.nsid) != key
        ]
        pending_operations.clear()
        pending_operations.extend(filtered)
        pending_operations.append(op)
        logger.debug(
            f"Queued {op.op_type} for {op.resource_type}/{op.resource_id} (queue size: {len(pending_operations)})"
        )


def store_state(resource_type: str, resource_id: str, nsid: str | None, state: dict):
    """
    Store resource state in Redis hash. Queues operation if Redis is unavailable.
    """
    global redis_available

    if not redis_client or not resource_id:
        return

    hash_key = get_hash_key(resource_type, nsid)

    state_data = {
        **state,
        'updated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    try:
        redis_client.hset(hash_key, resource_id, json.dumps(state_data))
        redis_available = True
        logger.debug(f"Stored state for {resource_type}/{resource_id}: {state.get('status')}")

    except redis.RedisError as e:
        logger.warning(f"Redis unavailable, queueing store for {resource_type}/{resource_id}: {e}")
        redis_available = False
        queue_operation(QueuedOperation(
            op_type='store',
            resource_type=resource_type,
            resource_id=resource_id,
            nsid=nsid,
            state=state,
        ))


def delete_state(resource_type: str, resource_id: str, nsid: str | None):
    """
    Delete resource state from Redis hash. Queues operation if Redis is unavailable.
    """
    global redis_available

    if not redis_client or not resource_id:
        return

    hash_key = get_hash_key(resource_type, nsid)

    try:
        redis_client.hdel(hash_key, resource_id)
        redis_available = True
        logger.debug(f"Deleted state for {resource_type}/{resource_id}")

    except redis.RedisError as e:
        logger.warning(f"Redis unavailable, queueing delete for {resource_type}/{resource_id}: {e}")
        redis_available = False
        queue_operation(QueuedOperation(
            op_type='delete',
            resource_type=resource_type,
            resource_id=resource_id,
            nsid=nsid,
            state=None,
        ))


def update_health_status(healthy: bool, message: str = ""):
    """
    Update the cluster operational health flag in Redis.
    """
    if not redis_client:
        return

    health_data = {
        'operational': healthy,
        'message': message,
        'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    try:
        redis_client.set(HEALTH_KEY, json.dumps(health_data))
        redis_client.expire(HEALTH_KEY, HEALTH_CHECK_INTERVAL * 3)
        logger.debug(f"Health status: operational={healthy}")

    except redis.RedisError as e:
        logger.error(f"Failed to update health status: {e}")


def flush_pending_operations():
    """
    Background thread that periodically attempts to flush queued operations to Redis.
    """
    global redis_available

    while not stop_event.is_set():
        stop_event.wait(QUEUE_FLUSH_INTERVAL)

        if stop_event.is_set():
            break

        with pending_lock:
            if not pending_operations:
                continue
            # Take a snapshot of operations to process
            ops_to_process = list(pending_operations)

        if not ops_to_process:
            continue

        logger.info(f"Attempting to flush {len(ops_to_process)} queued operations to Redis")

        success_count = 0
        for op in ops_to_process:
            try:
                hash_key = get_hash_key(op.resource_type, op.nsid)

                if op.op_type == 'store' and op.state is not None:
                    state_data = {
                        **op.state,
                        'updated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    }
                    redis_client.hset(hash_key, op.resource_id, json.dumps(state_data))

                elif op.op_type == 'delete':
                    redis_client.hdel(hash_key, op.resource_id)

                # Remove from queue on success
                with pending_lock:
                    try:
                        pending_operations.remove(op)
                    except ValueError:
                        pass  # Already removed by a newer operation
                success_count += 1

            except redis.RedisError as e:
                logger.warning(f"Still unable to reach Redis: {e}")
                redis_available = False
                break  # Stop trying, will retry next interval

        if success_count > 0:
            redis_available = True
            logger.info(f"Flushed {success_count} queued operations to Redis")


def health_check_loop(api_client: kubernetes.client.ApiClient):
    """
    Continuously check K8s API health using /readyz endpoint and update Redis.
    """
    while not stop_event.is_set():
        try:
            response = api_client.call_api(
                '/readyz',
                'GET',
                _return_http_data_only=True,
                _preload_content=False
            )
            status_code = response.status

            if status_code == 200:
                update_health_status(True, "K8s API is ready")
            else:
                update_health_status(False, f"K8s API not ready: {status_code}")
                logger.warning(f"K8s API readyz check returned: {status_code}")

        except kubernetes.client.exceptions.ApiException as e:
            update_health_status(False, f"K8s API error: {e.reason}")
            logger.warning(f"K8s API health check failed: {e.reason}")

        except Exception as e:
            update_health_status(False, f"Health check error: {str(e)}")
            logger.warning(f"Health check error: {e}")

        stop_event.wait(HEALTH_CHECK_INTERVAL)


# ============================================================================
# Watchers
# ============================================================================

def watch_namespaces(core_v1: kubernetes.client.CoreV1Api):
    while not stop_event.is_set():
        w = kubernetes.watch.Watch()
        watches.append(w)
        logger.info("Starting namespace watch...")

        try:
            for event in w.stream(core_v1.list_namespace):
                if stop_event.is_set():
                    break

                obj = event['object']
                event_type = event['type']
                name = obj.metadata.name

                nsid = extract_namespace_id(name)
                if not nsid:
                    continue

                if event_type == 'DELETED':
                    delete_state('namespace', nsid, None)
                    logger.info(f"[Namespace] {name} -> deleted")
                else:
                    state = get_namespace_state(obj)
                    store_state('namespace', nsid, None, state)
                    logger.info(f"[Namespace] {name} -> {state.get('status')}")

        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 410:  # Watch expired
                logger.info("Namespace watch expired, reconnecting...")
                continue
            logger.error(f"Namespace watch error: {e}")
            time.sleep(5)

        except Exception as e:
            logger.error(f"Namespace watch error: {e}")
            time.sleep(5)


def watch_deployments(apps_v1: kubernetes.client.AppsV1Api):
    while not stop_event.is_set():
        w = kubernetes.watch.Watch()
        watches.append(w)
        logger.info("Starting deployment watch...")

        try:
            for event in w.stream(apps_v1.list_deployment_for_all_namespaces):
                if stop_event.is_set():
                    break

                obj = event['object']
                event_type = event['type']
                name = obj.metadata.name
                namespace = obj.metadata.namespace

                app_id = extract_app_id(name)
                nsid = extract_namespace_id(namespace)
                if not app_id or not nsid:
                    continue

                if event_type == 'DELETED':
                    delete_state('app', app_id, nsid)
                    logger.info(f"[Deployment] {namespace}/{name} -> deleted")
                else:
                    state = get_deployment_state(obj)
                    store_state('app', app_id, nsid, state)
                    logger.info(f"[Deployment] {namespace}/{name} -> {state.get('status')}")

        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 410:
                logger.info("Deployment watch expired, reconnecting...")
                continue
            logger.error(f"Deployment watch error: {e}")
            time.sleep(5)

        except Exception as e:
            logger.error(f"Deployment watch error: {e}")
            time.sleep(5)


def watch_secrets(core_v1: kubernetes.client.CoreV1Api):
    while not stop_event.is_set():
        w = kubernetes.watch.Watch()
        watches.append(w)
        logger.info("Starting secret watch...")

        try:
            for event in w.stream(core_v1.list_secret_for_all_namespaces):
                if stop_event.is_set():
                    break

                obj = event['object']
                event_type = event['type']
                name = obj.metadata.name
                namespace = obj.metadata.namespace

                secret_id = extract_secret_id(name)
                nsid = extract_namespace_id(namespace)
                if not secret_id or not nsid:
                    continue

                if event_type == 'DELETED':
                    delete_state('secret', secret_id, nsid)
                    logger.info(f"[Secret] {namespace}/{name} -> deleted")
                else:
                    state = get_secret_state(obj)
                    store_state('secret', secret_id, nsid, state)
                    logger.info(f"[Secret] {namespace}/{name} -> {state.get('status')}")

        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 410:
                logger.info("Secret watch expired, reconnecting...")
                continue
            logger.error(f"Secret watch error: {e}")
            time.sleep(5)

        except Exception as e:
            logger.error(f"Secret watch error: {e}")
            time.sleep(5)


def watch_pvcs(core_v1: kubernetes.client.CoreV1Api):
    while not stop_event.is_set():
        w = kubernetes.watch.Watch()
        watches.append(w)
        logger.info("Starting PVC watch...")

        try:
            for event in w.stream(core_v1.list_persistent_volume_claim_for_all_namespaces):
                if stop_event.is_set():
                    break

                obj = event['object']
                event_type = event['type']
                name = obj.metadata.name
                namespace = obj.metadata.namespace

                vol_id = extract_volume_id(name)
                nsid = extract_namespace_id(namespace)
                if not vol_id or not nsid:
                    continue

                if event_type == 'DELETED':
                    delete_state('volume', vol_id, nsid)
                    logger.info(f"[PVC] {namespace}/{name} -> deleted")
                else:
                    state = get_pvc_state(obj)
                    store_state('volume', vol_id, nsid, state)
                    logger.info(f"[PVC] {namespace}/{name} -> {state.get('status')}")

        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 410:
                logger.info("PVC watch expired, reconnecting...")
                continue
            logger.error(f"PVC watch error: {e}")
            time.sleep(5)

        except Exception as e:
            logger.error(f"PVC watch error: {e}")
            time.sleep(5)


def shutdown(sig, frame):
    logger.info("Shutdown signal received, stopping watches...")
    stop_event.set()

    for w in watches:
        try:
            w.stop()
        except:
            pass

    # Mark cluster as unknown state on shutdown
    if redis_client:
        update_health_status(False, "State monitor shutting down")

    logger.info("Shutdown complete")
    sys.exit(0)


def main():
    logger.info("Starting Kargo State Monitor")

    init_redis()
    api_client, core_v1, apps_v1 = init_kubernetes()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    update_health_status(True, "State monitor starting")  # Set initial health status

    threads = [
        threading.Thread(target=watch_namespaces, args=(core_v1,), name="ns-watcher"),
        threading.Thread(target=watch_deployments, args=(apps_v1,), name="deploy-watcher"),
        threading.Thread(target=watch_secrets, args=(core_v1,), name="secret-watcher"),
        threading.Thread(target=watch_pvcs, args=(core_v1,), name="pvc-watcher"),
        threading.Thread(target=health_check_loop, args=(api_client,), name="health-checker"),
        threading.Thread(target=flush_pending_operations, name="queue-flusher"),
    ]

    for t in threads:
        t.daemon = True
        t.start()
        logger.info(f"Started thread: {t.name}")

    # Keep main thread alive
    for t in threads:
        t.join()


if __name__ == '__main__':
    main()
