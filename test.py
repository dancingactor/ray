import ray
from ray.cluster_utils import Cluster

cluster = Cluster()
head = cluster.add_node(num_cpus=4, num_gpus=2)
ray.init(address=cluster.address)
worker = cluster.add_node(num_cpus=4, num_gpus=2)
cluster.wait_for_nodes()
# Two nodes, each with 2 GPUs. Cluster total: 4 GPUs.

@ray.remote(num_gpus=0.6, num_cpus=0)
class Actor:
    def ready(self): return True

# Schedule 2 actors. Ray's hybrid scheduling prefers the local (head) node,
# so both land on head naturally, one per GPU.
# Head GPU state after: [0.4, 0.4], aggregate remaining = 0.8
# Worker still has [1.0, 1.0].
actors = [Actor.remote() for _ in range(2)]
ray.get([a.ready.remote() for a in actors])

# Schedule a 3rd actor (0.6 GPU). Worker can easily handle it, but:
# - Cluster scheduler sees head's aggregate 0.8 >= 0.6, picks head
# - Local allocator: no single GPU on head has >= 0.6, rejects
# - Spill back reruns scheduling, head aggregate still 0.8 >= 0.6, same result
# The actor is stuck pending even though the worker has 2 idle GPUs.
try:
    a3 = Actor.remote()
    ray.get(a3.ready.remote(), timeout=15)
    print("OK: scheduled on worker")
except ray.exceptions.GetTimeoutError:
    print("BUG: stuck pending despite worker having 2 free GPUs")