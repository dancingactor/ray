# PR Description
[WIP][core] Fix GPU fractional scheduling by tracking per-instance availability ray-project/ray#62005
Open • Yicheng-Lu-llll wants to merge 8 commits into master from fix-gpu-fractional-scheduling • about 4 days ago
+798 -372 • ✓ Checks passing
2 👀
Reviewers: cursor (Commented), dancingactor (Commented), gemini-code-assist (Commented),  (Requested)
Labels: core, go


## Background & Problem                                                                             
                                                                                                   
In short, the issue is essentially the different checking logic between scheduling (selects a node  
with "sufficient resources" from all nodes) and local allocation (allocates specific GPU instances  
on the selected node).                                                                              
                                                                                                   
More specifically, for unit resources like GPU, the accurate resource usage representation needs to 
fully show the GPU topology. For example,  [0.6, 0.6]  represents a node with 2 GPUs, each using    
0.6. So when you want to run a task with 0.8 GPUs, there is no sufficient room to fit even though   
the aggregate available is exactly 0.8. This is unlike CPU, where we already have lots of           
functionality in OS to slice CPU and can just represent it as a scalar value.                       
                                                                                                   
However, currently, at the scheduling level, Ray always uses scalars to represent resource usage. At
the allocation level, for unit-instance resources like GPU, it uses a vector to represent GPU       
topology and do allocation. This inconsistency causes problems. Here is a minimal reproduction      
script to demonstrate (you could also see #52133 and #54729 for more details):                      
                                                                                                   
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
                                                                                                   
In this example, the actor gets stuck in an infinite spill back loop because the scheduler keeps    
picking the fragmented head node.                                                                   
                                                                                                   
Actually this is also a big problem in k8s. The different logic between k8s scheduler and allocation
logic in kubelet causes scheduling failures, and was an issue at my previous company since these two
parts are managed by different teams. The solution is always trying to unify them and ensure no big 
perf regression.                                                                                    
                                                                                                   
If we think deeper, the root cause is that the logic in scheduling and allocation is different, and 
this causes a lot of problems. So this PR is essentially to make sure scheduling and allocation use 
the same computing logic.                                                                           
                                                                                                   
## Solution                                                                                         
                                                                                                   
You might think this is simple! We could either:                                                    
                                                                                                   
• When local allocation fails, exclude the local node and reschedule to another node.               
• Change unit resource to also use per-instance vectors, the same as allocation.                    
                                                                                                   
Approach 1 is not ideal. When multiple nodes are fragmented, it could cause a ping-pong infinite    
loop                                                                                                
since each node passes the aggregate check but fails local allocation.                              
                                                                                                   
So let's focus on changing unit resources to also use per-instance vectors.                         
                                                                                                   
## Changes                                                                                          
                                                                                                   
And here I summarized the changes that needs to be aware:                                           
                                                                                                   
1. Unify all scheduling policies to use the same resource sufficiency check                         
                                                                                                   
Ray has 8+ scheduling policies (Hybrid, Spread, NodeAffinity, PG bundle, etc.), each with its own   
resource checking logic. My previous PR #59278 consolidated all scheduling policies to use          
NodeResources::IsAvailable()  as the single entry point. This is exactly where the root cause lies --
IsAvailable()  uses aggregate scalars and cannot detect fragmentation.                              
                                                                                                   
2. Make raylet scheduling aware of unit resource (GPU) topology                                     
                                                                                                   
•  NodeResources.available  changed from scalar to per-instance vector. This is the core structural 
change that lets the scheduler see each GPU instance's remaining capacity.                          
• Extracted  CanAllocate  so that scheduling checks and local allocation use the same per instance  
logic for each resource type. Given that syncer keeps the resource view eventually consistent, if   
the scheduler says a node has enough resources, the allocator is eventually guaranteed to agree,    
there is no infinite loop.                                                                          
                                                                                                   
3. Make RaySyncer carry unit resource (GPU) topology in sync messages                               
4. Make GCS (for PG scheduling) aware of unit resource (GPU) topology and maintain best-effort      
resource topology view in GCS cache                                                                 
                                                                                                   
This is the most complex part. Let me first briefly describe how PGs get scheduled:                 
                                                                                                   
• GCS maintains a separate resource view, periodically updated by RaySyncer.                        
• Acquire: GCS speculatively deducts resources (checks if the node has enough). If sufficient, sends
Prepare RPC to that node and deducts resources in resource view.                                    
• Commit: If the node replies success, creates PG formatted resources (GPU_group_xxx, etc.).        
• Return: Several cases:                                                                            
   • Restore resources when Prepare or Commit fails.                                                 
   • Restore resources when a PG is removed.                                                         
• Note: Between Acquire and Commit, the resource view may be overwritten by syncer.                 
                                                                                                   
So the difficulty is that we now need to maintain GPU topology (per-instance) in the GCS resource   
view as accurately as possible, making our best effort, so that we get better scheduling quality.   
                                                                                                   
PG scheduling is very similar to k8s scheduling: we have centralized scheduling, optimistically     
estimate and speculatively deduct resources, then rely on k8s events (in Ray, this is RaySyncer) to 
correct any inaccurate resource estimates.                                                          
                                                                                                   
Following the same philosophy, we optimistically assume that the GCS resource view and each node's  
actual state are consistent. If there is any divergence, we rely on syncer to reconcile it.         
                                                                                                   
Then things become simple. The changes for each PG operation:                                       
                                                                                                   
• Acquire: When GCS speculatively deducts resources, we save the simulated allocation, e.g. GPU [1, 
0].                                                                                                 
• Commit: We optimistically assume the allocation computed during Acquire (e.g. [1, 0]) is correct, 
and use it to create PG formatted resources (GPU_group_xxx, etc.).                                  
• Return on Prepare/Commit failure: Since we know how GCS deducted the resources, we should add them
back the same way. So we naturally use the allocation saved during Acquire (e.g. [1, 0]) to restore.
We know that RaySyncer may have overwritten the resource view in between, so the restoration may not
be perfectly accurate. But this is the optimistic scheduling model.                                 
• Return on PG removal: At this point we no longer have the Acquire allocation info -- it's gone    
after PG scheduling completes. So here, I chose to do nothing. We have no allocation info and can't 
add resources back. Recomputing is not viable because the resource view at the time of Acquire is   
different from now. I'd rather do nothing and let RaySyncer automatically correct the state.        
• For GCS restart, since we lost all saved info, we just rely one ray syncer to corret resource view.
                                                                                                   
5. Fix STRICT_PACK scheduling policy for per instance available                                     
                                                                                                   
STRICT_PACK puts all bundles on one node. Previously it aggregated all bundle                       
demands into one request. This aggregation is problematic: e.g. node with GPU=[0.7, 0.3], two       
bundles each need 0.5 GPU.                                                                          
Aggregate 1.0 >= 1.0 passes (note: this aggregation happens inside the STRICT_PACK policy itself,   
not in the general scheduling checking logic), but actual per bundle allocation fails (no single    
instance has 0.5 free for the second bundle). Causes unnecessary reschedules.                       
                                                                                                   
We already have per instance checking logic from the previous changes. The extra work here is that  
for this policy, we try allocating each bundle one by one and rollback after, instead of aggregating
all bundle demands into one request.                                                                
                                                                                                   
6. Performance optimizations                                                                        
                                                                                                   
GCS rebuilds the per instance available from syncer every ~100ms. I added flags to the rebuild      
functions so that GCS skips building data that only the raylet needs. Based on my performance       
benchmark, this improves one sync from ~60ms to ~8ms under large instance vector(200) scenria.      
                                                                                                   
## Performance Testing                                                                              
                                                                                                   
Benchmarked 4 scheduling paths comparing branch vs master, focusing on two things:                  
                                                                                                   
• The impact of increasingly longer per instance vectors (8 to 200 elements) on scheduling.         
• Whether the scheduling checking logic change itself introduces overhead.                          
                                                                                                   
Config: 200 PGs, 0.04 GPU/CPU per bundle, 5 nodes, STRICT_SPREAD.                                   
                                                                                                   
GPU per node                   │ GPU (Branch/Master)            │ CPU control (Branch/Master)      
────────────────────────────────┼────────────────────────────────┼────────────────────────────────  
8                              │ 1.01x                          │ 0.98x                            
16                             │ 1.04x                          │ 1.00x                            
32                             │ 1.13x                          │ 0.98x                            
64                             │ 1.18x                          │ 0.94x                            
128                            │ 1.24x                          │ 0.99x                            
200                            │ 1.44x                          │ 1.04x                            
                                                                                                   
No regression for all CPU cases. Since CPU is always scalar, this isolates that the scheduling      
checking logic change itself has no overhead.                                                       
                                                                                                   
No regression for actor in PG, regular actor, and regular task at all counts for both GPU and CPU.  
                                                                                                   
For PG scheduling with GPU, the bottleneck is syncer data volume growing with longer per instance   
vectors, which saturates the GCS thread. Real world clusters (8 to 16 GPU) show no regression.      


———————— Not showing 1 comment ————————


cursor commented • 1d • Newest comment

                                                                                                   
Cursor Bugbot has reviewed your changes and found 1 potential issue. 

# PR diff
diff --git a/python/ray/tests/test_placement_group_mini_integration.py b/python/ray/tests/test_placement_group_mini_integration.py
index e89e6e988eed..9438506244c4 100644
--- a/python/ray/tests/test_placement_group_mini_integration.py
+++ b/python/ray/tests/test_placement_group_mini_integration.py
@@ -21,26 +21,21 @@ def run_mini_integration_test(cluster, pg_removal=True, num_pgs=999):
     resource_quantity = num_pgs
     num_nodes = 5
     custom_resources = {"pg_custom": resource_quantity}
-    # Create pg that uses 1 resource of cpu & custom resource.
     num_pg = resource_quantity
 
     # TODO(sang): Cluster setup. Remove when running in real clusters.
     nodes = []
     for _ in range(num_nodes):
-        nodes.append(
-            cluster.add_node(
-                num_cpus=3, num_gpus=resource_quantity, resources=custom_resources
-            )
-        )
+        nodes.append(cluster.add_node(num_cpus=3, resources=custom_resources))
     cluster.wait_for_nodes()
     num_nodes = len(nodes)
 
     ray.init(address=cluster.address)
     while not ray.is_initialized():
         time.sleep(0.1)
-    bundles = [{"GPU": 1, "pg_custom": 1}] * num_nodes
+    bundles = [{"pg_custom": 1}] * num_nodes
 
-    @ray.remote(num_cpus=0, num_gpus=1, max_calls=0)
+    @ray.remote(num_cpus=0, resources={"pg_custom": 1}, max_calls=0)
     def mock_task():
         time.sleep(0.1)
         return True
diff --git a/src/ray/common/scheduling/cluster_resource_data.cc b/src/ray/common/scheduling/cluster_resource_data.cc
index 79ef68391996..2eaabdb8e6f2 100644
--- a/src/ray/common/scheduling/cluster_resource_data.cc
+++ b/src/ray/common/scheduling/cluster_resource_data.cc
@@ -54,7 +54,8 @@ NodeResources ResourceMapToNodeResources(
     const absl::flat_hash_map<std::string, std::string> &node_labels) {
   NodeResources node_resources;
   node_resources.total = NodeResourceSet(resource_map_total);
-  node_resources.available = NodeResourceSet(resource_map_available);
+  node_resources.available =
+      NodeResourceInstanceSet(NodeResourceSet(resource_map_available));
   node_resources.labels = node_labels;
   return node_resources;
 }
@@ -66,7 +67,7 @@ float NodeResources::CalculateCriticalResourceUtilization() const {
     if (cur_total == 0) {
       continue;
     }
-    auto cur_available = this->available.Get(ResourceID(i)).Double();
+    auto cur_available = this->available.Sum(ResourceID(i)).Double();
     float utilization = 1 - (cur_available / cur_total.Double());
     if (utilization > highest) {
       highest = utilization;
@@ -88,7 +89,7 @@ bool NodeResources::IsAvailable(const ResourceRequest &resource_request,
     return false;
   }
 
-  return this->available >= resource_request.GetResourceSet();
+  return this->available.CanAllocate(resource_request.GetResourceSet());
 }
 
 bool NodeResources::IsFeasible(const ResourceRequest &resource_request) const {
diff --git a/src/ray/common/scheduling/cluster_resource_data.h b/src/ray/common/scheduling/cluster_resource_data.h
index ff981b1b8ef2..a21849996143 100644
--- a/src/ray/common/scheduling/cluster_resource_data.h
+++ b/src/ray/common/scheduling/cluster_resource_data.h
@@ -311,7 +311,7 @@ class NodeResources {
   explicit NodeResources(const NodeResourceSet &resources)
       : total(resources), available(resources) {}
   NodeResourceSet total;
-  NodeResourceSet available;
+  NodeResourceInstanceSet available;
   /// Only used by light resource report.
   ResourceSet load;
 
@@ -339,11 +339,9 @@ class NodeResources {
   /// of each resource and return the highest.
   float CalculateCriticalResourceUtilization() const;
   /// Returns true if the node has the available resources to run the task.
-  /// Note: This doesn't account for the binpacking of unit resources.
   bool IsAvailable(const ResourceRequest &resource_request,
                    bool ignore_at_capacity = false) const;
   /// Returns true if the node's total resources are enough to run the task.
-  /// Note: This doesn't account for the binpacking of unit resources.
   bool IsFeasible(const ResourceRequest &resource_request) const;
   // Returns true if the node's labels satisfy the label selector requirement.
   bool HasRequiredLabels(const LabelSelector &label_selector) const;
diff --git a/src/ray/common/scheduling/resource_instance_set.cc b/src/ray/common/scheduling/resource_instance_set.cc
index e64d9b329aab..ab590be892b3 100644
--- a/src/ray/common/scheduling/resource_instance_set.cc
+++ b/src/ray/common/scheduling/resource_instance_set.cc
@@ -14,6 +14,7 @@
 
 #include "ray/common/scheduling/resource_instance_set.h"
 
+#include <algorithm>
 #include <cmath>
 #include <sstream>
 #include <string>
@@ -49,24 +50,26 @@ bool NodeResourceInstanceSet::Has(ResourceID resource_id) const {
 void NodeResourceInstanceSet::Remove(ResourceID resource_id) {
   resources_.erase(resource_id);
 
-  // Remove from the pg_indexed_resources_ as well
-  auto data = ParsePgFormattedResource(resource_id.Binary(),
-                                       /*for_wildcard_resource=*/false,
-                                       /*for_indexed_resource=*/true);
-  if (data) {
-    ResourceID original_resource_id(data->original_resource);
-
-    auto pg_resource_map_it = pg_indexed_resources_.find(original_resource_id);
-    if (pg_resource_map_it != pg_indexed_resources_.end()) {
-      auto resource_set_it = pg_resource_map_it->second.find(data->group_id);
-
-      if (resource_set_it != pg_resource_map_it->second.end()) {
-        resource_set_it->second.erase(resource_id);
-        if (resource_set_it->second.empty()) {
-          pg_resource_map_it->second.erase(data->group_id);
-        }
-        if (pg_resource_map_it->second.empty()) {
-          pg_indexed_resources_.erase(original_resource_id);
+  if (track_pg_index_) {
+    // Remove from the pg_indexed_resources_ as well
+    auto data = ParsePgFormattedResource(resource_id.Binary(),
+                                         /*for_wildcard_resource=*/false,
+                                         /*for_indexed_resource=*/true);
+    if (data) {
+      ResourceID original_resource_id(data->original_resource);
+
+      auto pg_resource_map_it = pg_indexed_resources_.find(original_resource_id);
+      if (pg_resource_map_it != pg_indexed_resources_.end()) {
+        auto resource_set_it = pg_resource_map_it->second.find(data->group_id);
+
+        if (resource_set_it != pg_resource_map_it->second.end()) {
+          resource_set_it->second.erase(resource_id);
+          if (resource_set_it->second.empty()) {
+            pg_resource_map_it->second.erase(data->group_id);
+          }
+          if (pg_resource_map_it->second.empty()) {
+            pg_indexed_resources_.erase(original_resource_id);
+          }
         }
       }
     }
@@ -98,22 +101,24 @@ NodeResourceInstanceSet &NodeResourceInstanceSet::Set(ResourceID resource_id,
   } else {
     resources_[resource_id] = std::move(instances);
 
-    // Popluate the pg_indexed_resources_map_
-    // TODO(myan): The parsing of the resource_id String can be costly and impact the
-    // task creation throughput if the parting is required every time we allocate
-    // resources for a task and updating the available resources. The current benchmark
-    // shows no observable impact for now. But in the future, ideas of improvement are:
-    // (1) to add the placement group id as well as the bundle index inside the
-    // ResourceID class. And instead of parse the String, leveraging the fields in the
-    // ResourceID class directly; (2) to update the pg resource id format to start with
-    // a special prefix so that we can do "startwith" instead of regex match which is
-    // less costly
-    auto data = ParsePgFormattedResource(resource_id.Binary(),
-                                         /*for_wildcard_resource=*/false,
-                                         /*for_indexed_resource=*/true);
-    if (data) {
-      pg_indexed_resources_[ResourceID(data->original_resource)][data->group_id].emplace(
-          resource_id);
+    if (track_pg_index_) {
+      // Populate the pg_indexed_resources_map_
+      // TODO(myan): The parsing of the resource_id String can be costly and impact the
+      // task creation throughput if the parting is required every time we allocate
+      // resources for a task and updating the available resources. The current benchmark
+      // shows no observable impact for now. But in the future, ideas of improvement are:
+      // (1) to add the placement group id as well as the bundle index inside the
+      // ResourceID class. And instead of parse the String, leveraging the fields in the
+      // ResourceID class directly; (2) to update the pg resource id format to start with
+      // a special prefix so that we can do "startwith" instead of regex match which is
+      // less costly
+      auto data = ParsePgFormattedResource(resource_id.Binary(),
+                                           /*for_wildcard_resource=*/false,
+                                           /*for_indexed_resource=*/true);
+      if (data) {
+        pg_indexed_resources_[ResourceID(data->original_resource)][data->group_id]
+            .emplace(resource_id);
+      }
     }
   }
   return *this;
@@ -136,6 +141,80 @@ bool NodeResourceInstanceSet::operator==(const NodeResourceInstanceSet &other) c
   return this->resources_ == other.resources_;
 }
 
+/*static*/ std::optional<std::vector<FixedPoint>>
+NodeResourceInstanceSet::ComputeAllocation(const std::vector<FixedPoint> &available,
+                                           FixedPoint demand) {
+  if (available.empty()) {
+    return std::nullopt;
+  }
+
+  if (available.size() == 1) {
+    if (available[0] >= demand) {
+      return std::vector<FixedPoint>{demand};
+    }
+    return std::nullopt;
+  }
+
+  // If resources has multiple instances, each instance has total capacity of 1.
+  //
+  // As long as remaining_demand is greater than 1.,
+  // allocate full unit-capacity instances until the remaining_demand becomes fractional.
+  // Then try to find the best fit for the fractional remaining_demand. Best fit means
+  // allocating the resource instance with the smallest available capacity greater than
+  // remaining_demand.
+  std::vector<FixedPoint> allocation(available.size(), FixedPoint(0));
+  std::vector<FixedPoint> remaining_available = available;
+  FixedPoint remaining_demand = demand;
+
+  if (remaining_demand >= 1.) {
+    for (size_t i = 0; i < remaining_available.size(); i++) {
+      if (remaining_available[i] == 1.) {
+        allocation[i] = 1.;
+        remaining_available[i] = 0;
+        remaining_demand -= 1.;
+      }
+      if (remaining_demand < 1.) {
+        break;
+      }
+    }
+  }
+
+  if (remaining_demand >= 1.) {
+    // Not enough full-capacity instances to cover the integer part.
+    return std::nullopt;
+  }
+
+  // Remaining demand is fractional. Find the best fit, if one exists.
+  if (remaining_demand > 0.) {
+    int64_t idx_best_fit = -1;
+    FixedPoint remaining_after_fit = 1.;
+    for (size_t i = 0; i < remaining_available.size(); i++) {
+      if (remaining_available[i] >= remaining_demand) {
+        if (idx_best_fit == -1 ||
+            (remaining_available[i] - remaining_demand < remaining_after_fit)) {
+          remaining_after_fit = remaining_available[i] - remaining_demand;
+          idx_best_fit = static_cast<int64_t>(i);
+        }
+      }
+    }
+    if (idx_best_fit == -1) {
+      return std::nullopt;
+    }
+    allocation[idx_best_fit] = remaining_demand;
+  }
+
+  return allocation;
+}
+
+bool NodeResourceInstanceSet::CanAllocate(const ResourceSet &resource_demands) const {
+  for (const auto &[resource_id, demand] : resource_demands.Resources()) {
+    if (!ComputeAllocation(Get(resource_id), demand).has_value()) {
+      return false;
+    }
+  }
+  return true;
+}
+
 std::optional<absl::flat_hash_map<ResourceID, std::vector<FixedPoint>>>
 NodeResourceInstanceSet::TryAllocate(const ResourceSet &resource_demands) {
   absl::flat_hash_map<ResourceID, std::vector<FixedPoint>> allocations;
@@ -295,75 +374,16 @@ NodeResourceInstanceSet::TryAllocate(const ResourceSet &resource_demands) {
 std::optional<std::vector<FixedPoint>> NodeResourceInstanceSet::TryAllocate(
     ResourceID resource_id, FixedPoint demand) {
   std::vector<FixedPoint> available = Get(resource_id);
-  if (available.empty()) {
-    return std::nullopt;
-  }
-
-  std::vector<FixedPoint> allocation(available.size());
-  FixedPoint remaining_demand = demand;
-
-  if (available.size() == 1) {
-    // This resource has just one instance.
-    if (available[0] >= remaining_demand) {
-      available[0] -= remaining_demand;
-      allocation[0] = remaining_demand;
-      Set(resource_id, std::move(available));
-      return std::make_optional<std::vector<FixedPoint>>(std::move(allocation));
-    } else {
-      // Not enough capacity.
-      return std::nullopt;
-    }
-  }
-
-  // If resources has multiple instances, each instance has total capacity of 1.
-  //
-  // As long as remaining_demand is greater than 1.,
-  // allocate full unit-capacity instances until the remaining_demand becomes fractional.
-  // Then try to find the best fit for the fractional remaining_resources. Best fit means
-  // allocating the resource instance with the smallest available capacity greater than
-  // remaining_demand
-  if (remaining_demand >= 1.) {
-    for (size_t i = 0; i < available.size(); i++) {
-      if (available[i] == 1.) {
-        // Allocate a full unit-capacity instance.
-        allocation[i] = 1.;
-        available[i] = 0;
-        remaining_demand -= 1.;
-      }
-      if (remaining_demand < 1.) {
-        break;
-      }
-    }
-  }
-
-  if (remaining_demand >= 1.) {
-    // Cannot satisfy a demand greater than one if no unit capacity resource is available.
+  auto allocation = ComputeAllocation(available, demand);
+  if (!allocation) {
     return std::nullopt;
   }
 
-  // Remaining demand is fractional. Find the best fit, if exists.
-  if (remaining_demand > 0.) {
-    int64_t idx_best_fit = -1;
-    FixedPoint available_best_fit = 1.;
-    for (size_t i = 0; i < available.size(); i++) {
-      if (available[i] >= remaining_demand) {
-        if (idx_best_fit == -1 ||
-            (available[i] - remaining_demand < available_best_fit)) {
-          available_best_fit = available[i] - remaining_demand;
-          idx_best_fit = static_cast<int64_t>(i);
-        }
-      }
-    }
-    if (idx_best_fit == -1) {
-      return std::nullopt;
-    } else {
-      allocation[idx_best_fit] = remaining_demand;
-      available[idx_best_fit] -= remaining_demand;
-    }
+  for (size_t i = 0; i < available.size(); i++) {
+    available[i] -= (*allocation)[i];
   }
-
   Set(resource_id, std::move(available));
-  return std::make_optional<std::vector<FixedPoint>>(std::move(allocation));
+  return allocation;
 }
 
 void NodeResourceInstanceSet::AllocateWithReference(
diff --git a/src/ray/common/scheduling/resource_instance_set.h b/src/ray/common/scheduling/resource_instance_set.h
index f49b2d01fccf..6e1f2b48cc62 100644
--- a/src/ray/common/scheduling/resource_instance_set.h
+++ b/src/ray/common/scheduling/resource_instance_set.h
@@ -25,10 +25,17 @@
 
 namespace ray {
 
+using ResourceAllocation = absl::flat_hash_map<ResourceID, std::vector<FixedPoint>>;
+
 /// Represents a node resource set that contains the per-instance resource values.
 class NodeResourceInstanceSet {
  public:
-  NodeResourceInstanceSet(){};
+  /// \param track_pg_index If true, parse resource names on every Set/Remove
+  /// to build a lookup table for PG bundle resources. The raylet uses this to
+  /// find which bundle can satisfy a PG resource request. The GCS passes false
+  /// because it never does local PG allocation, and the parsing overhead is large.
+  explicit NodeResourceInstanceSet(bool track_pg_index = true)
+      : track_pg_index_(track_pg_index){};
 
   /// Construct a NodeResourceInstanceSet from a node total resources.
   explicit NodeResourceInstanceSet(const NodeResourceSet &total);
@@ -56,6 +63,23 @@ class NodeResourceInstanceSet {
 
   std::string DebugString() const;
 
+  /// Compute the per-instance allocation for a single resource without modifying state.
+  ///
+  /// Allocates full unit-capacity instances first (for the integer part of demand),
+  /// then uses best-fit for the fractional remainder: picks the instance with the
+  /// smallest available capacity that can still satisfy the remaining demand.
+  ///
+  /// Example: available = (1., 1., .7, 0.5), demand = 1.2
+  ///   -> allocate one full instance, then 0.2 from the 0.5 instance (best fit).
+  ///   -> returns (1., 0., 0., 0.2)
+  ///
+  /// Returns the allocation vector, or std::nullopt if the demand cannot be satisfied.
+  static std::optional<std::vector<FixedPoint>> ComputeAllocation(
+      const std::vector<FixedPoint> &available, FixedPoint demand);
+
+  /// Returns true if `resource_demands` can be satisfied without modifying state.
+  bool CanAllocate(const ResourceSet &resource_demands) const;
+
   /// Try to allocate resources specified by `resource_demands`.
   /// This operation is all or nothing meaning that if any single resource
   /// cannot be allocated, the entire allocation fails and std::nullopt is returned.
@@ -86,24 +110,12 @@ class NodeResourceInstanceSet {
   /// Convert to node resource set with summed per-instance values.
   NodeResourceSet ToNodeResourceSet() const;
 
-  /// Only for testing.
   const absl::flat_hash_map<ResourceID, std::vector<FixedPoint>> &Resources() const {
     return resources_;
   }
 
- private:
   /// Allocate enough capacity across the instances of a resource to satisfy "demand".
-  ///
-  /// Allocate full unit-capacity instances until
-  /// demand becomes fractional, and then satisfy the fractional demand using the
-  /// instance with the smallest available capacity that can satisfy the fractional
-  /// demand. For example, assume a resource conisting of 4 instances, with available
-  /// capacities: (1., 1., .7, 0.5) and deman of 1.2. Then we allocate one full
-  /// instance and then allocate 0.2 of the 0.5 instance (as this is the instance
-  /// with the smalest available capacity that can satisfy the remaining demand of 0.2).
-  /// As a result remaining available capacities will be (0., 1., .7, .3).
-  /// Thus, we will allocate a bunch of full instances and
-  /// at most a fractional instance.
+  /// Uses ComputeAllocation for the best-fit algorithm.
   ///
   /// During resource allocation with a placement group, no matter whether the
   /// allocation requirement specifies a bundle index, we generate the
@@ -145,11 +157,11 @@ class NodeResourceInstanceSet {
   ///
   /// \param resource_id: The id of the resource to be allocated.
   /// \param demand: The resource amount to be allocated.
-  ///
   /// \return the allocated instances, if allocation successful. Else, return nullopt.
   std::optional<std::vector<FixedPoint>> TryAllocate(ResourceID resource_id,
                                                      FixedPoint demand);
 
+ private:
   /// Allocate resource to the resource_id based on a provided reference allocation.
   /// The function is used for placement group allocation. Making the allocation of
   /// the wildcard resource be identical to the indexed resource allocation.
@@ -163,6 +175,8 @@ class NodeResourceInstanceSet {
   void AllocateWithReference(const std::vector<FixedPoint> &ref_allocation,
                              ResourceID resource_id);
 
+  bool track_pg_index_ = true;
+
   /// Map from the resource IDs to the resource instance values.
   absl::flat_hash_map<ResourceID, std::vector<FixedPoint>> resources_;
 
diff --git a/src/ray/gcs/gcs_placement_group_scheduler.cc b/src/ray/gcs/gcs_placement_group_scheduler.cc
index b3d733b470c5..f56c8e7b6d10 100644
--- a/src/ray/gcs/gcs_placement_group_scheduler.cc
+++ b/src/ray/gcs/gcs_placement_group_scheduler.cc
@@ -21,6 +21,7 @@
 #include <vector>
 
 #include "ray/common/asio/asio_util.h"
+#include "ray/common/bundle_spec.h"
 
 namespace ray {
 namespace gcs {
@@ -110,9 +111,8 @@ void GcsPlacementGroupScheduler::ScheduleUnplacedBundles(
                 .emplace(placement_group->GetPlacementGroupID(), lease_status_tracker)
                 .second);
 
-  // Acquire resources from gcs resources manager to reserve bundle resources.
   const auto &bundle_locations = lease_status_tracker->GetBundleLocations();
-  AcquireBundleResources(bundle_locations);
+  AcquireBundleResources(bundle_locations, lease_status_tracker);
 
   // Convert to a set of bundle specifications grouped by the node.
   std::unordered_map<NodeID, std::vector<std::shared_ptr<const BundleSpecification>>>
@@ -304,7 +304,8 @@ void GcsPlacementGroupScheduler::CommitAllBundles(
   if (lease_status_tracker->GetLeasingState() == LeasingState::CANCELLED) {
     DestroyPlacementGroupCommittedBundleResources(
         lease_status_tracker->GetPlacementGroup()->GetPlacementGroupID());
-    ReturnBundleResources(lease_status_tracker->GetBundleLocations());
+    ReturnBundleResources(lease_status_tracker->GetBundleLocations(),
+                          lease_status_tracker);
     schedule_failure_handler(lease_status_tracker->GetPlacementGroup(),
                              /*is_feasible=*/true);
     return;
@@ -344,7 +345,7 @@ void GcsPlacementGroupScheduler::CommitAllBundles(
         // Commit the bundle resources on the remote node to the cluster resources.
         // If status is not OK, no need to call ReturnBundleResources because the
         // OnAllBundleCommitRequestReturned function calls it.
-        CommitBundleResources(commited_bundle_locations);
+        CommitBundleResources(commited_bundle_locations, lease_status_tracker);
       }
 
       if (lease_status_tracker->AllCommitRequestReturned()) {
@@ -384,7 +385,8 @@ void GcsPlacementGroupScheduler::OnAllBundlePrepareRequestReturned(
     auto it = placement_group_leasing_in_progress_.find(placement_group_id);
     RAY_CHECK(it != placement_group_leasing_in_progress_.end());
     placement_group_leasing_in_progress_.erase(it);
-    ReturnBundleResources(lease_status_tracker->GetBundleLocations());
+    ReturnBundleResources(lease_status_tracker->GetBundleLocations(),
+                          lease_status_tracker);
     schedule_failure_handler(placement_group, /*is_feasible*/ true);
     return;
   }
@@ -438,7 +440,8 @@ void GcsPlacementGroupScheduler::OnAllBundleCommitRequestReturned(
   // to destroy them separately.
   if (lease_status_tracker->GetLeasingState() == LeasingState::CANCELLED) {
     DestroyPlacementGroupCommittedBundleResources(placement_group_id);
-    ReturnBundleResources(lease_status_tracker->GetBundleLocations());
+    ReturnBundleResources(lease_status_tracker->GetBundleLocations(),
+                          lease_status_tracker);
     schedule_failure_handler(placement_group, /*is_feasible*/ true);
     return;
   }
@@ -452,7 +455,7 @@ void GcsPlacementGroupScheduler::OnAllBundleCommitRequestReturned(
       placement_group->GetMutableBundle(bundle.first.second)->clear_node_id();
     }
     placement_group->UpdateState(rpc::PlacementGroupTableData::RESCHEDULING);
-    ReturnBundleResources(uncommitted_bundle_locations);
+    ReturnBundleResources(uncommitted_bundle_locations, lease_status_tracker);
     schedule_failure_handler(placement_group, /*is_feasible*/ true);
   } else {
     schedule_success_handler(placement_group);
@@ -638,15 +641,29 @@ void GcsPlacementGroupScheduler::DestroyPlacementGroupCommittedBundleResources(
   }
 }
 
+void LeaseStatusTracker::SetBundleAllocation(const BundleID &bundle_id,
+                                             ResourceAllocation allocation) {
+  acquired_resource_allocations_[bundle_id] = std::move(allocation);
+}
+
+const ResourceAllocation *LeaseStatusTracker::GetBundleAllocation(
+    const BundleID &bundle_id) const {
+  auto it = acquired_resource_allocations_.find(bundle_id);
+  return it != acquired_resource_allocations_.end() ? &it->second : nullptr;
+}
+
 void GcsPlacementGroupScheduler::AcquireBundleResources(
-    const std::shared_ptr<BundleLocations> &bundle_locations) {
-  // Acquire bundle resources from gcs resources manager.
+    const std::shared_ptr<BundleLocations> &bundle_locations,
+    const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker) {
   auto &cluster_resource_manager =
       cluster_resource_scheduler_.GetClusterResourceManager();
   for (auto &bundle : *bundle_locations) {
-    cluster_resource_manager.SubtractNodeAvailableResources(
+    auto allocation = cluster_resource_manager.SubtractNodeAvailableResources(
         scheduling::NodeID(bundle.second.first.Binary()),
         bundle.second.second->GetRequiredResources());
+    if (allocation.has_value() && lease_status_tracker) {
+      lease_status_tracker->SetBundleAllocation(bundle.first, std::move(*allocation));
+    }
   }
 }
 
@@ -682,29 +699,35 @@ bool GcsPlacementGroupScheduler::IsPlacementGroupWildcardResource(
 }
 
 void GcsPlacementGroupScheduler::CommitBundleResources(
-    const std::shared_ptr<BundleLocations> &bundle_locations) {
-  // Acquire bundle resources from gcs resources manager.
+    const std::shared_ptr<BundleLocations> &bundle_locations,
+    const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker) {
   auto &cluster_resource_manager =
       cluster_resource_scheduler_.GetClusterResourceManager();
-  auto node_bundle_resources_map = ToNodeBundleResourcesMap(bundle_locations);
-  for (const auto &[node_id, node_bundle_resources] : node_bundle_resources_map) {
-    for (const auto &resource_id : node_bundle_resources.ResourceIds()) {
-      // A placement group's wildcard resource has to be the sum of all related bundles.
-      // Even though `ToNodeBundleResourcesMap` has already considered this,
-      // it misses the scenario in which single (or subset of) bundle is rescheduled.
-      // When commiting this single bundle, its wildcard resource would wrongly overwrite
-      // the existing value, unless using the following additive operation.
-      auto capacity = node_bundle_resources.Get(resource_id);
-      if (IsPlacementGroupWildcardResource(resource_id.Binary())) {
-        auto new_capacity =
-            capacity +
-            cluster_resource_manager.GetNodeResources(node_id).total.Get(resource_id);
-        cluster_resource_manager.UpdateResourceCapacity(
-            node_id, resource_id, new_capacity.Double());
-      } else {
-        cluster_resource_manager.UpdateResourceCapacity(
-            node_id, resource_id, capacity.Double());
+
+  for (const auto &[bundle_id, location] : *bundle_locations) {
+    const auto &node_id = scheduling::NodeID(location.first.Binary());
+    const auto &bundle_spec = *location.second;
+
+    const auto *bundle_alloc = lease_status_tracker->GetBundleAllocation(bundle_id);
+
+    const auto &resources = bundle_spec.GetFormattedResources();
+    for (const auto &[resource_name, capacity] : resources) {
+      auto resource_id = scheduling::ResourceID(resource_name);
+      auto original_name = GetOriginalResourceName(resource_name);
+
+      if (bundle_alloc != nullptr) {
+        auto alloc_it = bundle_alloc->find(scheduling::ResourceID(original_name));
+        if (alloc_it != bundle_alloc->end()) {
+          cluster_resource_manager.AddResourceInstances(
+              node_id, resource_id, alloc_it->second);
+          continue;
+        }
       }
+      // Two cases reach here:
+      // 1. Synthetic resources like bundle_group have no physical allocation.
+      // 2. GCS restart loses the in-memory tracker; syncer corrects within ~100ms.
+      cluster_resource_manager.AddResourceInstances(
+          node_id, resource_id, {FixedPoint(capacity)});
     }
   }
 
@@ -714,16 +737,31 @@ void GcsPlacementGroupScheduler::CommitBundleResources(
 }
 
 void GcsPlacementGroupScheduler::ReturnBundleResources(
-    const std::shared_ptr<BundleLocations> &bundle_locations) {
-  // Return bundle resources to gcs resources manager should contains the following steps.
-  // 1. Remove related bundle resources from nodes.
-  // 2. Add resources allocated for bundles back to nodes.
+    const std::shared_ptr<BundleLocations> &bundle_locations,
+    const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker) {
+  // 1. Remove PG-formatted resources (GPU_group_xxx, etc.) from nodes.
+  // 2. Add original resources (GPU, CPU) back to nodes if has a tracker.
+  //    Without a tracker (e.g. PG remove, where it was destroyed after commit)
+  //    we don't know which instances were allocated, and recomputing is not a
+  //    good idea since the topology may have changed. Prefer relying on syncer
+  //    which delivers the real state within ~100ms.
   for (auto &bundle : *bundle_locations) {
     if (!TryReleasingBundleResources(bundle.second)) {
       waiting_removed_bundles_.push_back(bundle.second);
     }
   }
 
+  if (lease_status_tracker) {
+    auto &crm = cluster_resource_scheduler_.GetClusterResourceManager();
+    for (const auto &[bundle_id, location] : *bundle_locations) {
+      const auto *alloc = lease_status_tracker->GetBundleAllocation(bundle_id);
+      if (alloc) {
+        crm.AddNodeAvailableResources(scheduling::NodeID(location.first.Binary()),
+                                      *alloc);
+      }
+    }
+  }
+
   for (const auto &listener : resources_changed_listeners_) {
     listener();
   }
@@ -769,7 +807,7 @@ bool GcsPlacementGroupScheduler::TryReleasingBundleResources(
   }
 
   for (const auto &[resource_name, capacity] : wildcard_resources) {
-    if (capacity == 0) {
+    if (capacity <= 0) {
       bundle_resource_ids.emplace_back(scheduling::ResourceID(resource_name));
     } else {
       cluster_resource_manager.UpdateResourceCapacity(
@@ -780,9 +818,6 @@ bool GcsPlacementGroupScheduler::TryReleasingBundleResources(
   // It will affect nothing if the resource_id to be deleted does not exist in the
   // cluster_resource_manager_.
   cluster_resource_manager.DeleteResources(node_id, bundle_resource_ids);
-  // Add reserved bundle resources back to the node.
-  cluster_resource_manager.AddNodeAvailableResources(
-      node_id, bundle_spec->GetRequiredResources().GetResourceSet());
   return true;
 }
 
diff --git a/src/ray/gcs/gcs_placement_group_scheduler.h b/src/ray/gcs/gcs_placement_group_scheduler.h
index 9f509cc01561..eaed849bd37a 100644
--- a/src/ray/gcs/gcs_placement_group_scheduler.h
+++ b/src/ray/gcs/gcs_placement_group_scheduler.h
@@ -228,6 +228,11 @@ class LeaseStatusTracker {
   /// status tracker anymore.
   void MarkCommitPhaseStarted();
 
+  void SetBundleAllocation(const BundleID &bundle_id, ResourceAllocation allocation);
+
+  /// Returns nullptr if not found.
+  const ResourceAllocation *GetBundleAllocation(const BundleID &bundle_id) const;
+
  private:
   /// Method to update leasing states.
   ///
@@ -272,6 +277,11 @@ class LeaseStatusTracker {
   /// Bundles to schedule.
   std::vector<std::shared_ptr<const BundleSpecification>> bundles_to_schedule_;
 
+  /// Per-bundle per-instance resource allocations (original resources, not
+  /// PG-formatted), e.g. {GPU: [0, 1, 0, 0]} meaning GPU instance 1 was used.
+  absl::flat_hash_map<BundleID, ResourceAllocation, pair_hash>
+      acquired_resource_allocations_;
+
   /// Location of bundles.
   std::shared_ptr<BundleLocations> bundle_locations_;
 };
@@ -445,14 +455,21 @@ class GcsPlacementGroupScheduler : public GcsPlacementGroupSchedulerInterface {
       const PlacementGroupID &placement_group_id);
 
   /// Acquire the bundle resources from the cluster resources.
-  void AcquireBundleResources(const std::shared_ptr<BundleLocations> &bundle_locations);
+  void AcquireBundleResources(
+      const std::shared_ptr<BundleLocations> &bundle_locations,
+      const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker);
 
   /// Commit the bundle resources to the cluster resources.
-  void CommitBundleResources(const std::shared_ptr<BundleLocations> &bundle_locations);
+  void CommitBundleResources(
+      const std::shared_ptr<BundleLocations> &bundle_locations,
+      const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker);
 
   /// Return the bundle resources to the cluster resources.
-  /// It will remove bundle resources AND also add original resources back.
-  void ReturnBundleResources(const std::shared_ptr<BundleLocations> &bundle_locations);
+  /// Removes PG-formatted resources and restores original resources if a
+  /// LeaseStatusTracker with saved per-instance allocation is provided.
+  void ReturnBundleResources(
+      const std::shared_ptr<BundleLocations> &bundle_locations,
+      const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker = nullptr);
 
   /// Create scheduling context.
   std::unique_ptr<BundleSchedulingContext> CreateSchedulingContext(
diff --git a/src/ray/gcs/gcs_resource_manager.cc b/src/ray/gcs/gcs_resource_manager.cc
index 1b7a9a758f12..767f07561d3e 100644
--- a/src/ray/gcs/gcs_resource_manager.cc
+++ b/src/ray/gcs/gcs_resource_manager.cc
@@ -90,9 +90,10 @@ void GcsResourceManager::HandleGetAllAvailableResources(
     rpc::AvailableResources resource;
     resource.set_node_id(node_resources_entry.first.Binary());
     const auto &node_resources = node_resources_entry.second.GetLocalView();
-    for (const auto &resource_id : node_resources.available.ExplicitResourceIds()) {
+    auto available_set = node_resources.available.ToNodeResourceSet();
+    for (const auto &resource_id : available_set.ExplicitResourceIds()) {
       const auto &resource_name = resource_id.Binary();
-      const auto &resource_value = node_resources.available.Get(resource_id);
+      const auto &resource_value = available_set.Get(resource_id);
       resource.mutable_resources_available()->insert(
           {resource_name, resource_value.Double()});
     }
@@ -223,8 +224,15 @@ void GcsResourceManager::UpdateNodeResourceUsage(
         resource_view_sync_message.resources_total();
   }
 
-  (*iter->second.mutable_resources_available()) =
-      resource_view_sync_message.resources_available();
+  iter->second.mutable_resources_available()->clear();
+  for (const auto &[name, instances] :
+       resource_view_sync_message.resources_available_instances()) {
+    double sum = 0;
+    for (double v : instances.values()) {
+      sum += v;
+    }
+    (*iter->second.mutable_resources_available())[name] = sum;
+  }
 }
 
 void GcsResourceManager::Initialize(const GcsInitData &gcs_init_data) {
diff --git a/src/ray/gcs/tests/gcs_placement_group_scheduler_test.cc b/src/ray/gcs/tests/gcs_placement_group_scheduler_test.cc
index bad08cb3e3e6..5988ff9dcfe3 100644
--- a/src/ray/gcs/tests/gcs_placement_group_scheduler_test.cc
+++ b/src/ray/gcs/tests/gcs_placement_group_scheduler_test.cc
@@ -143,6 +143,15 @@ class GcsPlacementGroupSchedulerTest : public ::testing::Test {
     gcs_resource_manager_->OnNodeAdd(*node);
   }
 
+  void AddNodeWithGpu(const std::shared_ptr<rpc::GcsNodeInfo> &node,
+                      int cpu_num = 4,
+                      int gpu_num = 2) {
+    (*node->mutable_resources_total())["CPU"] = cpu_num;
+    (*node->mutable_resources_total())["GPU"] = gpu_num;
+    gcs_node_manager_->AddNode(node);
+    gcs_resource_manager_->OnNodeAdd(*node);
+  }
+
   void RemoveNode(const std::shared_ptr<rpc::GcsNodeInfo> &node) {
     rpc::NodeDeathInfo death_info;
     gcs_node_manager_->RemoveNode(
@@ -249,7 +258,8 @@ class GcsPlacementGroupSchedulerTest : public ::testing::Test {
     auto resource_view_before_scheduling = cluster_resource_manager.GetResourceView();
     // Make sure the resources are not used.
     for (const auto &[node_id, node] : resource_view_before_scheduling) {
-      if (node.GetLocalView().total != node.GetLocalView().available) {
+      if (!(node.GetLocalView().available ==
+            NodeResourceInstanceSet(node.GetLocalView().total))) {
         return false;
       }
     }
@@ -1426,5 +1436,58 @@ TEST_F(GcsPlacementGroupSchedulerTest, TestBundlesRemovedWhenNodeDead) {
   ASSERT_EQ(scheduler_->waiting_removed_bundles_.size(), 0);
 }
 
+TEST_F(GcsPlacementGroupSchedulerTest, TestGpuPgPrepareFailureRollback) {
+  // Add a node with 4 CPUs and 2 GPUs.
+  auto node = GenNodeInfo(0);
+  AddNodeWithGpu(node, /*cpu_num=*/4, /*gpu_num=*/2);
+
+  scheduling::NodeID scheduling_node_id(node->node_id());
+  const auto &view =
+      cluster_resource_scheduler_->GetClusterResourceManager().GetResourceView();
+
+  // Verify GPU per-instance: 2 instances, each 1.0.
+  auto gpu_before = view.at(scheduling_node_id)
+                        .GetLocalView()
+                        .available.Get(scheduling::ResourceID("GPU"));
+  ASSERT_EQ(gpu_before.size(), 2);
+  ASSERT_EQ(gpu_before[0], FixedPoint(1.0));
+  ASSERT_EQ(gpu_before[1], FixedPoint(1.0));
+
+  // Create a PG with 1 bundle requiring 1 GPU (using GenCreatePlacementGroupRequest
+  // with cpu_num=0, then manually add GPU to the bundle spec is hard, so we create
+  // the request with 1 CPU per bundle and rely on the node having both).
+  auto request = GenCreatePlacementGroupRequest(
+      /*name=*/"",
+      /*strategy=*/rpc::PlacementStrategy::SPREAD,
+      /*bundles_count=*/1,
+      /*cpu_num=*/1.0);
+  // Add GPU to the bundle.
+  auto *bundle = request.mutable_placement_group_spec()->mutable_bundles(0);
+  (*bundle->mutable_unit_resources())["GPU"] = 1.0;
+
+  auto pg = std::make_shared<GcsPlacementGroup>(request, "", counter_);
+  ScheduleUnplacedBundles(pg);
+
+  // After speculative deduction: 1 GPU instance should be deducted.
+  auto gpu_after = view.at(scheduling_node_id)
+                       .GetLocalView()
+                       .available.Get(scheduling::ResourceID("GPU"));
+  ASSERT_EQ(gpu_after.size(), 2);
+  ASSERT_EQ(gpu_after[0] + gpu_after[1], FixedPoint(1.0));
+
+  // Prepare fails → rollback should restore per-instance GPU state.
+  ASSERT_TRUE(
+      raylet_clients_[0]->GrantPrepareBundleResources(/*success=*/false, Status::OK()));
+  WaitPlacementGroupPendingDone(1, GcsPlacementGroupStatus::FAILURE);
+
+  ASSERT_TRUE(EnsureClusterResourcesAreNotInUse());
+  auto gpu_restored = view.at(scheduling_node_id)
+                          .GetLocalView()
+                          .available.Get(scheduling::ResourceID("GPU"));
+  ASSERT_EQ(gpu_restored.size(), 2);
+  ASSERT_EQ(gpu_restored[0], FixedPoint(1.0));
+  ASSERT_EQ(gpu_restored[1], FixedPoint(1.0));
+}
+
 }  // namespace gcs
 }  // namespace ray
diff --git a/src/ray/gcs/tests/gcs_resource_manager_test.cc b/src/ray/gcs/tests/gcs_resource_manager_test.cc
index dd85d72335d1..a58016954e54 100644
--- a/src/ray/gcs/tests/gcs_resource_manager_test.cc
+++ b/src/ray/gcs/tests/gcs_resource_manager_test.cc
@@ -46,8 +46,18 @@ class GcsResourceManagerTest : public ::testing::Test {
       int64_t draining_deadline_timestamp_ms = -1) {
     syncer::ResourceViewSyncMessage resource_view_sync_message;
     for (const auto &resource : available_resources) {
-      (*resource_view_sync_message.mutable_resources_available())[resource.first] =
-          resource.second;
+      rpc::syncer::ResourceInstances instances;
+      auto resource_id = scheduling::ResourceID(resource.first);
+      if (resource_id.IsUnitInstanceResource()) {
+        size_t num = static_cast<size_t>(resource.second);
+        for (size_t i = 0; i < num; i++) {
+          instances.add_values(1.0);
+        }
+      } else {
+        instances.add_values(resource.second);
+      }
+      (*resource_view_sync_message
+            .mutable_resources_available_instances())[resource.first] = instances;
     }
     for (const auto &resource : total_resources) {
       (*resource_view_sync_message.mutable_resources_total())[resource.first] =
@@ -90,16 +100,66 @@ TEST_F(GcsResourceManagerTest, TestBasic) {
       scheduling_node_id,
       resource_request,
       /*ignore_object_store_memory_requirement=*/true));
-  ASSERT_TRUE(cluster_resource_manager_.SubtractNodeAvailableResources(scheduling_node_id,
-                                                                       resource_request));
+  auto allocation = cluster_resource_manager_.SubtractNodeAvailableResources(
+      scheduling_node_id, resource_request);
+  ASSERT_TRUE(allocation.has_value());
   ASSERT_FALSE(cluster_resource_manager_.HasAvailableResources(
       scheduling_node_id,
       resource_request,
       /*ignore_object_store_memory_requirement=*/true));
 
   // Test `ReleaseResources`.
-  ASSERT_TRUE(cluster_resource_manager_.AddNodeAvailableResources(
-      scheduling_node_id, resource_request.GetResourceSet()));
+  ASSERT_TRUE(cluster_resource_manager_.AddNodeAvailableResources(scheduling_node_id,
+                                                                  allocation.value()));
+}
+
+TEST_F(GcsResourceManagerTest, TestPerInstanceGpuResources) {
+  auto node = GenNodeInfo();
+  node->mutable_resources_total()->insert({{"GPU", 4}});
+  gcs_resource_manager_->OnNodeAdd(*node);
+
+  auto node_id = NodeID::FromBinary(node->node_id());
+  scheduling::NodeID scheduling_node_id(node->node_id());
+
+  // Syncer reports 4 GPUs, each with 1.0 capacity.
+  UpdateFromResourceViewSync(node_id, {{"GPU", 4}}, {{"GPU", 4}});
+
+  // Verify per-instance: 4 instances, each 1.0.
+  const auto &view = cluster_resource_manager_.GetResourceView();
+  const auto &available = view.at(scheduling_node_id).GetLocalView().available;
+  auto gpu_instances = available.Get(scheduling::ResourceID("GPU"));
+  ASSERT_EQ(gpu_instances.size(), 4);
+  for (const auto &inst : gpu_instances) {
+    ASSERT_EQ(inst, FixedPoint(1.0));
+  }
+
+  // Allocate 0.5 GPU (should take from one instance).
+  absl::flat_hash_map<std::string, double> demand_map = {{"GPU", 0.5}};
+  auto request =
+      ResourceMapToResourceRequest(demand_map, /*requires_object_store_memory=*/false);
+  auto alloc = cluster_resource_manager_.SubtractNodeAvailableResources(
+      scheduling_node_id, request);
+  ASSERT_TRUE(alloc.has_value());
+
+  // One instance should have 0.5 remaining, others still 1.0.
+  const auto &after_alloc = view.at(scheduling_node_id).GetLocalView().available;
+  auto gpu_after = after_alloc.Get(scheduling::ResourceID("GPU"));
+  ASSERT_EQ(gpu_after.size(), 4);
+  FixedPoint sum(0);
+  for (const auto &inst : gpu_after) {
+    sum += inst;
+  }
+  ASSERT_EQ(sum, FixedPoint(3.5));
+
+  // Free it back.
+  ASSERT_TRUE(cluster_resource_manager_.AddNodeAvailableResources(scheduling_node_id,
+                                                                  alloc.value()));
+  auto gpu_freed = view.at(scheduling_node_id)
+                       .GetLocalView()
+                       .available.Get(scheduling::ResourceID("GPU"));
+  for (const auto &inst : gpu_freed) {
+    ASSERT_EQ(inst, FixedPoint(1.0));
+  }
 }
 
 TEST_F(GcsResourceManagerTest, TestResourceUsageAPI) {
@@ -117,7 +177,6 @@ TEST_F(GcsResourceManagerTest, TestResourceUsageAPI) {
   gcs_resource_manager_->OnNodeAdd(*node);
 
   syncer::ResourceViewSyncMessage resource_view_sync_message;
-  (*resource_view_sync_message.mutable_resources_available())["CPU"] = 2;
   (*resource_view_sync_message.mutable_resources_total())["CPU"] = 2;
   gcs_resource_manager_->UpdateNodeResourceUsage(node_id, resource_view_sync_message);
 
@@ -146,9 +205,10 @@ TEST_F(GcsResourceManagerTest, TestResourceUsageFromDifferentSyncMsgs) {
 
   syncer::ResourceViewSyncMessage resource_view_sync_message;
   resource_view_sync_message.mutable_resources_total()->insert({"CPU", 5});
-  resource_view_sync_message.mutable_resources_available()->insert({"CPU", 5});
+  rpc::syncer::ResourceInstances cpu_inst;
+  cpu_inst.add_values(5);
+  (*resource_view_sync_message.mutable_resources_available_instances())["CPU"] = cpu_inst;
 
-  // Update resource usage from resource view.
   gcs_resource_manager_->UpdateFromResourceView(NodeID::FromBinary(node->node_id()),
                                                 resource_view_sync_message);
   ASSERT_EQ(
@@ -171,7 +231,10 @@ TEST_F(GcsResourceManagerTest, TestSetAvailableResourcesWhenNodeDead) {
 
   syncer::ResourceViewSyncMessage resource_view_sync_message;
   resource_view_sync_message.mutable_resources_total()->insert({"CPU", 5});
-  resource_view_sync_message.mutable_resources_available()->insert({"CPU", 5});
+  rpc::syncer::ResourceInstances cpu_inst2;
+  cpu_inst2.add_values(5);
+  (*resource_view_sync_message.mutable_resources_available_instances())["CPU"] =
+      cpu_inst2;
   gcs_resource_manager_->UpdateFromResourceView(node_id, resource_view_sync_message);
   ASSERT_EQ(cluster_resource_manager_.GetResourceView().size(), 0);
 }
diff --git a/src/ray/gcs_rpc_client/tests/gcs_client_test.cc b/src/ray/gcs_rpc_client/tests/gcs_client_test.cc
index ac8366ccaf04..bdbdfb3345e7 100644
--- a/src/ray/gcs_rpc_client/tests/gcs_client_test.cc
+++ b/src/ray/gcs_rpc_client/tests/gcs_client_test.cc
@@ -686,8 +686,14 @@ TEST_P(GcsClientTest, TestGetAllAvailableResources) {
   NodeID node_id = NodeID::FromBinary(node_info->node_id());
   syncer::ResourceViewSyncMessage resource;
   // Set this flag to indicate resources has changed.
-  (*resource.mutable_resources_available())["CPU"] = 1.0;
-  (*resource.mutable_resources_available())["GPU"] = 10.0;
+  rpc::syncer::ResourceInstances cpu_instances;
+  cpu_instances.add_values(1.0);
+  rpc::syncer::ResourceInstances gpu_instances;
+  for (int i = 0; i < 10; i++) {
+    gpu_instances.add_values(1.0);
+  }
+  (*resource.mutable_resources_available_instances())["CPU"] = cpu_instances;
+  (*resource.mutable_resources_available_instances())["GPU"] = gpu_instances;
   (*resource.mutable_resources_total())["CPU"] = 1.0;
   (*resource.mutable_resources_total())["GPU"] = 10.0;
   gcs_server_->UpdateGcsResourceManagerInTest(node_id, resource);
diff --git a/src/ray/gcs_rpc_client/tests/global_state_accessor_test.cc b/src/ray/gcs_rpc_client/tests/global_state_accessor_test.cc
index 9c651561f5bb..3216d74bf9eb 100644
--- a/src/ray/gcs_rpc_client/tests/global_state_accessor_test.cc
+++ b/src/ray/gcs_rpc_client/tests/global_state_accessor_test.cc
@@ -294,8 +294,17 @@ TEST_P(GlobalStateAccessorTest, TestGetAllResourceUsage) {
   syncer::ResourceViewSyncMessage resources2;
   (*resources2.mutable_resources_total())["CPU"] = 1;
   (*resources2.mutable_resources_total())["GPU"] = 10;
-  (*resources2.mutable_resources_available())["CPU"] = 1;
-  (*resources2.mutable_resources_available())["GPU"] = 5;
+  rpc::syncer::ResourceInstances cpu_instances2;
+  cpu_instances2.add_values(1.0);
+  (*resources2.mutable_resources_available_instances())["CPU"] = cpu_instances2;
+  rpc::syncer::ResourceInstances gpu_instances2;
+  for (int i = 0; i < 5; i++) {
+    gpu_instances2.add_values(1.0);
+  }
+  for (int i = 0; i < 5; i++) {
+    gpu_instances2.add_values(0.0);
+  }
+  (*resources2.mutable_resources_available_instances())["GPU"] = gpu_instances2;
   gcs_server_->UpdateGcsResourceManagerInTest(
       NodeID::FromBinary(node_table_data->node_id()), resources2);
 
diff --git a/src/ray/protobuf/ray_syncer.proto b/src/ray/protobuf/ray_syncer.proto
index 6ec15782b83a..a60e63296b33 100644
--- a/src/ray/protobuf/ray_syncer.proto
+++ b/src/ray/protobuf/ray_syncer.proto
@@ -25,9 +25,13 @@ message CommandsSyncMessage {
   bool should_global_gc = 1;
 }
 
+// Per-instance available values for a single resource (e.g., [0.4, 0.6] for
+// a 2-GPU node where GPU 0 has 0.4 free and GPU 1 has 0.6 free).
+message ResourceInstances {
+  repeated double values = 1;
+}
+
 message ResourceViewSyncMessage {
-  // Resource capacity currently available on this node manager.
-  map<string, double> resources_available = 1;
   // Total resource capacity configured for this node manager.
   map<string, double> resources_total = 2;
   // Whether this node has object pulls queued. This can happen if
@@ -45,6 +49,11 @@ message ResourceViewSyncMessage {
   repeated string node_activity = 7;
   // The key-value labels of this node.
   map<string, string> labels = 8;
+  // Per-instance available resources. For unit instance resources like GPU,
+  // each element is one instance (e.g., {"GPU": [0.4, 0.6]} means
+  // GPU 0 has 0.4 free, GPU 1 has 0.6 free). For scalar resources like CPU,
+  // a single element with the aggregate value (e.g., {"CPU": [8.0]}).
+  map<string, ResourceInstances> resources_available_instances = 9;
 }
 
 message RaySyncMessage {
diff --git a/src/ray/raylet/scheduling/cluster_lease_manager.cc b/src/ray/raylet/scheduling/cluster_lease_manager.cc
index 945fb4642521..d532149a5425 100644
--- a/src/ray/raylet/scheduling/cluster_lease_manager.cc
+++ b/src/ray/raylet/scheduling/cluster_lease_manager.cc
@@ -353,8 +353,14 @@ void ClusterLeaseManager::FillResourceUsage(rpc::ResourcesData &data) {
       resource_view_sync_message);
   (*data.mutable_resources_total()) =
       std::move(*resource_view_sync_message.mutable_resources_total());
-  (*data.mutable_resources_available()) =
-      std::move(*resource_view_sync_message.mutable_resources_available());
+  for (const auto &[name, instances] :
+       resource_view_sync_message.resources_available_instances()) {
+    double sum = 0;
+    for (double v : instances.values()) {
+      sum += v;
+    }
+    (*data.mutable_resources_available())[name] = sum;
+  }
   data.set_object_pulls_queued(resource_view_sync_message.object_pulls_queued());
   data.set_idle_duration_ms(resource_view_sync_message.idle_duration_ms());
   data.set_is_draining(resource_view_sync_message.is_draining());
diff --git a/src/ray/raylet/scheduling/cluster_resource_manager.cc b/src/ray/raylet/scheduling/cluster_resource_manager.cc
index 1dc287d8e214..880f5831fdab 100644
--- a/src/ray/raylet/scheduling/cluster_resource_manager.cc
+++ b/src/ray/raylet/scheduling/cluster_resource_manager.cc
@@ -14,6 +14,8 @@
 
 #include "ray/raylet/scheduling/cluster_resource_manager.h"
 
+#include <algorithm>
+#include <cmath>
 #include <string>
 #include <utility>
 #include <vector>
@@ -82,16 +84,24 @@ bool ClusterResourceManager::UpdateNode(
 
   const auto resources_total =
       MapFromProtobuf(resource_view_sync_message.resources_total());
-  const auto resources_available =
-      MapFromProtobuf(resource_view_sync_message.resources_available());
   auto node_labels = MapFromProtobuf(resource_view_sync_message.labels());
-  NodeResources node_resources =
-      ResourceMapToNodeResources(resources_total, resources_available);
+
   NodeResources local_view;
   RAY_CHECK(GetNodeResources(node_id, &local_view));
 
-  local_view.total = std::move(node_resources.total);
-  local_view.available = std::move(node_resources.available);
+  local_view.total = NodeResourceSet(resources_total);
+
+  const auto &instances_map = resource_view_sync_message.resources_available_instances();
+  NodeResourceInstanceSet new_available(/*track_pg_index=*/false);
+  for (const auto &[resource_name, resource_instances] : instances_map) {
+    std::vector<FixedPoint> instances;
+    for (const auto &value : resource_instances.values()) {
+      instances.push_back(FixedPoint(value));
+    }
+    new_available.Set(ResourceID(resource_name), std::move(instances));
+  }
+  local_view.available = std::move(new_available);
+
   local_view.labels = std::move(node_labels);
   local_view.object_pulls_queued = resource_view_sync_message.object_pulls_queued();
 
@@ -161,25 +171,44 @@ void ClusterResourceManager::UpdateResourceCapacity(scheduling::NodeID node_id,
                                                     double resource_total) {
   auto it = nodes_.find(node_id);
   if (it == nodes_.end()) {
-    NodeResources node_resources;
-    it = nodes_.emplace(node_id, node_resources).first;
+    it = nodes_.emplace(node_id, NodeResources{}).first;
   }
 
-  auto local_view = it->second.GetMutableLocalView();
-  FixedPoint resource_total_fp(resource_total);
-  auto local_total = local_view->total.Get(resource_id);
-  auto local_available = local_view->available.Get(resource_id);
-  auto diff_capacity = resource_total_fp - local_total;
-  auto total = local_total + diff_capacity;
-  auto available = local_available + diff_capacity;
-  if (total < 0) {
-    total = 0;
-  }
-  if (available < 0) {
-    available = 0;
+  auto *local_view = it->second.GetMutableLocalView();
+  FixedPoint new_total = std::max(FixedPoint(resource_total), FixedPoint(0));
+  local_view->total.Set(resource_id, new_total);
+
+  // Only init available for new resources. Existing resources' available can't be
+  // correctly adjusted from a scalar total (don't know which instances to update).
+  if (!local_view->available.Has(resource_id)) {
+    // Build per-instance vector: unit-instance resources get N x 1.0, others
+    // get a single element.
+    std::vector<FixedPoint> instances;
+    if (resource_id.IsUnitInstanceResource()) {
+      size_t num = static_cast<size_t>(std::max(new_total.Double(), 0.0));
+      for (size_t i = 0; i < num; i++) {
+        instances.push_back(FixedPoint(1.0));
+      }
+    } else {
+      instances.push_back(std::max(new_total, FixedPoint(0)));
+    }
+    if (!instances.empty()) {
+      local_view->available.Set(resource_id, std::move(instances));
+    }
   }
-  local_view->total.Set(resource_id, total);
-  local_view->available.Set(resource_id, available);
+}
+
+void ClusterResourceManager::AddResourceInstances(
+    scheduling::NodeID node_id,
+    scheduling::ResourceID resource_id,
+    const std::vector<FixedPoint> &instances) {
+  auto it = nodes_.find(node_id);
+  RAY_CHECK(it != nodes_.end()) << "Node " << node_id.ToInt() << " not found.";
+
+  auto *local_view = it->second.GetMutableLocalView();
+  auto total_add = FixedPoint::Sum(instances);
+  local_view->total.Set(resource_id, local_view->total.Get(resource_id) + total_add);
+  local_view->available.Add(resource_id, instances);
 }
 
 bool ClusterResourceManager::DeleteResources(
@@ -192,7 +221,7 @@ bool ClusterResourceManager::DeleteResources(
   auto local_view = it->second.GetMutableLocalView();
   for (const auto &resource_id : resource_ids) {
     local_view->total.Set(resource_id, 0);
-    local_view->available.Set(resource_id, 0);
+    local_view->available.Remove(resource_id);
   }
   return true;
 }
@@ -208,23 +237,37 @@ const absl::flat_hash_map<scheduling::NodeID, Node>
   return nodes_;
 }
 
-bool ClusterResourceManager::SubtractNodeAvailableResources(
+std::optional<ResourceAllocation> ClusterResourceManager::SubtractNodeAvailableResources(
     scheduling::NodeID node_id, const ResourceRequest &resource_request) {
   auto it = nodes_.find(node_id);
   if (it == nodes_.end()) {
-    return false;
+    return std::nullopt;
   }
 
   NodeResources *resources = it->second.GetMutableLocalView();
 
-  resources->available -= resource_request.GetResourceSet();
-  resources->available.RemoveNegative();
+  // Use single-resource TryAllocate (not the multi-resource variant) because the
+  // multi-resource version has PG cross-bundle logic that only applies to local
+  // allocation, not speculative remote deduction.
+  ResourceAllocation allocation;
+  for (const auto &[resource_id, demand] :
+       resource_request.GetResourceSet().Resources()) {
+    auto alloc = resources->available.TryAllocate(resource_id, demand);
+    if (!alloc.has_value()) {
+      // Rollback already applied allocations.
+      for (const auto &[rid, instances] : allocation) {
+        resources->available.Free(rid, instances);
+      }
+      return std::nullopt;
+    }
+    allocation[resource_id] = std::move(*alloc);
+  }
 
   // TODO(swang): We should also subtract object store memory if the task has
   // arguments. Right now we do not modify object_pulls_queued in case of
   // performance regressions in spillback.
 
-  return true;
+  return allocation;
 }
 
 bool ClusterResourceManager::HasFeasibleResources(
@@ -250,24 +293,16 @@ bool ClusterResourceManager::HasAvailableResources(
                                                ignore_object_store_memory_requirement);
 }
 
-bool ClusterResourceManager::AddNodeAvailableResources(scheduling::NodeID node_id,
-                                                       const ResourceSet &resource_set) {
+bool ClusterResourceManager::AddNodeAvailableResources(
+    scheduling::NodeID node_id, const ResourceAllocation &allocation) {
   auto it = nodes_.find(node_id);
   if (it == nodes_.end()) {
     return false;
   }
 
   auto node_resources = it->second.GetMutableLocalView();
-  for (auto &resource_id : resource_set.ResourceIds()) {
-    if (node_resources->total.Has(resource_id)) {
-      auto available = node_resources->available.Get(resource_id);
-      auto total = node_resources->total.Get(resource_id);
-      auto new_available = available + resource_set.Get(resource_id);
-      if (new_available > total) {
-        new_available = total;
-      }
-      node_resources->available.Set(resource_id, new_available);
-    }
+  for (const auto &[resource_id, instances] : allocation) {
+    node_resources->available.Free(resource_id, instances);
   }
   return true;
 }
diff --git a/src/ray/raylet/scheduling/cluster_resource_manager.h b/src/ray/raylet/scheduling/cluster_resource_manager.h
index 7c80b0295da3..1e98b15d7feb 100644
--- a/src/ray/raylet/scheduling/cluster_resource_manager.h
+++ b/src/ray/raylet/scheduling/cluster_resource_manager.h
@@ -70,15 +70,20 @@ class ClusterResourceManager {
   /// Get number of nodes in the cluster.
   int64_t NumNodes() const;
 
-  /// Update total capacity of a given resource of a given node.
-  ///
-  /// \param node_id: Node whose resource we want to update.
-  /// \param resource_id: Resource which we want to update.
-  /// \param resource_total: New capacity of the resource.
+  /// Update the total capacity of a resource on a node. Only sets total; for
+  /// resources that don't yet exist in available, initializes available = total.
+  /// Existing resources' available is untouched -- a scalar total change can't be
+  /// correctly distributed across per-instance available.
   void UpdateResourceCapacity(scheduling::NodeID node_id,
                               scheduling::ResourceID resource_id,
                               double resource_total);
 
+  /// Add per-instance capacity to a resource. Updates both total (scalar sum
+  /// of instances added) and available (element-wise addition).
+  void AddResourceInstances(scheduling::NodeID node_id,
+                            scheduling::ResourceID resource_id,
+                            const std::vector<FixedPoint> &instances);
+
   /// Delete a given resource from a given node.
   ///
   /// \param node_id: Node whose resource we want to delete.
@@ -94,9 +99,9 @@ class ClusterResourceManager {
   const NodeResources &GetNodeResources(scheduling::NodeID node_id) const;
 
   /// Subtract available resource from a given node.
-  /// Return false if such node doesn't exist.
-  bool SubtractNodeAvailableResources(scheduling::NodeID node_id,
-                                      const ResourceRequest &resource_request);
+  /// Return std::nullopt if such node doesn't exist or allocation fails.
+  std::optional<ResourceAllocation> SubtractNodeAvailableResources(
+      scheduling::NodeID node_id, const ResourceRequest &resource_request);
 
   /// Check if we have available resources to fullfill resource request for an given node.
   ///
@@ -111,10 +116,10 @@ class ClusterResourceManager {
   bool HasFeasibleResources(scheduling::NodeID node_id,
                             const ResourceRequest &resource_request) const;
 
-  /// Add available resource to a given node.
-  /// Return false if such node doesn't exist.
+  /// Restore previously subtracted resources using a precise per-instance allocation.
+  /// Returns false if the node doesn't exist.
   bool AddNodeAvailableResources(scheduling::NodeID node_id,
-                                 const ResourceSet &resource_set);
+                                 const ResourceAllocation &allocation);
 
   /// Return if the node is tracked.
   bool HasNode(const scheduling::NodeID &node_id) const {
diff --git a/src/ray/raylet/scheduling/cluster_resource_scheduler.cc b/src/ray/raylet/scheduling/cluster_resource_scheduler.cc
index 1ba18e346ca8..e1ff141c09c6 100644
--- a/src/ray/raylet/scheduling/cluster_resource_scheduler.cc
+++ b/src/ray/raylet/scheduling/cluster_resource_scheduler.cc
@@ -255,8 +255,9 @@ bool ClusterResourceScheduler::SubtractRemoteNodeAvailableResources(
   if (!IsSchedulable(resource_request, node_id)) {
     return false;
   }
-  return cluster_resource_manager_->SubtractNodeAvailableResources(node_id,
-                                                                   resource_request);
+  return cluster_resource_manager_
+      ->SubtractNodeAvailableResources(node_id, resource_request)
+      .has_value();
 }
 
 std::string ClusterResourceScheduler::DebugString(void) const {
diff --git a/src/ray/raylet/scheduling/local_resource_manager.cc b/src/ray/raylet/scheduling/local_resource_manager.cc
index 65e5c20c34ab..b8e69f658bf7 100644
--- a/src/ray/raylet/scheduling/local_resource_manager.cc
+++ b/src/ray/raylet/scheduling/local_resource_manager.cc
@@ -44,7 +44,7 @@ LocalResourceManager::LocalResourceManager(
       shutdown_raylet_gracefully_(shutdown_raylet_gracefully),
       resource_change_subscriber_(resource_change_subscriber),
       resource_usage_gauge_(resource_usage_gauge) {
-  RAY_CHECK(node_resources.total == node_resources.available);
+  RAY_CHECK(node_resources.total == node_resources.available.ToNodeResourceSet());
   local_resources_.available = NodeResourceInstanceSet(node_resources.total);
   local_resources_.total = NodeResourceInstanceSet(node_resources.total);
   local_resources_.labels = node_resources.labels;
@@ -317,7 +317,7 @@ void LocalResourceManager::ReleaseWorkerResources(
 
 NodeResources LocalResourceManager::ToNodeResources() const {
   NodeResources node_resources;
-  node_resources.available = local_resources_.available.ToNodeResourceSet();
+  node_resources.available = local_resources_.available;
   node_resources.total = local_resources_.total.ToNodeResourceSet();
   node_resources.labels = local_resources_.labels;
   node_resources.is_draining = IsLocalNodeDraining();
@@ -374,14 +374,19 @@ void LocalResourceManager::PopulateResourceViewSyncMessage(
   resource_view_sync_message.mutable_resources_total()->insert(total.begin(),
                                                                total.end());
 
-  for (const auto &[resource_name, available] : resources.available.GetResourceMap()) {
-    // Resource availability can be negative locally but treat it as 0
-    // when we broadcast to others since other parts of the
-    // system assume resource availability cannot be negative and
-    // there is no difference between negative and zero from other nodes
-    // and gcs's point of view.
-    (*resource_view_sync_message.mutable_resources_available())[resource_name] =
-        std::max(available, 0.0);
+  // Resource availability can be negative locally but treat it as 0
+  // when we broadcast to others since other parts of the
+  // system assume resource availability cannot be negative and
+  // there is no difference between negative and zero from other nodes
+  // and gcs's point of view.
+  for (const auto &[resource_id, instances] : resources.available.Resources()) {
+    rpc::syncer::ResourceInstances resource_instances;
+    for (const auto &value : instances) {
+      resource_instances.add_values(std::max(value.Double(), 0.0));
+    }
+    (*resource_view_sync_message
+          .mutable_resources_available_instances())[resource_id.Binary()] =
+        std::move(resource_instances);
   }
 
   if (get_pull_manager_at_capacity_ != nullptr) {
diff --git a/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc b/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc
index cf3aeff2c90a..b3a2536096f2 100644
--- a/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc
+++ b/src/ray/raylet/scheduling/policy/bundle_scheduling_policy.cc
@@ -177,6 +177,8 @@ SchedulingResult BundlePackSchedulingPolicy::Schedule(
 
   std::vector<scheduling::NodeID> result_nodes;
   result_nodes.resize(sorted_resource_request_list.size());
+  std::vector<std::optional<ResourceAllocation>> allocations;
+  allocations.resize(sorted_resource_request_list.size());
   std::list<std::pair<int, const ResourceRequest *>> required_resources_list_copy;
   int index = 0;
   for (const auto &resource_request : sorted_resource_request_list) {
@@ -194,8 +196,10 @@ SchedulingResult BundlePackSchedulingPolicy::Schedule(
 
     const auto &node_resources = best_node.second->GetLocalView();
 
-    RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
-        best_node.first, *required_resources));
+    auto allocation = cluster_resource_manager_.SubtractNodeAvailableResources(
+        best_node.first, *required_resources);
+    RAY_CHECK(allocation.has_value());
+    allocations[required_resources_index] = std::move(allocation);
     result_nodes[required_resources_index] = best_node.first;
     required_resources_list_copy.pop_front();
 
@@ -204,8 +208,10 @@ SchedulingResult BundlePackSchedulingPolicy::Schedule(
          iter != required_resources_list_copy.end();) {
       // If the node has sufficient resources, allocate it.
       if (node_resources.IsAvailable(*iter->second)) {
-        RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
-            best_node.first, *iter->second));
+        auto iter_allocation = cluster_resource_manager_.SubtractNodeAvailableResources(
+            best_node.first, *iter->second);
+        RAY_CHECK(iter_allocation.has_value());
+        allocations[iter->first] = std::move(iter_allocation);
         result_nodes[iter->first] = best_node.first;
         required_resources_list_copy.erase(iter++);
       } else {
@@ -219,10 +225,9 @@ SchedulingResult BundlePackSchedulingPolicy::Schedule(
   // Releasing the resources temporarily deducted from `cluster_resource_manager_`.
   for (size_t res_node_idx = 0; res_node_idx < result_nodes.size(); res_node_idx++) {
     // If `PackSchedule` fails, the id of some nodes may be nil.
-    if (!result_nodes[res_node_idx].IsNil()) {
+    if (!result_nodes[res_node_idx].IsNil() && allocations[res_node_idx].has_value()) {
       RAY_CHECK(cluster_resource_manager_.AddNodeAvailableResources(
-          result_nodes[res_node_idx],
-          (*sorted_resource_request_list[res_node_idx]).GetResourceSet()));
+          result_nodes[res_node_idx], allocations[res_node_idx].value()));
     }
   }
 
@@ -258,6 +263,7 @@ SchedulingResult BundleSpreadSchedulingPolicy::Schedule(
   }
 
   std::vector<scheduling::NodeID> result_nodes;
+  std::vector<std::optional<ResourceAllocation>> allocations;
   absl::flat_hash_map<scheduling::NodeID, const Node *> selected_nodes;
   for (const auto &resource_request : sorted_resource_request_list) {
     // Score and sort nodes.
@@ -266,8 +272,10 @@ SchedulingResult BundleSpreadSchedulingPolicy::Schedule(
     // There are nodes to meet the scheduling requirements.
     if (!best_node.first.IsNil()) {
       result_nodes.emplace_back(best_node.first);
-      RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
-          best_node.first, *resource_request));
+      auto allocation = cluster_resource_manager_.SubtractNodeAvailableResources(
+          best_node.first, *resource_request);
+      RAY_CHECK(allocation.has_value());
+      allocations.emplace_back(std::move(allocation));
       candidate_nodes.erase(result_nodes.back());
       selected_nodes.emplace(best_node);
     } else {
@@ -275,8 +283,10 @@ SchedulingResult BundleSpreadSchedulingPolicy::Schedule(
       best_node = GetBestNode(*resource_request, selected_nodes, options);
       if (!best_node.first.IsNil()) {
         result_nodes.emplace_back(best_node.first);
-        RAY_CHECK(cluster_resource_manager_.SubtractNodeAvailableResources(
-            best_node.first, *resource_request));
+        auto allocation = cluster_resource_manager_.SubtractNodeAvailableResources(
+            best_node.first, *resource_request);
+        RAY_CHECK(allocation.has_value());
+        allocations.emplace_back(std::move(allocation));
       } else {
         break;
       }
@@ -285,10 +295,10 @@ SchedulingResult BundleSpreadSchedulingPolicy::Schedule(
 
   // Releasing the resources temporarily deducted from `cluster_resource_manager_`.
   for (size_t index = 0; index < result_nodes.size(); index++) {
-    // If `PackSchedule` fails, the id of some nodes may be nil.
-    if (!result_nodes[index].IsNil()) {
+    // If `SpreadSchedule` fails, the id of some nodes may be nil.
+    if (!result_nodes[index].IsNil() && allocations[index].has_value()) {
       RAY_CHECK(cluster_resource_manager_.AddNodeAvailableResources(
-          result_nodes[index], (*sorted_resource_request_list[index]).GetResourceSet()));
+          result_nodes[index], allocations[index].value()));
     }
   }
 
@@ -347,32 +357,63 @@ SchedulingResult BundleStrictPackSchedulingPolicy::Schedule(
     return SchedulingResult::Infeasible();
   }
 
-  std::pair<scheduling::NodeID, const Node *> best_node(scheduling::NodeID::Nil(),
-                                                        nullptr);
+  // Try each candidate node by allocating bundles one by one (per-bundle
+  // SubtractNodeAvailableResources). This avoids the aggregated-request problem
+  // where per-instance CanAllocate rejects a summed demand that could be
+  // satisfied by distributing across instances.
+  auto try_allocate_all_bundles = [&](scheduling::NodeID node_id) -> bool {
+    std::vector<ResourceAllocation> allocs;
+    for (const auto *request : resource_request_list) {
+      auto alloc =
+          cluster_resource_manager_.SubtractNodeAvailableResources(node_id, *request);
+      if (!alloc.has_value()) {
+        for (auto &prev : allocs) {
+          cluster_resource_manager_.AddNodeAvailableResources(node_id, prev);
+        }
+        return false;
+      }
+      allocs.push_back(std::move(*alloc));
+    }
+    for (auto &a : allocs) {
+      cluster_resource_manager_.AddNodeAvailableResources(node_id, a);
+    }
+    return true;
+  };
+
+  // Prefer the soft target node if specified and viable.
+  scheduling::NodeID best_node_id = scheduling::NodeID::Nil();
   if (!options.bundle_strict_pack_soft_target_node_id_.IsNil()) {
-    if (candidate_nodes.contains(options.bundle_strict_pack_soft_target_node_id_)) {
-      best_node = GetBestNode(
-          aggregated_resource_request,
-          absl::flat_hash_map<scheduling::NodeID, const ray::Node *>{
-              {options.bundle_strict_pack_soft_target_node_id_,
-               candidate_nodes[options.bundle_strict_pack_soft_target_node_id_]}},
-          options);
+    if (candidate_nodes.contains(options.bundle_strict_pack_soft_target_node_id_) &&
+        try_allocate_all_bundles(options.bundle_strict_pack_soft_target_node_id_)) {
+      best_node_id = options.bundle_strict_pack_soft_target_node_id_;
     }
   }
 
-  if (best_node.first.IsNil()) {
-    best_node = GetBestNode(aggregated_resource_request, candidate_nodes, options);
+  if (best_node_id.IsNil()) {
+    // Score viable candidates per-bundle using the existing scorer.
+    // try_allocate_all_bundles already verified all bundles fit, so each
+    // individual Score(bundle) will pass IsAvailable.
+    double best_score = -1;
+    for (const auto &[node_id, node] : candidate_nodes) {
+      if (!try_allocate_all_bundles(node_id)) {
+        continue;
+      }
+      double score = 0;
+      for (const auto *request : resource_request_list) {
+        score += node_scorer_->Score(*request, node->GetLocalView());
+      }
+      if (best_node_id.IsNil() || score > best_score) {
+        best_score = score;
+        best_node_id = node_id;
+      }
+    }
   }
 
-  // Select the node with the highest score.
-  // `StrictPackSchedule` does not need to consider the scheduling context, because it
-  // only schedules to a node and triggers rescheduling when node dead.
   std::vector<scheduling::NodeID> result_nodes;
-  if (!best_node.first.IsNil()) {
-    result_nodes.resize(resource_request_list.size(), best_node.first);
+  if (!best_node_id.IsNil()) {
+    result_nodes.resize(resource_request_list.size(), best_node_id);
   }
   if (result_nodes.empty()) {
-    // Can't meet the scheduling requirements temporarily.
     return SchedulingResult::Failed();
   }
 
diff --git a/src/ray/raylet/scheduling/policy/scorer.cc b/src/ray/raylet/scheduling/policy/scorer.cc
index 3fb655ac8df7..c9ccff7e1a5a 100644
--- a/src/ray/raylet/scheduling/policy/scorer.cc
+++ b/src/ray/raylet/scheduling/policy/scorer.cc
@@ -26,7 +26,7 @@ double LeastResourceScorer::Score(const ResourceRequest &required_resources,
   double node_score = 0.;
   for (auto &resource_id : required_resources.ResourceIds()) {
     const auto &request_resource = required_resources.Get(resource_id);
-    const auto &node_available_resource = node_resources.available.Get(resource_id);
+    auto node_available_resource = node_resources.available.Sum(resource_id);
     node_score += Calculate(request_resource, node_available_resource);
   }
   return node_score;
diff --git a/src/ray/raylet/scheduling/policy/tests/hybrid_scheduling_policy_test.cc b/src/ray/raylet/scheduling/policy/tests/hybrid_scheduling_policy_test.cc
index f0a0042ae3ac..d43750370685 100644
--- a/src/ray/raylet/scheduling/policy/tests/hybrid_scheduling_policy_test.cc
+++ b/src/ray/raylet/scheduling/policy/tests/hybrid_scheduling_policy_test.cc
@@ -32,9 +32,21 @@ NodeResources CreateNodeResources(double available_cpu,
                                   double available_gpu,
                                   double total_gpu) {
   NodeResources resources;
-  resources.available.Set(ResourceID::CPU(), available_cpu)
-      .Set(ResourceID::Memory(), available_memory)
-      .Set(ResourceID::GPU(), available_gpu);
+  resources.available.Set(ResourceID::CPU(), {FixedPoint(available_cpu)})
+      .Set(ResourceID::Memory(), {FixedPoint(available_memory)});
+  size_t num_gpu = static_cast<size_t>(total_gpu);
+  if (num_gpu > 0) {
+    std::vector<FixedPoint> gpu_instances;
+    double remaining_avail = available_gpu;
+    for (size_t i = 0; i < num_gpu; i++) {
+      double per_instance = std::min(remaining_avail, 1.0);
+      gpu_instances.push_back(FixedPoint(std::max(per_instance, 0.0)));
+      remaining_avail -= per_instance;
+    }
+    resources.available.Set(ResourceID::GPU(), std::move(gpu_instances));
+  } else if (available_gpu > 0) {
+    resources.available.Set(ResourceID::GPU(), {FixedPoint(available_gpu)});
+  }
   resources.total.Set(ResourceID::CPU(), total_cpu)
       .Set(ResourceID::Memory(), total_memory)
       .Set(ResourceID::GPU(), total_gpu);
diff --git a/src/ray/raylet/scheduling/policy/tests/scheduling_policy_test.cc b/src/ray/raylet/scheduling/policy/tests/scheduling_policy_test.cc
index 38c209791459..7e52b4f710a1 100644
--- a/src/ray/raylet/scheduling/policy/tests/scheduling_policy_test.cc
+++ b/src/ray/raylet/scheduling/policy/tests/scheduling_policy_test.cc
@@ -30,9 +30,21 @@ NodeResources CreateNodeResources(double available_cpu,
                                   double available_gpu,
                                   double total_gpu) {
   NodeResources resources;
-  resources.available.Set(ResourceID::CPU(), available_cpu)
-      .Set(ResourceID::Memory(), available_memory)
-      .Set(ResourceID::GPU(), available_gpu);
+  resources.available.Set(ResourceID::CPU(), {FixedPoint(available_cpu)})
+      .Set(ResourceID::Memory(), {FixedPoint(available_memory)});
+  size_t num_gpu = static_cast<size_t>(total_gpu);
+  if (num_gpu > 0) {
+    std::vector<FixedPoint> gpu_instances;
+    double remaining_avail = available_gpu;
+    for (size_t i = 0; i < num_gpu; i++) {
+      double per_instance = std::min(remaining_avail, 1.0);
+      gpu_instances.push_back(FixedPoint(std::max(per_instance, 0.0)));
+      remaining_avail -= per_instance;
+    }
+    resources.available.Set(ResourceID::GPU(), std::move(gpu_instances));
+  } else if (available_gpu > 0) {
+    resources.available.Set(ResourceID::GPU(), {FixedPoint(available_gpu)});
+  }
   resources.total.Set(ResourceID::CPU(), total_cpu)
       .Set(ResourceID::Memory(), total_memory)
       .Set(ResourceID::GPU(), total_gpu);
@@ -232,7 +244,7 @@ TEST_F(SchedulingPolicyTest, AvailableDefinitionTest) {
   auto task_req2 = ResourceMapToResourceRequest({{"CPU", 1}}, false);
 
   NodeResources resources;
-  resources.available.Set(ResourceID::CPU(), 2.0);
+  resources.available.Set(ResourceID::CPU(), {FixedPoint(2.0)});
   resources.total.Set(ResourceID::CPU(), 2.0);
   ASSERT_FALSE(resources.IsAvailable(task_req1));
   ASSERT_TRUE(resources.IsAvailable(task_req2));
@@ -241,17 +253,17 @@ TEST_F(SchedulingPolicyTest, AvailableDefinitionTest) {
 TEST_F(SchedulingPolicyTest, CriticalResourceUtilizationDefinitionTest) {
   {
     NodeResources resources;
-    resources.available.Set(ResourceID::CPU(), 1.0);
+    resources.available.Set(ResourceID::CPU(), {FixedPoint(1.0)});
     resources.total.Set(ResourceID::CPU(), 2.0);
     ASSERT_EQ(resources.CalculateCriticalResourceUtilization(), 0.5);
   }
   {
     // Basic test of max
     NodeResources resources;
-    resources.available.Set(ResourceID::CPU(), 1.0)
-        .Set(ResourceID::Memory(), 0.25)
-        .Set(ResourceID::GPU(), 1)
-        .Set(ResourceID::ObjectStoreMemory(), 50);
+    resources.available.Set(ResourceID::CPU(), {FixedPoint(1.0)})
+        .Set(ResourceID::Memory(), {FixedPoint(0.25)})
+        .Set(ResourceID::GPU(), {FixedPoint(1), FixedPoint(0)})
+        .Set(ResourceID::ObjectStoreMemory(), {FixedPoint(50)});
     resources.total.Set(ResourceID::CPU(), 2.0)
         .Set(ResourceID::Memory(), 1)
         .Set(ResourceID::GPU(), 2)
@@ -262,10 +274,10 @@ TEST_F(SchedulingPolicyTest, CriticalResourceUtilizationDefinitionTest) {
   {
     // Skip GPU
     NodeResources resources;
-    resources.available.Set(ResourceID::CPU(), 1.0)
-        .Set(ResourceID::Memory(), 0.25)
-        .Set(ResourceID::GPU(), 0)
-        .Set(ResourceID::ObjectStoreMemory(), 50);
+    resources.available.Set(ResourceID::CPU(), {FixedPoint(1.0)})
+        .Set(ResourceID::Memory(), {FixedPoint(0.25)})
+        .Set(ResourceID::GPU(), {FixedPoint(0), FixedPoint(0)})
+        .Set(ResourceID::ObjectStoreMemory(), {FixedPoint(50)});
     resources.total.Set(ResourceID::CPU(), 2.0)
         .Set(ResourceID::Memory(), 1)
         .Set(ResourceID::GPU(), 2)
@@ -565,6 +577,27 @@ TEST_F(SchedulingPolicyTest, StrictPackBundleSchedulingTest) {
   ASSERT_EQ(to_schedule.selected_nodes[0], local_node);
 }
 
+TEST_F(SchedulingPolicyTest, StrictPackFractionalUnitResourceTest) {
+  NodeResources fragmented;
+  fragmented.available.Set(ResourceID::CPU(), {FixedPoint(10)});
+  fragmented.available.Set(ResourceID::GPU(), {FixedPoint(0.7), FixedPoint(0.3)});
+  fragmented.total.Set(ResourceID::CPU(), 10);
+  fragmented.total.Set(ResourceID::GPU(), 2.0);
+  nodes.emplace(local_node, fragmented);
+  auto cluster_resource_manager = MockClusterResourceManager(nodes);
+
+  ResourceRequest req = ResourceMapToResourceRequest({{"GPU", 0.5}}, false);
+  std::vector<const ResourceRequest *> two_bundles(2, &req);
+
+  auto op = SchedulingOptions::BundleStrictPack(scheduling::NodeID::Nil());
+  auto result = raylet_scheduling_policy::BundleStrictPackSchedulingPolicy(
+                    *cluster_resource_manager, [](auto) { return true; })
+                    .Schedule(two_bundles, op);
+  // bundle1 takes 0.5 from the 0.7 instance -> [0.2, 0.3].
+  // bundle2 needs 0.5 but max available is 0.3 -> rejected.
+  ASSERT_FALSE(result.status.IsSuccess());
+}
+
 TEST_F(SchedulingPolicyTest, StrictPackBundleLabelSelectorSuccessTest) {
   nodes.emplace(local_node, CreateNodeResourcesWithLabels(10, 10, {{"zone", "us-east"}}));
   nodes.emplace(remote_node,
diff --git a/src/ray/raylet/scheduling/tests/cluster_lease_manager_test.cc b/src/ray/raylet/scheduling/tests/cluster_lease_manager_test.cc
index a302300d2e91..5cb20178039a 100644
--- a/src/ray/raylet/scheduling/tests/cluster_lease_manager_test.cc
+++ b/src/ray/raylet/scheduling/tests/cluster_lease_manager_test.cc
@@ -2288,18 +2288,18 @@ TEST_F(ClusterLeaseManagerTest, NegativePlacementGroupCpuResources) {
 
   // ray.get() returns and worker1 acquires the CPU resource again
   ASSERT_TRUE(local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker1));
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), -1);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_1_aaa")), 1);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_0_aaa")), -1);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_1_aaa")), 1);
 
   auto worker3 = std::make_shared<MockWorker>(WorkerID::FromRandom(), 7678);
   allocated_instances = std::make_shared<TaskResourceInstances>();
   ASSERT_TRUE(scheduler_->GetLocalResourceManager().AllocateLocalTaskResources(
       {{"CPU_group_aaa", 1.}, {"CPU_group_1_aaa", 1.}}, allocated_instances));
   worker3->SetAllocatedInstances(allocated_instances);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), -1);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), -1);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_1_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_aaa")), -1);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_0_aaa")), -1);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_1_aaa")), 0);
 }
 
 TEST_F(ClusterLeaseManagerTestWithGPUsAtHead, ReleaseAndReturnWorkerCpuResources) {
@@ -2316,8 +2316,8 @@ TEST_F(ClusterLeaseManagerTestWithGPUsAtHead, ReleaseAndReturnWorkerCpuResources
   const NodeResources &node_resources =
       scheduler_->GetClusterResourceManager().GetNodeResources(
           scheduling::NodeID(id_.Binary()));
-  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 8);
-  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 4);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::CPU()), 8);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::GPU()), 4);
 
   auto worker1 = std::make_shared<MockWorker>(WorkerID::FromRandom(), 1234);
   auto worker2 = std::make_shared<MockWorker>(WorkerID::FromRandom(), 5678);
@@ -2347,24 +2347,24 @@ TEST_F(ClusterLeaseManagerTestWithGPUsAtHead, ReleaseAndReturnWorkerCpuResources
   worker2->SetAllocatedInstances(allocated_instances);
 
   // Check that the resources are allocated successfully.
-  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 7);
-  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 3);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_0_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::CPU()), 7);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::GPU()), 3);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_0_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("GPU_group_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("GPU_group_0_aaa")), 0);
 
   // Check that the cpu resources are released successfully.
   ASSERT_TRUE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker1));
   ASSERT_TRUE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker2));
 
   // Check that only cpu resources are released.
-  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 8);
-  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 3);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 1);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), 1);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_0_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::CPU()), 8);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::GPU()), 3);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_aaa")), 1);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_0_aaa")), 1);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("GPU_group_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("GPU_group_0_aaa")), 0);
 
   // Mark worker as blocked.
   worker1->MarkBlocked();
@@ -2373,24 +2373,24 @@ TEST_F(ClusterLeaseManagerTestWithGPUsAtHead, ReleaseAndReturnWorkerCpuResources
   ASSERT_FALSE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker1));
   ASSERT_FALSE(local_lease_manager_->ReleaseCpuResourcesFromBlockedWorker(worker2));
   // Check nothing will be changed.
-  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 8);
-  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 3);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 1);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), 1);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_0_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::CPU()), 8);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::GPU()), 3);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_aaa")), 1);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_0_aaa")), 1);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("GPU_group_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("GPU_group_0_aaa")), 0);
 
   // Check that the cpu resources are returned back to worker successfully.
   ASSERT_TRUE(local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker1));
   ASSERT_TRUE(local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker2));
 
   // Check that only cpu resources are returned back to the worker.
-  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 7);
-  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 3);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_0_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::CPU()), 7);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::GPU()), 3);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_0_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("GPU_group_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("GPU_group_0_aaa")), 0);
 
   // Mark worker as unblocked.
   worker1->MarkUnblocked();
@@ -2398,12 +2398,12 @@ TEST_F(ClusterLeaseManagerTestWithGPUsAtHead, ReleaseAndReturnWorkerCpuResources
   ASSERT_FALSE(local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker1));
   ASSERT_FALSE(local_lease_manager_->ReturnCpuResourcesToUnblockedWorker(worker2));
   // Check nothing will be changed.
-  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 7);
-  ASSERT_EQ(node_resources.available.Get(ResourceID::GPU()), 3);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU_group_0_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_aaa")), 0);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("GPU_group_0_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::CPU()), 7);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::GPU()), 3);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU_group_0_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("GPU_group_aaa")), 0);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("GPU_group_0_aaa")), 0);
 }
 
 TEST_F(ClusterLeaseManagerTest, TestSpillWaitingLeases) {
diff --git a/src/ray/raylet/scheduling/tests/cluster_resource_manager_test.cc b/src/ray/raylet/scheduling/tests/cluster_resource_manager_test.cc
index 2ae6b1cc0a7c..b43506e654e7 100644
--- a/src/ray/raylet/scheduling/tests/cluster_resource_manager_test.cc
+++ b/src/ray/raylet/scheduling/tests/cluster_resource_manager_test.cc
@@ -26,9 +26,10 @@ NodeResources CreateNodeResources(double available_cpu,
                                   double total_custom_resource = 0,
                                   bool object_pulls_queued = false) {
   NodeResources resources;
-  resources.available.Set(ResourceID::CPU(), available_cpu);
+  resources.available.Set(ResourceID::CPU(), {FixedPoint(available_cpu)});
   resources.total.Set(ResourceID::CPU(), total_cpu);
-  resources.available.Set(scheduling::ResourceID("CUSTOM"), available_custom_resource);
+  resources.available.Set(scheduling::ResourceID("CUSTOM"),
+                          {FixedPoint(available_custom_resource)});
   resources.total.Set(scheduling::ResourceID("CUSTOM"), total_custom_resource);
   resources.object_pulls_queued = object_pulls_queued;
   return resources;
@@ -53,6 +54,9 @@ struct ClusterResourceManagerTest : public ::testing::Test {
                                                  /*total_custom*/ 1,
                                                  /*object_pulls_queued*/ true));
   }
+  void AddNode(scheduling::NodeID node_id, const NodeResources &resources) {
+    manager->AddOrUpdateNode(node_id, resources);
+  }
   scheduling::NodeID node0 = scheduling::NodeID(0);
   scheduling::NodeID node1 = scheduling::NodeID(1);
   scheduling::NodeID node2 = scheduling::NodeID(2);
@@ -64,7 +68,9 @@ TEST_F(ClusterResourceManagerTest, UpdateNode) {
   // Prepare a sync message with updated totals/available, labels and flags.
   syncer::ResourceViewSyncMessage payload;
   payload.mutable_resources_total()->insert({"CPU", 10.0});
-  payload.mutable_resources_available()->insert({"CPU", 5.0});
+  rpc::syncer::ResourceInstances cpu_instances;
+  cpu_instances.add_values(5.0);
+  (*payload.mutable_resources_available_instances())["CPU"] = cpu_instances;
   payload.mutable_labels()->insert({"zone", "us-east-1a"});
   payload.set_object_pulls_queued(true);
   payload.set_idle_duration_ms(42);
@@ -76,7 +82,7 @@ TEST_F(ClusterResourceManagerTest, UpdateNode) {
 
   const auto &node_resources = manager->GetNodeResources(node0);
   ASSERT_EQ(node_resources.total.Get(scheduling::ResourceID("CPU")), 10);
-  ASSERT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU")), 5);
+  ASSERT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU")), 5);
   ASSERT_EQ(node_resources.labels.at("zone"), "us-east-1a");
   ASSERT_TRUE(node_resources.object_pulls_queued);
   ASSERT_EQ(node_resources.idle_resource_duration_ms, 42);
@@ -115,10 +121,13 @@ TEST_F(ClusterResourceManagerTest, HasFeasibleResourcesTest) {
       node0,
       ResourceMapToResourceRequest({{"CPU", 1}},
                                    /*requires_object_store_memory=*/false)));
-  manager->SubtractNodeAvailableResources(
-      node0,
-      ResourceMapToResourceRequest({{"CPU", 1}},
-                                   /*requires_object_store_memory=*/false));
+  ASSERT_TRUE(
+      manager
+          ->SubtractNodeAvailableResources(
+              node0,
+              ResourceMapToResourceRequest({{"CPU", 1}},
+                                           /*requires_object_store_memory=*/false))
+          .has_value());
   // node0 has no available CPU resource but it's still feasible.
   ASSERT_TRUE(manager->HasFeasibleResources(
       node0,
@@ -163,26 +172,61 @@ TEST_F(ClusterResourceManagerTest, HasAvailableResourcesTest) {
 
 TEST_F(ClusterResourceManagerTest, SubtractAndAddNodeAvailableResources) {
   const auto &node_resources = manager->GetNodeResources(node0);
-  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 1);
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::CPU()), 1);
 
-  manager->SubtractNodeAvailableResources(
+  auto allocation = manager->SubtractNodeAvailableResources(
       node0,
       ResourceMapToResourceRequest({{"CPU", 1}},
                                    /*requires_object_store_memory=*/false));
-  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 0);
-  // Subtract again and make sure the available == 0.
-  manager->SubtractNodeAvailableResources(
+  ASSERT_TRUE(allocation.has_value());
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::CPU()), 0);
+
+  auto allocation2 = manager->SubtractNodeAvailableResources(
       node0,
       ResourceMapToResourceRequest({{"CPU", 1}},
                                    /*requires_object_store_memory=*/false));
-  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 0);
-
-  // Add resources back.
-  manager->AddNodeAvailableResources(node0, ResourceSet({{"CPU", FixedPoint(1)}}));
-  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 1);
-  // Add again and make sure the available == 1 (<= total).
-  manager->AddNodeAvailableResources(node0, ResourceSet({{"CPU", FixedPoint(1)}}));
-  ASSERT_EQ(node_resources.available.Get(ResourceID::CPU()), 1);
+  ASSERT_FALSE(allocation2.has_value());
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::CPU()), 0);
+
+  manager->AddNodeAvailableResources(node0, allocation.value());
+  ASSERT_EQ(node_resources.available.Sum(ResourceID::CPU()), 1);
+}
+
+TEST_F(ClusterResourceManagerTest, FractionalUnitResourceRejectsFragmentedNode) {
+  NodeResources gpu_node;
+  gpu_node.available.Set(ResourceID::GPU(), {FixedPoint(1.0), FixedPoint(1.0)});
+  gpu_node.total.Set(ResourceID::GPU(), 2.0);
+  scheduling::NodeID gpu_id = scheduling::NodeID(10);
+  AddNode(gpu_id, gpu_node);
+
+  auto a1 = manager->SubtractNodeAvailableResources(
+      gpu_id,
+      ResourceMapToResourceRequest({{"GPU", 0.6}},
+                                   /*requires_object_store_memory=*/false));
+  ASSERT_TRUE(a1.has_value());
+  auto a2 = manager->SubtractNodeAvailableResources(
+      gpu_id,
+      ResourceMapToResourceRequest({{"GPU", 0.6}},
+                                   /*requires_object_store_memory=*/false));
+  ASSERT_TRUE(a2.has_value());
+
+  ASSERT_FALSE(manager->HasAvailableResources(
+      gpu_id,
+      ResourceMapToResourceRequest({{"GPU", 0.5}},
+                                   /*requires_object_store_memory=*/false),
+      /*ignore_object_store_memory_requirement=*/false));
+
+  ASSERT_TRUE(manager->HasAvailableResources(
+      gpu_id,
+      ResourceMapToResourceRequest({{"GPU", 0.4}},
+                                   /*requires_object_store_memory=*/false),
+      /*ignore_object_store_memory_requirement=*/false));
+
+  auto a3 = manager->SubtractNodeAvailableResources(
+      gpu_id,
+      ResourceMapToResourceRequest({{"GPU", 0.5}},
+                                   /*requires_object_store_memory=*/false));
+  ASSERT_FALSE(a3.has_value());
 }
 
 }  // namespace ray
diff --git a/src/ray/raylet/scheduling/tests/cluster_resource_scheduler_2_test.cc b/src/ray/raylet/scheduling/tests/cluster_resource_scheduler_2_test.cc
index 19d1a139a4aa..00ed9fba2f77 100644
--- a/src/ray/raylet/scheduling/tests/cluster_resource_scheduler_2_test.cc
+++ b/src/ray/raylet/scheduling/tests/cluster_resource_scheduler_2_test.cc
@@ -82,7 +82,7 @@ class GcsResourceSchedulerTest : public ::testing::Test {
     auto resource_id = scheduling::ResourceID(resource_name);
 
     ASSERT_TRUE(node_resources.available.Has(resource_id));
-    ASSERT_EQ(node_resources.available.Get(resource_id).Double(), resource_value);
+    ASSERT_EQ(node_resources.available.Sum(resource_id).Double(), resource_value);
   }
 
   void TestResourceLeaks(SchedulingOptions scheduling_options) {
diff --git a/src/ray/raylet/scheduling/tests/cluster_resource_scheduler_test.cc b/src/ray/raylet/scheduling/tests/cluster_resource_scheduler_test.cc
index c08d72249dd8..28277791b68e 100644
--- a/src/ray/raylet/scheduling/tests/cluster_resource_scheduler_test.cc
+++ b/src/ray/raylet/scheduling/tests/cluster_resource_scheduler_test.cc
@@ -618,9 +618,9 @@ TEST_F(ClusterResourceSchedulerTest, SchedulingUpdateAvailableResourcesTest) {
         resource_scheduler.GetClusterResourceManager().GetNodeResources(node_id, &nr2));
 
     for (auto &resource_id : nr1.total.ExplicitResourceIds()) {
-      auto t = nr1.available.Get(resource_id) - resource_request.Get(resource_id);
+      auto t = nr1.available.Sum(resource_id) - resource_request.Get(resource_id);
       if (t < 0) t = 0;
-      ASSERT_EQ(nr2.available.Get(resource_id), t);
+      ASSERT_EQ(nr2.available.Sum(resource_id), t);
     }
   }
 }
@@ -1264,7 +1264,7 @@ TEST_F(ClusterResourceSchedulerTest,
       NodeResources nr;
       resource_scheduler.GetClusterResourceManager().GetNodeResources(
           scheduling::NodeID(0), &nr);
-      ASSERT_TRUE(nr.available.Get(ResourceID::GPU()) == 1.5);
+      ASSERT_TRUE(nr.available.Sum(ResourceID::GPU()) == 1.5);
     }
 
     {
@@ -1286,7 +1286,7 @@ TEST_F(ClusterResourceSchedulerTest,
       NodeResources nr;
       resource_scheduler.GetClusterResourceManager().GetNodeResources(
           scheduling::NodeID(0), &nr);
-      ASSERT_TRUE(nr.available.Get(ResourceID::GPU()) == 3.8);
+      ASSERT_TRUE(nr.available.Sum(ResourceID::GPU()) == 3.8);
     }
   }
 }
diff --git a/src/ray/raylet/scheduling/tests/local_resource_manager_test.cc b/src/ray/raylet/scheduling/tests/local_resource_manager_test.cc
index fd010fdce877..9054f76a43d1 100644
--- a/src/ray/raylet/scheduling/tests/local_resource_manager_test.cc
+++ b/src/ray/raylet/scheduling/tests/local_resource_manager_test.cc
@@ -36,7 +36,7 @@ class LocalResourceManagerTest : public ::testing::Test {
       absl::flat_hash_map<ResourceID, double> resource_usage_map) {
     NodeResources resources;
     for (auto &[resource_id, total] : resource_usage_map) {
-      resources.available.Set(resource_id, total);
+      resources.available.Set(resource_id, {FixedPoint(total)});
       resources.total.Set(resource_id, total);
     }
     return resources;
@@ -407,7 +407,9 @@ TEST_F(LocalResourceManagerTest, CreateSyncMessageNegativeResourceAvailability)
       ResourceID::CPU(), {2.0}, /*allow_going_negative=*/true);
 
   const auto &resource_view_sync_messge = GetSyncMessageForResourceReport();
-  ASSERT_EQ(resource_view_sync_messge.resources_available().at("CPU"), 0);
+  // resources_available is no longer sent; check per-instance data.
+  ASSERT_EQ(resource_view_sync_messge.resources_available_instances().at("CPU").values(0),
+            0);
 }
 
 TEST_F(LocalResourceManagerTest, PopulateResourceViewSyncMessage) {
diff --git a/src/ray/raylet/tests/node_manager_test.cc b/src/ray/raylet/tests/node_manager_test.cc
index 884e710ead16..af5ada24d3c6 100644
--- a/src/ray/raylet/tests/node_manager_test.cc
+++ b/src/ray/raylet/tests/node_manager_test.cc
@@ -709,7 +709,9 @@ TEST_F(NodeManagerTest, TestConsumeSyncMessage) {
   // Create and wrap a mock resource view sync message.
   syncer::ResourceViewSyncMessage payload;
   payload.mutable_resources_total()->insert({"CPU", kTestTotalCpuResource});
-  payload.mutable_resources_available()->insert({"CPU", kTestTotalCpuResource});
+  rpc::syncer::ResourceInstances cpu_instances;
+  cpu_instances.add_values(kTestTotalCpuResource);
+  (*payload.mutable_resources_available_instances())["CPU"] = cpu_instances;
   payload.mutable_labels()->insert({"label1", "value1"});
 
   std::string serialized;
@@ -730,7 +732,7 @@ TEST_F(NodeManagerTest, TestConsumeSyncMessage) {
   EXPECT_EQ(node_resources.labels.at("label1"), "value1");
   EXPECT_EQ(node_resources.total.Get(scheduling::ResourceID("CPU")).Double(),
             kTestTotalCpuResource);
-  EXPECT_EQ(node_resources.available.Get(scheduling::ResourceID("CPU")).Double(),
+  EXPECT_EQ(node_resources.available.Sum(scheduling::ResourceID("CPU")).Double(),
             kTestTotalCpuResource);
 }
 
diff --git a/src/ray/raylet/tests/placement_group_resource_manager_test.cc b/src/ray/raylet/tests/placement_group_resource_manager_test.cc
index 01b7f397bcda..9f7d18f7480f 100644
--- a/src/ray/raylet/tests/placement_group_resource_manager_test.cc
+++ b/src/ray/raylet/tests/placement_group_resource_manager_test.cc
@@ -110,7 +110,9 @@ class NewPlacementGroupResourceManagerTest : public ::testing::Test {
     auto local_node_resource =
         cluster_resource_scheduler_->GetClusterResourceManager().GetNodeResources(
             scheduling::NodeID("local"));
-    ASSERT_TRUE(local_node_resource == node_resources);
+    ASSERT_TRUE(local_node_resource.total == node_resources.total);
+    ASSERT_TRUE(local_node_resource.available.ToNodeResourceSet() ==
+                node_resources.available.ToNodeResourceSet());
   }
 
   // TODO(@clay4444): Remove this once we did the batch rpc request refactor!

# Ray C++ Architecture Guide

This document provides detailed architecture information about Ray's C++ components, including state machines, component relationships, and design patterns. For a quick reference of key files, see [CLAUDE.md](CLAUDE.md).

## Table of Contents
- [Task Lifecycle](#task-lifecycle)
- [Actor Lifecycle](#actor-lifecycle)
- [Object Lifecycle](#object-lifecycle)
- [Resource Management & Scheduling](#resource-management--scheduling)
- [Placement Groups](#placement-groups)
- [Global Control Service (GCS)](#global-control-service-gcs)
- [Fault Tolerance](#fault-tolerance)

---

## Task Lifecycle

### Overview

Tasks in Ray go through multiple components: the submitting CoreWorker, the Raylet scheduler, and the executing worker. The `TaskManager` on the owner side tracks task state, while the Raylet handles scheduling and worker assignment.

### Task State Machine (Owner Side)

```
                    ┌─────────────────────┐
                    │  PENDING_ARGS_AVAIL │  Task submitted, waiting for dependencies
                    └──────────┬──────────┘
                               │ Dependencies resolved
                               ▼
                    ┌─────────────────────┐
                    │ SUBMITTED_TO_WORKER │  Task sent to worker for execution
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
       ┌──────────┐     ┌──────────┐     ┌──────────┐
       │ FINISHED │     │  FAILED  │     │ RETRYING │
       └──────────┘     └──────────┘     └────┬─────┘
                                              │
                                              └──► Back to PENDING_ARGS_AVAIL
```

### Task Execution Flow

```
User Code                CoreWorker (Owner)              Raylet                    CoreWorker (Executor)
    │                          │                           │                              │
    │ ray.remote(f).remote()   │                           │                              │
    │─────────────────────────►│                           │                              │
    │                          │                           │                              │
    │                          │ SubmitTask()              │                              │
    │                          │──────────────────────────►│                              │
    │                          │                           │                              │
    │                          │                           │ Schedule task                │
    │                          │                           │ (find node, lease worker)    │
    │                          │                           │                              │
    │                          │ WorkerLease granted       │                              │
    │                          │◄──────────────────────────│                              │
    │                          │                           │                              │
    │                          │ PushTask RPC              │                              │
    │                          │─────────────────────────────────────────────────────────►│
    │                          │                           │                              │
    │                          │                           │                              │ Execute task
    │                          │                           │                              │
    │                          │ PushTaskReply (result)    │                              │
    │                          │◄─────────────────────────────────────────────────────────│
    │                          │                           │                              │
    │ ray.get() returns        │                           │                              │
    │◄─────────────────────────│                           │                              │
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| TaskSpecification | `common/task/task_spec.h` | Task metadata, dependencies, resources |
| TaskManager | `core_worker/task_manager.h` | Owner-side task state, retries, lineage |
| NormalTaskSubmitter | `core_worker/task_submission/normal_task_submitter.h` | Worker leasing, task queuing |
| DependencyResolver | `core_worker/task_submission/dependency_resolver.h` | Resolve task arguments |
| TaskReceiver | `core_worker/task_execution/task_receiver.h` | Worker-side task handling |
| ClusterLeaseManager | `raylet/scheduling/cluster_lease_manager.h` | Cluster-wide worker leasing |

### Dependency Resolution

Before a task can execute, all its arguments must be available:

1. **Inline arguments**: Small values passed directly in the task spec
2. **ObjectRef arguments**: References to objects that must be fetched
3. **Actor dependencies**: For actor tasks, previous task must complete

The `LocalDependencyResolver` waits for all ObjectRefs to be available in the local object store before marking the task ready for submission.

---

## Actor Lifecycle

### Overview

Actors are long-running stateful workers. The GCS manages cluster-wide actor state, while each CoreWorker tracks local actor handles.

### Actor State Machine

```
                        ┌────────────────────────┐
                        │  DEPENDENCIES_UNREADY  │  Waiting for actor creation args
                        └───────────┬────────────┘
                                    │ Args available
                                    ▼
                        ┌────────────────────────┐
                        │   PENDING_CREATION     │  Waiting for worker assignment
                        └───────────┬────────────┘
                                    │ Worker assigned, actor started
                                    ▼
                        ┌────────────────────────┐
            ┌──────────►│        ALIVE           │◄──────────┐
            │           └───────────┬────────────┘           │
            │                       │                        │
            │                       │ Worker/node failure    │
            │                       ▼                        │
            │           ┌────────────────────────┐           │
            │           │      RESTARTING        │───────────┘
            │           └───────────┬────────────┘   (if restarts remaining)
            │                       │
            │                       │ Max restarts exceeded OR
            │                       │ Owner died OR
            │                       │ Explicit kill
            │                       ▼
            │           ┌────────────────────────┐
            └───────────│        DEAD            │
              (detached └────────────────────────┘
               actors
               can restart)
```

### Actor Creation Flow

```
User Code           CoreWorker              GCS                      Raylet              Worker
    │                   │                    │                          │                   │
    │ @ray.remote       │                    │                          │                   │
    │ class Foo         │                    │                          │                   │
    │                   │                    │                          │                   │
    │ Foo.remote()      │                    │                          │                   │
    │──────────────────►│                    │                          │                   │
    │                   │                    │                          │                   │
    │                   │ RegisterActor      │                          │                   │
    │                   │───────────────────►│                          │                   │
    │                   │                    │                          │                   │
    │                   │                    │ Schedule actor           │                   │
    │                   │                    │─────────────────────────►│                   │
    │                   │                    │                          │                   │
    │                   │                    │                          │ Start worker      │
    │                   │                    │                          │──────────────────►│
    │                   │                    │                          │                   │
    │                   │                    │ ActorCreated             │                   │
    │                   │                    │◄─────────────────────────────────────────────│
    │                   │                    │                          │                   │
    │                   │ Actor ready        │                          │                   │
    │                   │◄───────────────────│                          │                   │
    │                   │                    │                          │                   │
    │ actor.method()    │                    │                          │                   │
    │──────────────────►│                    │                          │                   │
    │                   │                    │                          │                   │
    │                   │ Direct RPC (bypasses Raylet)                  │                   │
    │                   │──────────────────────────────────────────────────────────────────►│
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| GcsActorManager | `gcs/gcs_actor_manager.h` | Cluster-wide actor lifecycle |
| GcsActorScheduler | `gcs/gcs_actor_scheduler.h` | Actor placement decisions |
| ActorManager | `core_worker/actor_manager.h` | Per-worker actor tracking |
| ActorCreator | `core_worker/actor_creator.h` | Actor creation requests |
| ActorHandle | `core_worker/actor_handle.h` | Handle serialization |
| ActorTaskSubmitter | `core_worker/task_submission/actor_task_submitter.h` | Actor method calls |

### Actor Task Ordering

Actor tasks have sequence numbers to ensure ordering:
- **Sequential actors**: Tasks execute in strict sequence number order
- **Threaded actors**: Tasks can execute concurrently but still track sequence numbers
- **Async actors**: Use out-of-order scheduling queues

---

## Object Lifecycle

### Overview

Ray objects are immutable values stored in the distributed object store (Plasma). Reference counting determines when objects can be deleted.

### Reference Types

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Reference Types                               │
├─────────────────────┬───────────────────────────────────────────────┤
│ Local Reference     │ Python/Java variable holding ObjectRef        │
├─────────────────────┼───────────────────────────────────────────────┤
│ Submitted Task Arg  │ Task using this object as argument            │
├─────────────────────┼───────────────────────────────────────────────┤
│ Contained Reference │ Object nested inside another object           │
├─────────────────────┼───────────────────────────────────────────────┤
│ Borrower Reference  │ Object passed to another worker               │
├─────────────────────┼───────────────────────────────────────────────┤
│ Lineage Reference   │ For reconstruction of lost objects            │
└─────────────────────┴───────────────────────────────────────────────┘
```

### Object Resolution Flow

```
                    ┌─────────────────┐
                    │   ray.get(ref)  │
                    └────────┬────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │  Check local memory store    │
              │  (for small/inlined objects) │
              └──────────────┬───────────────┘
                             │ Not found
                             ▼
              ┌──────────────────────────────┐
              │  Check local plasma store    │
              └──────────────┬───────────────┘
                             │ Not found
                             ▼
              ┌──────────────────────────────┐
              │  Get object location from    │
              │  owner via RPC               │
              └──────────────┬───────────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
           ▼                 ▼                 ▼
    ┌────────────┐   ┌────────────────┐  ┌────────────────┐
    │Pull from   │   │Restore from    │  │Reconstruct via │
    │remote node │   │spilled storage │  │lineage         │
    └────────────┘   └────────────────┘  └────────────────┘
```

### Memory Management & Spilling

When memory pressure is detected:

1. **Eviction**: Objects not pinned by any worker are evicted from plasma
2. **Spilling**: Primary copies are written to external storage (disk/S3)
3. **Restoration**: Spilled objects are restored on demand

```
┌──────────────────────────────────────────────────────────────────┐
│                    Memory Pressure Flow                           │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│   Memory Monitor detects pressure                                 │
│           │                                                       │
│           ▼                                                       │
│   LocalObjectManager::SpillObjectsOfSize()                        │
│           │                                                       │
│           ▼                                                       │
│   Select objects to spill (LRU, not pinned)                       │
│           │                                                       │
│           ▼                                                       │
│   Write to external storage (filesystem/S3)                       │
│           │                                                       │
│           ▼                                                       │
│   Update object directory with spilled URL                        │
│           │                                                       │
│           ▼                                                       │
│   Release plasma memory                                           │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| ReferenceCounter | `core_worker/reference_counter.h` | Distributed ref counting |
| ObjectRecoveryManager | `core_worker/object_recovery_manager.h` | Lineage reconstruction |
| LocalObjectManager | `raylet/local_object_manager.h` | Spilling, pinning |
| ObjectManager | `object_manager/object_manager.h` | Push/pull coordination |
| PullManager | `object_manager/pull_manager.h` | Remote object fetching |
| PlasmaStore | `object_manager/plasma/object_store.h` | Shared memory storage |
| MemoryMonitor | `common/memory_monitor.h` | OOM detection |

---

## Resource Management & Scheduling

### Overview

Ray uses a hierarchical scheduling system: the local Raylet handles per-node scheduling, while the GCS coordinates cluster-wide decisions for actors and placement groups.

### Scheduling Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                       GCS (Cluster-wide)                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
│  │ Actor Scheduler │  │ PG Scheduler    │  │ Resource Manager│   │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘   │
└───────────┼────────────────────┼────────────────────┼─────────────┘
            │                    │                    │
            ▼                    ▼                    ▼
┌───────────────────────────────────────────────────────────────────┐
│                        Raylet (Per-node)                          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              ClusterResourceScheduler                        │ │
│  │  ┌──────────────────┐  ┌──────────────────┐                 │ │
│  │  │ ClusterResourceMgr│  │ LocalResourceMgr │                 │ │
│  │  │ (cluster view)    │  │ (local resources)│                 │ │
│  │  └──────────────────┘  └──────────────────┘                 │ │
│  └─────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              ClusterLeaseManager                             │ │
│  │  (Worker lease requests and assignment)                      │ │
│  └─────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              Scheduling Policies                             │ │
│  │  ┌────────┐ ┌────────┐ ┌──────────┐ ┌────────────┐         │ │
│  │  │ Hybrid │ │ Spread │ │ Affinity │ │ NodeLabel  │         │ │
│  │  └────────┘ └────────┘ └──────────┘ └────────────┘         │ │
│  └─────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────┘
```

### Scheduling Policies

| Policy | File | Description |
|--------|------|-------------|
| **Hybrid** | `hybrid_scheduling_policy.h` | Default. Balances locality and load spreading. Prefers local node, then spreads based on resource utilization. |
| **Spread** | `spread_scheduling_policy.h` | Round-robin across available nodes. Good for load balancing. |
| **NodeAffinity** | `node_affinity_scheduling_policy.h` | Schedule on specific node (soft or hard constraint). |
| **NodeLabel** | `node_label_scheduling_policy.h` | Filter nodes by label key-value pairs. |
| **BundleAffinity** | `affinity_with_bundle_scheduling_policy.h` | Schedule on node with specific placement group bundle. |

### Lease-Based Execution Model

```
CoreWorker                  Raylet                       Worker Pool
    │                          │                              │
    │ RequestWorkerLease       │                              │
    │─────────────────────────►│                              │
    │                          │                              │
    │                          │ Find suitable node           │
    │                          │ (via scheduling policy)      │
    │                          │                              │
    │                          │ GetOrCreateWorker            │
    │                          │─────────────────────────────►│
    │                          │                              │
    │                          │ Worker ready                 │
    │                          │◄─────────────────────────────│
    │                          │                              │
    │ WorkerLeaseReply         │                              │
    │ (worker address)         │                              │
    │◄─────────────────────────│                              │
    │                          │                              │
    │ PushTask to worker       │                              │
    │──────────────────────────────────────────────────────────►
```

---

## Placement Groups

### Overview

Placement groups reserve resources across nodes with specific placement strategies. They use a 2-phase commit protocol for atomicity.

### Placement Strategies

| Strategy | Description |
|----------|-------------|
| **PACK** | Pack bundles on fewest nodes possible |
| **SPREAD** | Spread bundles across different nodes |
| **STRICT_PACK** | All bundles must be on same node |
| **STRICT_SPREAD** | Each bundle on different node (fails if not enough nodes) |

### Placement Group State Machine

```
              ┌──────────────┐
              │   PENDING    │  Waiting in queue
              └──────┬───────┘
                     │
                     ▼
              ┌──────────────┐
              │  PREPARING   │  2-phase commit: prepare phase
              └──────┬───────┘
                     │
              ┌──────┴──────┐
              │             │
              ▼             ▼
       ┌──────────┐  ┌──────────────┐
       │ PREPARED │  │  RESCHEDULING│──► Back to PENDING
       └────┬─────┘  └──────────────┘
            │
            ▼
       ┌──────────┐
       │  PLACED  │  Resources committed
       └────┬─────┘
            │
            ▼
       ┌──────────┐
       │ REMOVED  │
       └──────────┘
```

### 2-Phase Commit Protocol

```
GCS PG Scheduler              Raylet 1                    Raylet 2
       │                          │                           │
       │ PrepareBundle(bundle1)   │                           │
       │─────────────────────────►│                           │
       │                          │                           │
       │ PrepareBundle(bundle2)   │                           │
       │──────────────────────────────────────────────────────►│
       │                          │                           │
       │ PrepareReply(success)    │                           │
       │◄─────────────────────────│                           │
       │                          │                           │
       │ PrepareReply(success)    │                           │
       │◄──────────────────────────────────────────────────────│
       │                          │                           │
       │ (All prepared - proceed to commit)                   │
       │                          │                           │
       │ CommitBundle(bundle1)    │                           │
       │─────────────────────────►│                           │
       │                          │                           │
       │ CommitBundle(bundle2)    │                           │
       │──────────────────────────────────────────────────────►│
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| GcsPlacementGroupManager | `gcs/gcs_placement_group_manager.h` | PG lifecycle management |
| GcsPlacementGroupScheduler | `gcs/gcs_placement_group_scheduler.h` | Bundle scheduling, 2PC |
| PlacementGroupResourceManager | `raylet/placement_group_resource_manager.h` | Local PG resources |
| BundleSchedulingPolicy | `raylet/scheduling/policy/bundle_scheduling_policy.h` | Bundle placement strategies |

---

## Global Control Service (GCS)

### Overview

The GCS is the centralized control plane for Ray clusters. It manages metadata, coordinates actors and placement groups, and provides fault tolerance.

### GCS Component Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           GCS Server                                 │
│                                                                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │  NodeManager    │  │  ActorManager   │  │  JobManager     │     │
│  │  - Registration │  │  - Lifecycle    │  │  - Job tracking │     │
│  │  - Health check │  │  - Scheduling   │  │  - Cleanup      │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘     │
│                                                                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │ ResourceManager │  │  PGManager      │  │  WorkerManager  │     │
│  │  - Cluster view │  │  - PG lifecycle │  │  - Worker track │     │
│  │  - Autoscaler   │  │  - 2PC commit   │  │  - Failure      │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘     │
│                                                                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │   KVManager     │  │  TaskManager    │  │HealthCheckMgr  │     │
│  │  - Metadata     │  │  - Task events  │  │  - Node health  │     │
│  │  - Internal KV  │  │  - Observability│  │  - Failure det  │     │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘     │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                     Storage Backend                          │   │
│  │  ┌─────────────────┐              ┌─────────────────┐       │   │
│  │  │  Redis Client   │      OR      │  In-Memory Store│       │   │
│  │  └─────────────────┘              └─────────────────┘       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | File | Responsibility |
|-----------|------|----------------|
| GcsServer | `gcs/gcs_server.h` | Main server, orchestrates all managers |
| GcsNodeManager | `gcs/gcs_node_manager.h` | Node registration, cluster membership |
| GcsActorManager | `gcs/gcs_actor_manager.h` | Actor lifecycle across cluster |
| GcsPlacementGroupManager | `gcs/gcs_placement_group_manager.h` | Placement group management |
| GcsResourceManager | `gcs/gcs_resource_manager.h` | Cluster resource aggregation |
| GcsJobManager | `gcs/gcs_job_manager.h` | Job lifecycle, cleanup |
| GcsHealthCheckManager | `gcs/gcs_health_check_manager.h` | Node health monitoring |
| GcsKVManager | `gcs/gcs_kv_manager.h` | Key-value metadata storage |
| RedisStoreClient | `gcs/store_client/redis_store_client.h` | Redis persistence |

---

## Fault Tolerance

### Node Failure Handling

```
┌──────────────────────────────────────────────────────────────────┐
│                    Node Failure Detection                         │
│                                                                   │
│   GcsHealthCheckManager                                           │
│           │                                                       │
│           │ Heartbeat timeout                                     │
│           ▼                                                       │
│   Mark node as DEAD                                               │
│           │                                                       │
│           ├──► GcsActorManager: Restart affected actors           │
│           │                                                       │
│           ├──► GcsPlacementGroupManager: Reschedule PG bundles    │
│           │                                                       │
│           ├──► GcsResourceManager: Remove node resources          │
│           │                                                       │
│           └──► Notify all Raylets to update cluster view          │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### Actor Reconstruction

When an actor dies unexpectedly:
1. GCS detects death (via worker failure report or health check)
2. If restarts remaining: state → RESTARTING → PENDING_CREATION
3. GCS schedules actor on new node
4. Callers with pending tasks get notified of new actor address
5. If max restarts exceeded: state → DEAD, notify all callers

### Object Reconstruction (Lineage)

When an object is lost and cannot be fetched:
1. `ObjectRecoveryManager` checks if object is reconstructable
2. If lineage available: re-execute the task that created the object
3. Recursively reconstruct any missing dependencies
4. Store new result in plasma

```
Lost Object                         Task Spec (from lineage)
    │                                        │
    │                                        │
    └──────────► ObjectRecoveryManager ◄─────┘
                        │
                        │ Re-submit task
                        ▼
                   TaskManager
                        │
                        │ Execute
                        ▼
                 New Object Created
```

---

## Appendix: Key Data Structures

### TaskSpecification (`common/task/task_spec.h`)
- Task ID, Job ID, Parent Task ID
- Function descriptor (module, class, function name)
- Arguments (inline or ObjectRef)
- Resource requirements
- Scheduling strategy
- Actor ID (for actor tasks)

### ActorTableData (`protobuf/gcs.proto`)
- Actor ID, Job ID
- Owner address
- State (PENDING, ALIVE, DEAD, etc.)
- Resource requirements
- Restart count, max restarts
- Death cause (if dead)

### ObjectTableData
- Object ID
- Owner address
- Size
- Locations (node IDs where object exists)
- Spilled URL (if spilled to external storage)

### NodeResources (`raylet/scheduling/cluster_resource_manager.h`)
- Total resources (CPU, GPU, memory, custom)
- Available resources
- Load metrics
- Labels