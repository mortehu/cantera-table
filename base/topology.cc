#include "topology.h"

#include <algorithm>
#include <system_error>

namespace ev {

HardwareTopology::HardwareTopology() {
  if (hwloc_topology_init(&topology_) < 0)
    throw std::runtime_error("hwloc_topology_init()");
  if (hwloc_topology_load(topology_) < 0)
    throw std::runtime_error("hwloc_topology_load()");

  int node_depth = hwloc_get_type_depth(topology_, HWLOC_OBJ_NODE);
  if (node_depth < 0)
    throw std::runtime_error("hwloc_get_type_depth(..., HWLOC_OBJ_NODE)");
  int core_depth = hwloc_get_type_depth(topology_, HWLOC_OBJ_CORE);
  if (core_depth < 0)
    throw std::runtime_error("hwloc_get_type_depth(..., HWLOC_OBJ_CORE)");
  int pu_depth = hwloc_get_type_depth(topology_, HWLOC_OBJ_PU);
  if (pu_depth < 0)
    throw std::runtime_error("hwloc_get_type_depth(..., HWLOC_OBJ_PU)");

  unsigned node_num = hwloc_get_nbobjs_by_depth(topology_, node_depth);
  for (unsigned node_idx = 0; node_idx < node_num; node_idx++) {
    hwloc_obj_t node_obj =
        hwloc_get_obj_by_depth(topology_, node_depth, node_idx);

    hwloc_cpuset_t node_cpuset = hwloc_bitmap_alloc();
    if (!node_cpuset) throw std::bad_alloc();
    hwloc_bitmap_and(node_cpuset, node_obj->online_cpuset,
                     node_obj->allowed_cpuset);

    std::vector<size_t> child_cores_of_node;
    std::vector<size_t> child_threads_of_node;
    unsigned node_core_num = hwloc_get_nbobjs_inside_cpuset_by_depth(
        topology_, node_cpuset, core_depth);
    unsigned node_pu_num = hwloc_get_nbobjs_inside_cpuset_by_depth(
        topology_, node_cpuset, pu_depth);
    child_cores_of_node.reserve(node_core_num);
    child_threads_of_node.reserve(node_pu_num);
    for (unsigned core_idx = 0; core_idx < node_core_num; core_idx++) {
      hwloc_obj_t core_obj = hwloc_get_obj_inside_cpuset_by_depth(
          topology_, node_cpuset, core_depth, core_idx);

      hwloc_cpuset_t core_cpuset = hwloc_bitmap_alloc();
      if (!core_cpuset) {
        hwloc_bitmap_free(node_cpuset);
        throw std::bad_alloc();
      }
      hwloc_bitmap_and(core_cpuset, core_obj->online_cpuset,
                       core_obj->allowed_cpuset);

      std::vector<size_t> child_threads_of_core;
      unsigned core_pu_num = hwloc_get_nbobjs_inside_cpuset_by_depth(
          topology_, core_cpuset, pu_depth);
      child_threads_of_core.reserve(core_pu_num);
      for (unsigned pu_idx = 0; pu_idx < core_pu_num; pu_idx++) {
        hwloc_obj_t pu_obj = hwloc_get_obj_inside_cpuset_by_depth(
            topology_, core_cpuset, pu_depth, pu_idx);

        hwloc_cpuset_t cpuset = hwloc_bitmap_dup(pu_obj->cpuset);

        child_threads_of_node.push_back(threads_.size());
        child_threads_of_core.push_back(threads_.size());
        threads_.emplace_back(core_idx, cpuset);
      }

      child_cores_of_node.push_back(cores_.size());
      cores_.emplace_back(node_idx, core_cpuset,
                          std::move(child_threads_of_core));
    }

    nodes_.emplace_back(node_cpuset, std::move(child_cores_of_node),
                        std::move(child_threads_of_node));
  }
}

HardwareTopology::~HardwareTopology() { hwloc_topology_destroy(topology_); }

void HardwareTopology::CPUSet::Destroy() {
  if (cpuset_) {
    hwloc_bitmap_free(cpuset_);
    cpuset_ = nullptr;
  }
}

void HardwareTopology::CPUSet::BindThread(hwloc_topology_t topology) const {
  if (hwloc_set_cpubind(topology, cpuset_, HWLOC_CPUBIND_THREAD) == -1)
    throw std::system_error(errno, std::generic_category(),
                            "hwloc_set_cpubind()");
}

void* HardwareTopology::Alloc(size_t node, size_t size) const {
  hwloc_nodeset_t nodeset = hwloc_bitmap_alloc();
  if (!nodeset) throw std::bad_alloc();

  hwloc_bitmap_set(nodeset, node);
  void* ptr = hwloc_alloc_membind_nodeset(topology_, size, nodeset,
                                          HWLOC_MEMBIND_BIND, 0);
  hwloc_bitmap_free(nodeset);
  return ptr;
}

void HardwareTopology::Free(void* ptr, size_t size) const {
  hwloc_free(topology_, ptr, size);
}

}  // namespace ev
