#ifndef BASE_TOPOLOGY_H_
#define BASE_TOPOLOGY_H_

#include <hwloc.h>

#include <vector>
#include <thread>

namespace ev {

class HardwareTopology {
 public:
  HardwareTopology();
  ~HardwareTopology();

  HardwareTopology(HardwareTopology const&) = delete;
  HardwareTopology& operator=(HardwareTopology const&) = delete;

  unsigned int NumberOfNodes() const { return nodes_.size(); }
  unsigned int NumberOfCores() const { return cores_.size(); }
  unsigned int NumberOfThreads() const { return threads_.size(); }

  size_t ParentNodeOfCore(size_t core) const { return cores_[core].Node(); }

  size_t ParentCoreOfThread(size_t thread) const {
    return threads_[thread].Core();
  }

  size_t ParentNodeOfThread(size_t thread) const {
    return ParentNodeOfCore(ParentCoreOfThread(thread));
  }

  std::vector<size_t> const& ChildCoresOfNode(size_t node) const {
    return nodes_[node].Cores();
  }

  std::vector<size_t> const& ChildThreadsOfCore(size_t core) const {
    return cores_[core].Threads();
  }

  std::vector<size_t> const& ChildThreadsOfNode(size_t node) const {
    return nodes_[node].Threads();
  }

  void BindThreadToNode(size_t node) const {
    nodes_.at(node).BindThread(topology_);
  }

  void BindThreadToCore(size_t core) const {
    cores_.at(core).BindThread(topology_);
  }

  void BindThreadToThread(size_t thread) const {
    threads_.at(thread).BindThread(topology_);
  }

  void* Alloc(size_t node, size_t size) const;
  void Free(void* ptr, size_t size) const;

 private:
  class CPUSet {
   public:
    CPUSet() noexcept : cpuset_{nullptr} {}

    CPUSet(hwloc_cpuset_t cpuset) noexcept : cpuset_{cpuset} {}

    CPUSet(CPUSet&& other) noexcept : cpuset_{other.cpuset_} {
      other.cpuset_ = 0;
    }

    ~CPUSet() { Destroy(); }

    CPUSet(CPUSet const&) = delete;
    CPUSet& operator=(CPUSet const&) = delete;

    void BindThread(hwloc_topology_t topology) const;

   private:
    void Destroy();

    hwloc_cpuset_t cpuset_;
  };

  class Node : public CPUSet {
   public:
    Node(hwloc_cpuset_t cpuset, std::vector<size_t>&& cores,
         std::vector<size_t>&& threads) noexcept : CPUSet{cpuset},
                                                   cores_(cores),
                                                   threads_(threads) {}

    Node(Node&& other) noexcept : CPUSet(std::move(other)),
                                  cores_(other.cores_),
                                  threads_(other.threads_) {}

    std::vector<size_t> const& Cores() const { return cores_; }
    std::vector<size_t> const& Threads() const { return threads_; }

   private:
    std::vector<size_t> cores_;
    std::vector<size_t> threads_;
  };

  class Core : public CPUSet {
   public:
    Core(size_t node, hwloc_cpuset_t cpuset,
         std::vector<size_t>&& threads) noexcept : CPUSet{cpuset},
                                                   node_(node),
                                                   threads_(threads) {}

    Core(Core&& other) noexcept : CPUSet(std::move(other)),
                                  node_(other.node_),
                                  threads_(other.threads_) {}

    size_t Node() const { return node_; }

    std::vector<size_t> const& Threads() const { return threads_; }

   private:
    size_t node_;
    std::vector<size_t> threads_;
  };

  class Thread : public CPUSet {
   public:
    Thread(size_t core, hwloc_cpuset_t cpuset) noexcept : CPUSet{cpuset},
                                                          core_{core} {}
    Thread(Thread&& other) noexcept : CPUSet(std::move(other)),
                                      core_(other.core_) {}

    size_t Core() const { return core_; }

   private:
    size_t core_;
  };

  hwloc_topology_t topology_;
  std::vector<Node> nodes_;
  std::vector<Core> cores_;
  std::vector<Thread> threads_;
};

}  // namespace ev

#endif  // !BASE_TOPOLOGY_H_
