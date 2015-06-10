#ifndef BASE_THREAD_POOL2_H_
#define BASE_THREAD_POOL2_H_

#include <algorithm>
#include <atomic>
#include <thread>

#include "concurrency.h"
#include "macros.h"
#include "topology.h"

namespace ev {
namespace concurrency {

enum class ThreadAffinity {
  kNone,
  kNode,
  kCore,
  kThread,
};

enum class QueueUsage {
  kSingle,
  kPerNode,
  kPerCore,
  kPerThread,
};

template <template <typename> class Queue, typename Task>
class ThreadPool {
 public:
  template <typename... QueueArgs>
  ThreadPool(size_t nthreads, QueueArgs... queue_args)
      : next_queue_{0}, started_threads_{0} {
    queues_.reserve(1);
    queues_.emplace_back(queue_args...);

    threads_.reserve(nthreads);
    for (size_t thread = 0; thread < nthreads; thread++)
      threads_.emplace_back(*this, queues_[0]);
    for (size_t thread = 0; thread < nthreads; thread++)
      threads_[thread].Start();
    WaitStart(nthreads);
  }

  template <typename... QueueArgs>
  ThreadPool(bool use_smt, ThreadAffinity affinity, QueueUsage queue_usage,
             QueueArgs... queue_args)
      : ThreadPool(ev::HardwareTopology{}, use_smt, affinity, queue_usage,
                   queue_args...) {}

  template <typename... QueueArgs>
  ThreadPool(ev::HardwareTopology const& topology, bool use_smt,
             ThreadAffinity affinity, QueueUsage queue_usage,
             QueueArgs... queue_args)
      : next_queue_{0}, started_threads_{0} {
    size_t nqueues = NumberOfQueues(topology, use_smt, queue_usage);
    queues_.reserve(nqueues);
    for (size_t queue = 0; queue < nqueues; queue++)
      queues_.emplace_back(queue_args...);

    size_t nthreads = NumberOfThreads(topology, use_smt);
    threads_.reserve(nthreads);
    for (size_t thread = 0; thread < nthreads; thread++) {
      size_t queue = ThreadToQueue(topology, use_smt, queue_usage, thread);
      threads_.emplace_back(*this, queues_[queue]);
    }

    for (size_t thread = 0; thread < nthreads; thread++)
      threads_[thread].Start(
          MakeThreadBinder(topology, use_smt, affinity, thread));
    WaitStart(nthreads);
  }

  void Post(Task&& task) {
    size_t queue = 0;
    if (queues_.size() > 1)
      queue =
          next_queue_.fetch_add(1, std::memory_order_relaxed) % queues_.size();
    queues_[queue].Enqueue(TaskPackage{std::move(task)});
  }

  void Stop() {
    for (Queue<TaskPackage>& queue : queues_) queue.Finish();
    for (Thread& thread : threads_) thread.Stop();
  }

  void Join() {
    for (Thread& thread : threads_) thread.Join();
  }

  void Stat() const {
    for (Thread const& thread : threads_) thread.Stat();
  }

 private:
  struct TaskPackage {
    /*
    TaskPackage() = default;

    TaskPackage(Task &&task) : task_(std::move(task)) {}

    TaskPackage(TaskPackage &&other) {
      std::swap(task_, other.task_);
    }
    */

    Task task_;
  };

  class ThreadBinder {
   public:
    ThreadBinder(ev::HardwareTopology const& topology, ThreadAffinity affinity,
                 size_t index)
        : topology_(topology), affinity_(affinity), index_(index) {}

    void Bind() {
      try {
        switch (affinity_) {
          case ThreadAffinity::kNone:
            break;
          case ThreadAffinity::kNode:
            topology_.BindThreadToNode(index_);
            std::this_thread::yield();
            break;
          case ThreadAffinity::kCore:
            topology_.BindThreadToCore(index_);
            std::this_thread::yield();
            break;
          case ThreadAffinity::kThread:
            topology_.BindThreadToThread(index_);
            std::this_thread::yield();
            break;
        }
      } catch (std::system_error& error) {
        fprintf(stderr, "%s\n", error.what());
      }
    }

   private:
    ev::HardwareTopology const& topology_;
    ThreadAffinity affinity_;
    size_t index_;
  };

  class Thread {
   public:
    Thread(ThreadPool& pool, Queue<TaskPackage>& task_queue) noexcept
        : task_queue_{task_queue},
          task_count_{0},
          stop_{false},
          pool_{pool} {}

    Thread(Thread&& other) noexcept : task_queue_(other.task_queue_),
                                      task_count_{0},
                                      stop_{false},
                                      pool_{other.pool_} {
      other.stop_ = true;
    }

    ~Thread() { Join(); }

    void Start() { thread_ = std::thread(&Thread::Entry, this); }

    void Start(ThreadBinder binder) {
      thread_ = std::thread(&Thread::BoundEntry, this, binder);
    }

    void Stop() { stop_ = true; }

    void Join() {
      if (thread_.joinable()) thread_.join();
    }

    void Stat() const {
      auto position =
          std::find_if(pool_.threads_.begin(), pool_.threads_.end(),
                       [this](Thread const& other) { return &other == this; });
      fprintf(stderr, "thread #%ld tasks: %llu\n",
              std::distance(pool_.threads_.begin(), position),
              (unsigned long long)task_count_);
    }

   private:
    void Entry() {
      std::unique_lock<std::mutex> lock(pool_.start_mutex_);
      if (++pool_.started_threads_ == pool_.threads_.size())
        pool_.start_cond_.notify_one();
      lock.unlock();

      for (;;) {
        TaskPackage package;
        if (!task_queue_.Dequeue(package)) {
          if (stop_) break;
        } else {
          task_count_++;
          try {
            package.task_();
          } catch (...) {
          }
        }
      }
    }

    void BoundEntry(ThreadBinder binder) {
      binder.Bind();
      Entry();
    }

    Queue<TaskPackage>& task_queue_;
    uint64_t task_count_;

    bool stop_;
    ThreadPool& pool_;

    std::thread thread_;

  } __attribute__((aligned(EV_CACHELINE_SIZE)));

  void WaitStart(size_t nthreads) {
    std::unique_lock<std::mutex> lock(start_mutex_);
    while (started_threads_ < nthreads) start_cond_.wait(lock);
  }

  size_t NumberOfThreads(ev::HardwareTopology const& topology, bool use_smt) {
    if (use_smt)
      return topology.NumberOfThreads();
    else
      return topology.NumberOfCores();
  }

  size_t NumberOfQueues(ev::HardwareTopology const& topology, bool use_smt,
                        QueueUsage queue_usage) {
    if (queue_usage == QueueUsage::kPerThread)
      return use_smt ? topology.NumberOfThreads() : topology.NumberOfCores();
    else if (queue_usage == QueueUsage::kPerCore)
      return topology.NumberOfCores();
    else if (queue_usage == QueueUsage::kPerNode)
      return topology.NumberOfNodes();
    return 1;
  }

  size_t ThreadToQueue(ev::HardwareTopology const& topology, bool use_smt,
                       QueueUsage queue_usage, size_t thread) {
    if (queue_usage == QueueUsage::kPerThread)
      return thread;
    else if (queue_usage == QueueUsage::kPerCore)
      return use_smt ? topology.ParentCoreOfThread(thread) : thread;
    else if (queue_usage == QueueUsage::kPerNode)
      return use_smt ? topology.ParentNodeOfThread(thread)
                     : topology.ParentNodeOfCore(thread);
    return 0;
  }

  ThreadBinder MakeThreadBinder(ev::HardwareTopology const& topology,
                                bool use_smt, ThreadAffinity affinity,
                                size_t thread) {
    if (affinity == ThreadAffinity::kThread)
      return use_smt ? ThreadBinder(topology, affinity, thread)
                     : ThreadBinder(topology, ThreadAffinity::kCore, thread);
    else if (affinity == ThreadAffinity::kCore)
      return use_smt ? ThreadBinder(topology, affinity,
                                    topology.ParentCoreOfThread(thread))
                     : ThreadBinder(topology, affinity, thread);
    else if (affinity == ThreadAffinity::kNode)
      return use_smt ? ThreadBinder(topology, affinity,
                                    topology.ParentNodeOfThread(thread))
                     : ThreadBinder(topology, affinity,
                                    topology.ParentNodeOfCore(thread));
    return ThreadBinder(topology, ThreadAffinity::kNone, 0);
  }

  std::vector<Thread> threads_;
  std::vector<Queue<TaskPackage>> queues_;

  std::atomic<size_t> next_queue_;

  size_t started_threads_;
  std::mutex start_mutex_;
  std::condition_variable start_cond_;
};

}  // namespace concurrency
}  // namespace ev

#endif  // BASE_THREAD_POOL2_H_
