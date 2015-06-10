#ifndef BASE_CHUNK_H_
#define BASE_CHUNK_H_

#include <utility>

#include <kj/debug.h>

#include "macros.h"

// The Chunk utility class implements memory allocation on top of a fixed
// memory chunk. It is possible to combine compile-time and run-time memory
// allocation with it. This is akin to C flexible array member syntax.
//
// In C++ flexible arrays are not standardized, although both g++ and clang++
// accept them as an extension. Using the Chunk class frees from the dependency
// on this extension and also it is a bit more flexible than flexible arrays,
// albeit more verbose.
//
// A usage example:
//
// struct A {
//   int value = 0xa;
// };
// struct B {
//   int value = 0xb;
// };
//
//  char buffer[1024];
//  memory::Chunk<A, B> chunk(buffer, sizeof buffer);
//
//  A *a = chunk.Get<A>();
//  B *b = chunk.Get<B>();
//  void *p = chunk.Allocate(100);
//  ...
//

namespace ev {
namespace memory {

namespace detail {

// Calculate a data item alignment according to its size.
constexpr std::size_t Align(std::size_t size, std::size_t offset) {
  return size < 0x08 ? ev::AlignUp(offset, 0x04)
                     : size < 0x10 ? ev::AlignUp(offset, 0x08)
                                   : ev::AlignUp(offset, 0x10);
}

// Services for placement of a given type instance within a memory chunk at
// the specified offset.
template <typename T, std::size_t S>
class EntryLayout {
 public:
  using Type = T;
  using Pointer = T*;

  static constexpr std::size_t Size = sizeof(Type);
  static constexpr std::size_t Offset = Align(Size, S);
  static constexpr std::size_t EndOffset = Offset + Size;

  static Pointer Instance(char* ptr) noexcept {
    return reinterpret_cast<Pointer>(RawData(ptr));
  }

  template <typename... Args>
  static void Construct(std::size_t cutoff, char* ptr, Args&&... args) {
    if (Offset >= cutoff) new (RawData(ptr)) Type(std::forward<Args>(args)...);
  }

  static void Destruct(std::size_t cutoff, char* ptr) noexcept {
    if (Offset >= cutoff) Instance(ptr)->~Type();
  }

 private:
  static char* RawData(char* ptr) noexcept { return ptr + Offset; }
};

// Services for placement of a given list of types within a memory chunk
// at the specified offset.
template <std::size_t S, typename... Tail>
class ChunkLayout {
 public:
  static constexpr bool LayoutEnd = true;
  static constexpr std::size_t StartOffset = S;
  static constexpr std::size_t EndOffset = S;

  template <typename... Args>
  static void Construct(std::size_t, char*, Args...) {}

  static void Destruct(std::size_t, char*) noexcept {}
};

// Recursive template specialization of the above.
template <std::size_t S, typename Head, typename... Tail>
class ChunkLayout<S, Head, Tail...>
    : public ChunkLayout<EntryLayout<Head, S>::EndOffset, Tail...> {
 public:
  using EntryType = Head;
  using HeadLayout = EntryLayout<Head, S>;
  using TailLayout = ChunkLayout<HeadLayout::EndOffset, Tail...>;

  static constexpr bool LayoutEnd = false;
  static constexpr std::size_t StartOffset = S;
  static constexpr std::size_t EndOffset = TailLayout::EndOffset;

  static typename HeadLayout::Pointer Instance(char* ptr) {
    return HeadLayout::Instance(ptr);
  }

  template <typename... Args>
  static void Construct(std::size_t cutoff, char* ptr, Args... args) {
    HeadLayout::Construct(cutoff, ptr, args...);
    TailLayout::Construct(cutoff, ptr, args...);
  }

  static void Destruct(std::size_t cutoff, char* ptr) noexcept {
    TailLayout::Destruct(cutoff, ptr);
    HeadLayout::Destruct(cutoff, ptr);
  }
};

template <typename T, typename U>
class ChunkLayoutCutoff {
 public:
  static constexpr std::size_t Value = ChunkLayoutCutoff::Helper(
      reinterpret_cast<T*>(0), reinterpret_cast<U*>(0));

 private:
  template <
      typename X = T, typename Y = U,
      typename std::enable_if<X::LayoutEnd || Y::LayoutEnd>::type* = nullptr>
  static constexpr std::size_t Helper(X*, Y*) {
    return kj::min(X::StartOffset, Y::StartOffset);
  }

  template <typename X = T, typename Y = U,
            typename std::enable_if<
                !std::is_same<typename X::HeadLayout,
                              typename Y::HeadLayout>::value>::type* = nullptr>
  static constexpr std::size_t Helper(X*, Y*) {
    return kj::min(X::StartOffset, Y::StartOffset);
  }

  template <typename X = T, typename Y = U,
            typename std::enable_if<std::is_same<
                typename X::HeadLayout, typename Y::HeadLayout>::value>::type* =
                nullptr>
  static constexpr std::size_t Helper(X*, Y*) {
    return Helper(reinterpret_cast<typename X::TailLayout*>(0),
                  reinterpret_cast<typename Y::TailLayout*>(0));
  }
};

}  // namespace detail

// Control of memory chunk free and used space.
class ChunkSpace {
 public:
  ChunkSpace(std::size_t size) noexcept : free_{size}, used_(0) {}

  std::size_t Used() const { return used_; }
  std::size_t Free() const { return free_; }
  std::size_t Size() const { return free_ + used_; }

  bool Alloc(std::size_t size) {
    if (size > free_) return false;
    free_ -= size;
    used_ += size;
    return true;
  }

  void Reset(std::size_t size) {
    KJ_REQUIRE(size <= Size());
    free_ = free_ + used_ - size;
    used_ = size;
  }

 private:
  std::size_t free_;
  std::size_t used_;
};

// Memory chunk with a list of types that get automatically allocated on it.
template <typename... EntryType>
class ChunkBase : private detail::ChunkLayout<0, ChunkSpace, EntryType...> {
  using Layout = detail::ChunkLayout<0, ChunkSpace, EntryType...>;

 public:
  static constexpr std::size_t StartSize = Layout::EndOffset;

  ChunkBase(char* data) noexcept : data_{data} {}

  char* Data() { return data_; }

  std::size_t Used() const { return Get<ChunkSpace>()->Used(); }
  std::size_t Free() const { return Get<ChunkSpace>()->Free(); }
  std::size_t Size() const { return Get<ChunkSpace>()->Size(); }

  template <typename T>
  T* Get() {
    return decltype(Upcast<T>(this))::Instance(data_);
  }

  template <typename T>
  const T* Get() const {
    return decltype(Upcast<T>(this))::Instance(data_);
  }

  void* Allocate(std::size_t size) {
    std::size_t offset = Used();
    std::size_t aligned_offset = detail::Align(size, offset);
    std::size_t offset_padding = aligned_offset - offset;
    if (!Alloc(size + offset_padding)) return nullptr;
    return data_ + aligned_offset;
  }

  void Reset(std::size_t size = StartSize) {
    KJ_REQUIRE(size >= Layout::HeadLayout::EndOffset);
    Get<ChunkSpace>()->Reset(size);
  }

  static void swap(ChunkBase &chunk1, ChunkBase &chunk2) {
    std::swap(chunk1.data_, chunk2.data_);
  }

 protected:
  void Construct(std::size_t size) {
    KJ_REQUIRE(size > StartSize);

    // Construct ChunkSpace instance to bootstrap allocation capability.
    Layout::HeadLayout::Construct(0, data_, size);
    // Allocate space required for all the chunk data.
    Alloc(StartSize);
    // Construct the rest of the chunk data.
    Layout::TailLayout::Construct(Layout::TailLayout::StartOffset, data_);
  }

  void Destruct() noexcept { Layout::Destruct(0, data_); }

  template <typename OtherLayout>
  void Restruct() {
    std::size_t cutoff = detail::ChunkLayoutCutoff<Layout, OtherLayout>();
    if (cutoff < Used()) {
      Layout::TailLayout::Destruct(cutoff, data_);
      Reset(cutoff);
    } else if (cutoff > Used()) {
      Reset(cutoff);
      Layout::TailLayout::Construct(cutoff, data_);
    }
  }

 private:
  bool Alloc(std::size_t size) { return Get<ChunkSpace>()->Alloc(size); }

  // Some C++ magic to upcast to the base class that contains layout info
  // for a given entry type.
  template <typename Head, std::size_t S, typename... Tail>
  static typename detail::ChunkLayout<S, Head, Tail...>::HeadLayout Upcast(
      const detail::ChunkLayout<S, Head, Tail...>*);

  char* data_;
};

// Memory chunk with a list of types that get automatically allocated on it.
template <typename... EntryType>
class Chunk : public ChunkBase<EntryType...> {
  using BaseType = ChunkBase<EntryType...>;

 public:
  Chunk() noexcept : BaseType(nullptr) {}

  Chunk(char* data, std::size_t size) : BaseType(data) {
    BaseType::Construct(size);
  }

  Chunk(Chunk&& other) noexcept : Chunk(nullptr) { swap(*this, other); }

  Chunk& operator=(Chunk&& other) {
    swap(*this, other);
    return *this;
  }

  ~Chunk() {
    if (BaseType::Data()) BaseType::Destruct();
  }
};

}  // namespace memory
}  // namespace ev

#endif  // BASE_CHUNK_H_
