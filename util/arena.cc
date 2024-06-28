// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

static const int kBlockSize = 4096;
//构造函数
Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}
//析构函数
Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

char* Arena::AllocateFallback(size_t bytes) {
  //如果大小>1k 就直接申请新内存
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    // 对象大于我们块大小的四分之一。单独分配它
    // 以避免在剩余字节中浪费太多空间。
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  // 我们浪费了当前块中剩余的空间。
  // 直接放弃现在剩余内存，重新申请新的4k内存
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  //调整最后指针位置
  alloc_ptr_ += bytes;
  //重新计算剩余内存
  alloc_bytes_remaining_ -= bytes;
  return result;
}

/**
 * reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
 * 
 * 这段表达式是用来计算当前分配指针alloc_ptr_距离下一个对齐边界有多远，即需要多少字节的填充（padding）才能使指针对齐到指定的对齐粒度align。下面详细解释这个计算过程：

  类型转换 (reinterpret_cast<uintptr_t>(alloc_ptr_)): 首先，将alloc_ptr_（一个指向char的指针）转换为uintptr_t类型。uintptr_t是一个足够大的无符号整数类型，能够表示内存地址。这样做的目的是为了能够对指针地址进行位操作。
  按位与运算 (& (align - 1)):
  align - 1会得到一个值，其二进制形式的低align位都是1，而高阶位都是0。例如，如果align是8（即需要8字节对齐），align - 1就是7，其二进制为00000111。
  接下来的按位与运算(&)会比较两个操作数的每一位：如果两个相应的位都是1，则结果位为1；否则为0。因此，这个操作实际上保留了alloc_ptr_地址转换后值的低阶位中与align对齐要求相对应的部分，丢弃了高位。
  结果就是alloc_ptr_地址相对于当前对齐粒度的“余数”，即当前地址距离下一个对齐边界还差多少个字节（模align的结果）。
  举个例子，假设align=8，alloc_ptr_的地址转换为无符号整数后是0x0000000D（十进制13），那么align - 1 = 7，也就是二进制的00000111。按位与操作后，我们只保留了0000000D的低三位001，得到1，意味着当前位置距离下一个8字节边界还差1字节。

  ---
  使用c语言
  (size_t)(uintptr_t)(void *)alloc_ptr_ & (align - 1);

  通过这个计算，可以确定为了达到指定的对齐要求，需要在当前指针位置之后填充多少字节（slop），以确保下一次分配的内存地址是对齐的。
 */
char* Arena::AllocateAligned(size_t bytes) {
  //首先，通过判断sizeof(void*)与8的大小关系，决定内存对齐的粒度align。
  // 这里确保对齐至少为8字节，通常用于保证数据结构对齐以优化访问速度，尤其是在64位系统中。
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  // 使用static_assert确保对齐粒度align是一个2的幂。这是内存对齐的基本要求，
  // 确保可以简单通过位运算来计算对齐偏移。
  // 128 = 1000000
  // 127 = 0111111
  // 128 & 127 = 0
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  //假设align 等于 8 ，那么7的二进制为111,则该运算采用位的与操作将返回alloc_ptr的最后3位
  //147 = 10010011
  //7   = 00000111
  //--------------
  //      00000011
  //        10进制3
  // 147 - 3 = 144 = 8 * 18 
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  //计算当前分配指针alloc_ptr_距离下一个对齐边界还有多少字节的差距（slop）。这是通过获取alloc_ptr_的地址的最低几位与对齐粒度减一进行按位与操作得到的。
  // slop = 8 - （3）（147中多了3个） = 5 需要跳过5个字节
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  //加上前面计算的slop值，得到实际需要分配的总字节数needed，以确保新分配的内存地址是对齐的。
  size_t needed = bytes + slop;
  char* result;
  
  if (needed <= alloc_bytes_remaining_) {
    //如果needed小于等于当前剩余可分配字节数alloc_bytes_remaining_，
    //则直接在当前分配指针之后分配内存，并更新alloc_ptr_和alloc_bytes_remaining_。
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    //否则，调用AllocateFallback(bytes)从其他地方（可能是堆或系统调用）分配所需大小的内存。这个函数假设返回的内存已经是正确对齐的。
    result = AllocateFallback(bytes);
  }
  //使用assert确保返回的内存地址确实满足了对齐要求。
  //在调试构建中，如果对齐失败，程序会断言失败并终止，帮助开发者发现和修正问题。
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  //放到内存数组中
  blocks_.push_back(result);
  //统计申请的内存
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb
