// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// This file contains the specification, but not the implementations,
// of the types/operations/etc. that should be defined by a platform
// specific port_<platform>.h file.  Use this file as a reference for
// how to port this package to a new platform.

#ifndef STORAGE_LEVELDB_PORT_PORT_EXAMPLE_H_
#define STORAGE_LEVELDB_PORT_PORT_EXAMPLE_H_

#include "port/thread_annotations.h"

namespace leveldb {
namespace port {

// TODO(jorlow): Many of these belong more in the environment class rather than
//               here. We should try moving them and see if it affects perf.

// ------------------ Threading -------------------

// A Mutex represents an exclusive lock.
// 互斥锁 (Mutex) 代表独占锁。
// 是一种实现线程同步的主要方法，如果一个程序中有多个线程，
//    任何时候均只有一个线程可以拥有这个互斥量，
//    并且只有在拥有互斥量时才能对共享的数据资源进行访问
//线程对数据的访问权，以实现多线程环境下的线程安全
class LOCKABLE Mutex {
 public:
  Mutex();
  ~Mutex();

  // Lock the mutex.  Waits until other lockers have exited.
  // Will deadlock if the mutex is already locked by this thread.
  // 锁定互斥锁。等待直到其他锁定器退出。
  // 如果互斥锁已被此线程锁定，则将发生死锁
  void Lock() EXCLUSIVE_LOCK_FUNCTION();

  // Unlock the mutex.
  // REQUIRES: This mutex was locked by this thread.
  // 解锁互斥锁。
  // 要求：此互斥锁已被该线程锁定。
  void Unlock() UNLOCK_FUNCTION();

  // Optionally crash if this thread does not hold this mutex.
  // The implementation must be fast, especially if NDEBUG is
  // defined.  The implementation is allowed to skip all checks.
  // 如果此线程不持有此互斥锁，则可选择崩溃。
  // 实现必须快速，特别是如果定义了 NDEBUG
  //。实现可以跳过所有检查。
  void AssertHeld() ASSERT_EXCLUSIVE_LOCK();
};

//条件变量
//实现线程同步的另外一个方法
//在某些场情况下 要根据某些条件判断是否满足， 从而决定是等待还是继续执行
//常规做法是线程一直处于活动状态，会一直占用CPU的计算资源，导致其他线程不能实现高效的异步操作
//条件变量正式为了解决这个问题而设计的
//在使用条件变量时，活动线程在某个条件成立时 调用Signal或者SignalAll唤醒阻塞的线程

class CondVar {
 public:
  //条件变量通常情况下是和互斥量搭配使用
  //调用Wait的时候自动释放对mu对象的所有权
  explicit CondVar(Mutex* mu);
  ~CondVar();

  // Atomically release *mu and block on this condition variable until
  // either a call to SignalAll(), or a call to Signal() that picks
  // this thread to wakeup.
  // REQUIRES: this thread holds *mu
  // 原子地释放 *mu 并阻塞此条件变量，直到
  // 调用 SignalAll()，或调用 Signal() 来选择
  // 此线程被唤醒。
  // 要求：此线程持有 *mu
  void Wait();

  // If there are some threads waiting, wake up at least one of them.
  // 如果有一些线程在等待，则唤醒其中至少一个。
  void Signal();

  // Wake up all waiting threads.
  // 唤醒所有等待的线程。
  void SignallAll();
};

// ------------------ Compression -------------------

// Store the snappy compression of "input[0,input_length-1]" in *output.
// Returns false if snappy is not supported by this port.
// input待压缩数据
// input_length 数据长度
// 压缩结果存储到output
// 如果无法压缩返回false
bool Snappy_Compress(const char* input, size_t input_length,
                     std::string* output);

// If input[0,input_length-1] looks like a valid snappy compressed
// buffer, store the size of the uncompressed data in *result and
// return true.  Else return false.

// input 待解压的数据
// length 表示未解压数据长度
// 进行解压 后的数据长度 存储到result中
// 如果无法解压则返回false
bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                  size_t* result);

// Attempt to snappy uncompress input[0,input_length-1] into *output.
// Returns true if successful, false if the input is invalid lightweight
// compressed data.
//
// REQUIRES: at least the first "n" bytes of output[] must be writable
// where "n" is the result of a successful call to
// Snappy_GetUncompressedLength.
// input 待解压数据
// input_length 待解压数据长度
// output 解压后数据存储到output
// 如果无法解压返回false
bool Snappy_Uncompress(const char* input_data, size_t input_length,
                       char* output);

// ------------------ Miscellaneous -------------------

// If heap profiling is not supported, returns false.
// Else repeatedly calls (*func)(arg, data, n) and then returns true.
// The concatenation of all "data[0,n-1]" fragments is the heap profile.
// 如果不支持堆分析，则返回 false。
// 否则重复调用 (*func)(arg, data, n)，然后返回 true。
// 所有“data[0,n-1]”片段的连接就是堆分析。
bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg);

// Extend the CRC to include the first n bytes of buf.
//
// Returns zero if the CRC cannot be extended using acceleration, else returns
// the newly extended CRC value (which may also be zero).
// 扩展 CRC 以包含 buf 的前 n 个字节。
//
// 如果无法使用加速扩展 CRC，则返回0，
// 否则返回新扩展的 CRC 值（也可能为零）。
uint32_t AcceleratedCRC32C(uint32_t crc, const char* buf, size_t size);

}  // namespace port
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_PORT_PORT_EXAMPLE_H_
