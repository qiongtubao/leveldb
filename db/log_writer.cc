// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;
/**
 * 案例1
 *  记录大小500字节，原来已经有1000字节
 *  总共1000 + 7 + 500
 * 案例2
 *   记录大小31755字节，原来已经有1000字节
 *  1000 + 7 + 31755 = 32762  还剩下6个字节  设置\x00 * 6
 * 案例3
 *   记录50000字节 原来已经有1000字节
*    1000 + 7 + 31761 = 32768
 *  7 + （50000 - 31761 = 18239） = 18246 
 */
//方法将记录写入一个Slice结构
Status Writer::AddRecord(const Slice& slice) {
  //ptr指向需要写入的记录内容
  const char* ptr = slice.data();
  //left代表需要写入的记录内容长度
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  // 如果需要，对记录进行分段并发出。请注意，如果切片
  // 为空，我们仍然希望迭代一次以发出单个
  // 零长度记录
  Status s;
  bool begin = true;
  do {
    //一个块的大小（32768字节），block_offset_ 代表当前块的写入偏移量
    //因此leftover表明当前块还剩余多少字节可用
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    //剩余内容少于7 （header）剩余内容填充\x00
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);
    //本块block剩余可用
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    //本块写入的长度  （数据长度和可用值 取最小值）
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    //是否能在本块写完
    const bool end = (left == fragment_length);
    if (begin && end) {
      //开始和结束都在同一块  类型就是kFullType
      type = kFullType;
    } else if (begin) {
      // 如果本次开始的话 类型就是kFirstType
      type = kFirstType;
    } else if (end) {
      // 如果本次结束的话 类型就是kLastType
      type = kLastType;
    } else {
      // 其他 类型就是kMiddleType
      type = kMiddleType;
    }
    //写入数据
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    //修改数据指针
    ptr += fragment_length;
    //修改剩余长度
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  // 格式化开头 7个字节
  char buf[kHeaderSize];
  //4,5 保存长度
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  //6 保存类型
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  // 计算记录类型和有效载荷的 crc。
  // 通过计算crc 
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  //buf 前4位存放crc
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // 文件写入 header
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    //写入数据
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      //刷到磁盘
      s = dest_->Flush();
    }
  }
  //修改offset
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
