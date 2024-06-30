// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  //第一个重启点在偏移量0处
  restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

/**
 * 写入所有重启点
 * 写入重启点个数
 */
Slice BlockBuilder::Finish() {
  // Append restart array
  // 将重启点偏移量写入buffer_
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  //将重启点的个数追加到buffer_中
  PutFixed32(&buffer_, restarts_.size());
  //将finished_
  finished_ = true;
  //返回生成的Block
  return Slice(buffer_);
}
/**
 *  Block
 *  {共享长度} {非共享长度} {值的长度} {非共享数据} {值}
 */
void BlockBuilder::Add(const Slice& key, const Slice& value) {
  //last_key_保存上一个加入的键
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  //shared用来保存本次加入的键和上一个加入的键的共同前缀长度
  size_t shared = 0;
  //查看当前已经插入的键值对数据是否已经超过16
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    //和上一个key 的长度和当前的长度 取最小值
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    //计算共同前缀的长度
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    //如果键值对数量已经超过16 则开启新的重启点
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  //non_shared为键的长度减去共同前缀的长度，即非共享部分的键长度
  const size_t non_shared = key.size() - shared;
  // 将共同前缀长度，非共享部分长度，值长度分别写入buffer_
  // Add "<shared><non_shared><value_size>" to buffer_
  //可变32bit
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  // 将键的非共享部分数据追加到buffer_中
  buffer_.append(key.data() + shared, non_shared);
  // 将值数据追加到buffer_中
  buffer_.append(value.data(), value.size());

  // Update state
  // 更新last_key_为当前写入的键
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  //将当前buffer_中写入键值对的数量加1
  counter_++;
}

}  // namespace leveldb
