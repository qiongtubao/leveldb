// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  //表示一些配置选项
  const Options* options_;          
  //块的内容 所有的建值对都保存在buffer_
  std::string buffer_;              // Destination buffer 
  //每次开启新的重启点后，会将当前buffer_的数据长度保存到restarts_中，当前buffer_ 中的数据长度即为每个重启点的偏移量
  std::vector<uint32_t> restarts_;  // Restart points  
  //开启新的重启点之后加入的建值对 默认保存16个建值对
  int counter_;                     // Number of entries emitted since restart 
  //指明是否已经调用了Finish方法,BlockBuilder中的Add方法会将数据保存到各个成员变量中，
  //而Finish方法会依据成员变量的值生成一个块
  bool finished_;                   // Has Finish() been called?
  //上一个保存的键 当加入新键时，用来计算和上一个键的共同前缀部分
  std::string last_key_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
