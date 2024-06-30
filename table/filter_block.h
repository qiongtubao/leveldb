// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
//SSTable中保存
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;
  std::string keys_;             // Flattened key contents   生成布隆过滤器的键 比如有3个键  level,level1,level2 那么keys_为levellevel1level2
  std::vector<size_t> start_;    // Starting index in keys_ of each key 数组类型，保存keys_每个key的开始索引 比如上面的0,5,11
  std::string result_;           // Filter data computed so far   生成布隆过滤器内容
  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument 生成布隆过滤器的时候会把keys_ 分割
  std::vector<uint32_t> filter_offsets_; //过滤器偏移量 在元数据块中的偏移量
};
//SSTable中读取
class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;   //布隆过滤器中为BloomFilterPolicy
  const char* data_;    // Pointer to filter data (at block-start)  元数据的开始位置
  const char* offset_;  // Pointer to beginning of offset array (at block-end)  偏移量
  size_t num_;          // Number of entries in offset array  过滤器的偏移量个数
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file) 过滤器的基数
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
