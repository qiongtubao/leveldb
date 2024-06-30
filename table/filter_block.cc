// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // blockoffset/2k 总共filter的个数   
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  //keys_当前keys_的长度为添加key的索引
  start_.push_back(keys_.size());
  //追加新key字符串
  keys_.append(k.data(), k.size());
}
//过滤器
Slice FilterBlockBuilder::Finish() {
  //如果start_变量不为空就说明需要生成布隆过滤器
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  //将过滤器偏移量一次追加到result_成员变量，每个偏移量占据固定的4字节空间
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }
  //过滤器内容总大小追加到result_
  PutFixed32(&result_, array_offset);
  //过滤器基数11追加到result_
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  //返回过滤器块
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }
  //最后一个索引
  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  //分隔keys
  for (size_t i = 0; i < num_keys; i++) {
    //获得索引
    const char* base = keys_.data() + start_[i];
    //获得长度
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());
  //创建filter
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  //元数据块的长度
  size_t n = contents.size();
  //元数据块中至少包括1个字节过滤基数 4个字节过滤器内容总大小
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  //最后一个字节为 过滤器基数
  base_lg_ = contents[n - 1];
  //4个字节  过滤器内容总大小
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  //检验元数据大小  要大于 过滤内容大小 + 5个字节
  if (last_word > n - 5) return;
  //元数据块开始位置
  data_ = contents.data();
  //元数据块偏移量开始位置
  offset_ = data_ + last_word;
  //过滤器偏移量的个数 每4个字节为一个偏移量
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
