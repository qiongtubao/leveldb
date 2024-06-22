// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>

#include "leveldb/slice.h"
#include "util/logging.h"
#include "util/no_destructor.h"

namespace leveldb {

Comparator::~Comparator() = default;

namespace {
//leveldb中内置的默认比较器 （字典序进行比较）
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() = default;

  const char* Name() const override { return "leveldb.BytewiseComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    return a.compare(b);
  }

  //传入参数 start 对应原字符串 ，经过一系列逻辑处理后相应的短字符串也将保存在start中以返回
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    // Find length of common prefix
    //2个字符串长度取最小长度
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    //对比前缀字符串相同个数
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }
   

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
      //如果前缀一致  不压缩
    } else {
      //读取不相同的字符
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      //如果字符 <0xff 而且  +1 要小于limit相同位置字符 才进行修改 字符+1 调整长度
      //start（abcd）和   limit（abdc） 不改变
      //start （abcd）和  limit（abec）  ，abcd=> abd
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  //不对比  直接自己压缩
  void FindShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      //找到第一个非0xff的字符  然后进行+1 并截断后面
      //abcd  =>  b
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i + 1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

const Comparator* BytewiseComparator() {
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace leveldb
