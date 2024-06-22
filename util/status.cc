// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/status.h"

#include <cstdio>

#include "port/port.h"

namespace leveldb {

const char* Status::CopyState(const char* state) {
  uint32_t size;
  std::memcpy(&size, state, sizeof(size));
  char* result = new char[size + 5];
  std::memcpy(result, state, size + 5);
  return result;
}

//msg2为空                    {msg长度}{code}{msg}
//如果msg2为不为空的话内容就是   {msg长度+2+msg2长度}{code}{msg}: {msg2}
Status::Status(Code code, const Slice& msg, const Slice& msg2) {
  assert(code != kOk);
  const uint32_t len1 = static_cast<uint32_t>(msg.size());
  const uint32_t len2 = static_cast<uint32_t>(msg2.size());
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
  char* result = new char[size + 5];
  //前4位存放长度 将指针二进制内容直接存放进去
  std::memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  std::memcpy(result + 5, msg.data(), len1);
  if (len2) {
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    std::memcpy(result + 7 + len1, msg2.data(), len2);
  }
  state_ = result;
}

//转换成字符串
//如果正常就是OK
//否则  状态字符串 {code-message}: {message}
std::string Status::ToString() const {
  if (state_ == nullptr) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case kOk:
        type = "OK";
        break;
      case kNotFound:
        type = "NotFound: ";
        break;
      case kCorruption:
        type = "Corruption: ";
        break;
      case kNotSupported:
        type = "Not implemented: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      default:
        std::snprintf(tmp, sizeof(tmp),
                      "Unknown code(%d): ", static_cast<int>(code()));
        type = tmp;
        break;
    }
    std::string result(type);
    uint32_t length;
    //读取message 长度 （前3位） sizeof(uint32_t) 一定是4 
    // uint32_t 是一种无符号整数类型，它是标准C99中定义的固定宽度整数类型之一，明确表示该类型占据32位，即4字节
    //和前的面存入的时候存放2进制相对应  std::memcpy(result, &size, sizeof(size));
    std::memcpy(&length, state_, sizeof(length));
    //字符串追加
    result.append(state_ + 5, length);
    return result;
  }
}

}  // namespace leveldb
