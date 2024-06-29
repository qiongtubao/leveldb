// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_WRITER_H_
/**
 * 预写日志为Log
 * 当向LevelDb写入数据时候，只需要将数据写入内存中的MemTable 而内存是易失性存储 
 * 因此LevelDb需要一个额外的持久化文件：预写日志Write-Ahead Log(重做日志)
 * 这是一个追加修改，顺序写入磁盘的文件
 * 当宕机或者程序崩溃时 WAL 能够保证写入成功的数据不会丢失
 * 将MemTable 成功写入SSTable 后，相应的预写日志就可以删除了
 * 
 */
#include <cstdint>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class WritableFile;

namespace log {

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(WritableFile* dest);

  // Create a writer that will append data to "*dest".
  // "*dest" must have initial length "dest_length".
  // "*dest" must remain live while this Writer is in use.
  Writer(WritableFile* dest, uint64_t dest_length);

  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  ~Writer();

  Status AddRecord(const Slice& slice);

 private:
  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  WritableFile* dest_;
  int block_offset_;  // Current offset in block

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  // 所有受支持的记录类型的 crc32c 值。这些值是
  // 预先计算的，以减少计算存储在标头中的
  // 记录类型的 crc 的开销。
  uint32_t type_crc_[kMaxRecordType + 1];
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_WRITER_H_
