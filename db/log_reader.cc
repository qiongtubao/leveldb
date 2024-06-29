// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

bool Reader::SkipToInitialBlock() {
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}
//数据存入到record 如果多个block的保存scratch
bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  //最后一次记录offset 小于初始化值 
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }
  //清理数据
  scratch->clear();
  record->clear();
  //碎片记录
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  // 记录我们正在读取的逻辑记录的偏移量
  // 0 是一个虚拟值，以使编译器满意
  uint64_t prospective_record_offset = 0;
  //分段
  Slice fragment;
  while (true) {
    //解析log  数据在fragment
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    // ReadPhysicalRecord 的内部缓冲区中可能只剩下一个空的尾部。
    // 现在计算它返回的下一个物理记录的偏移量，并正确计算其标头大小。
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

    if (resyncing_) {
      //如果是同步的话 可以跳过？
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;
        //清理临时
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        //偏移
        prospective_record_offset = physical_record_offset;
        //设置数据
        scratch->assign(fragment.data(), fragment.size());
        //打开碎片记录
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          //追加数据
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          //追加数据
          scratch->append(fragment.data(), fragment.size());
          //合并所有碎片
          *record = Slice(*scratch);
          //保存最后的偏移量
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          // 这可能是由于写入器在写入物理记录之后但在完成下一个记录之前立即死亡所致；不要
          // 将其视为损坏，而应忽略整个逻辑记录。
          scratch->clear();
        }
        //返回失败
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        //记录类型无识别
        char buf[40];
        std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

//删除报告 回滚
void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    if (buffer_.size() < kHeaderSize) {
      //长度小于7
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        // 上次读取是完整读取，因此这是一段可以跳过的预告片
        // 清理缓存
        buffer_.clear();
        //buffer读取32768字节， 顺序读
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        //保存读取到那里offset保存
        end_of_buffer_offset_ += buffer_.size();
        if (!status.ok()) {
          //失败 清理buffer
          buffer_.clear();
          //记录错误
          ReportDrop(kBlockSize, status);
          //设置eof为true
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          //设置eof为true
          eof_ = true;
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        // 请注意，如果 buffer_ 非空，则文件末尾的标头会被截断，
        // 这可能是由于写入程序在写入标头的过程中崩溃造成的。
        // 不要将此视为错误，而只需报告 EOF。
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header
    const char* header = buffer_.data();
    //读取4,5 获得长度
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    //6 获得类型
    const unsigned int type = header[6];
    //合并长度
    const uint32_t length = a | (b << 8);
    if (kHeaderSize + length > buffer_.size()) {
      //7 + 长度 大于buffer的长度
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        //记录错误长度
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      // 如果到达文件末尾而未读取有效负载的 |length| 字节
      // ，则假设写入器在写入记录的过程中死亡。
      // 不报告损坏。
      return kEof;
    }

    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      // 跳过零长度记录而不报告任何丢失，因为
      // 此类记录是由 env_posix.cc 中基于 mmap 的写入代码生成的，
      // 该代码预分配文件区域。
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    // 检查crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        // 删除缓冲区的其余部分，因为“长度”本身可能
        // 已被损坏，如果我们相信它，我们可以找到一些
        // 真实日志记录的片段，这些片段恰好看起来像
        // 有效的日志记录。
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }
    //buffer后移 一个数据段
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    // 跳过在 initial_offset_ 之前开始的物理记录
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
          //清理数据
      result->clear();
      return kBadRecord;
    }
    //读取数据
    *result = Slice(header + kHeaderSize, length);
    //返回类型
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
