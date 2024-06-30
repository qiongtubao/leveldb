// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
/**
 * 
 *  <data_block> 
 *  <meta_block>
 *  <index_block> 
 *  <footer>
 *    <meta index handle>
 *    <index handle>
 *    <padding>
 *    <magic 8字节>
 */
namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;      //SSTable生成后的文件
  uint64_t offset;
  Status status;
  BlockBuilder data_block;  //生成SSTable中的数据区域
  BlockBuilder index_block; //生成SSTable中的数据索引区域
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block; //生成SSTable中的元数据区域

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;    //判断是否需要生成SSTable中的数据索引,SSTable中每次
                               //生成一个完整的块之后,需要将该值置为true，说明需要为
                               //该块添加索引
  BlockHandle pending_handle;  // Handle to add to index block
                               // pending_handle记录需要生成数据索引的数据块在
                               // SSTable中的偏移量和大小

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  //r赋值为TableBuilder中Rep类型成员
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  //判断是否需要增加数据索引。当完整生成一个块之后,需要写入数据索引
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    //查找该块中的最大键与即将插入键（即下一个块的最小键）之间的最短分隔符
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    //将该块的BlockHandle编码，即将偏移量和大小分别编码为可变长度64位整型
    r->pending_handle.EncodeTo(&handle_encoding);
    //在数据索引中写入键和该块的BlockHandle,BlockHandle结构包括块的偏移以及块大小
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    //将pending_index_entrry置为flase,等待下一次生成一个完整的Block并将该值再次置为true
    r->pending_index_entry = false;
  }
  //在元数据块中增加该key
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }
  //将last_key赋值即将插入的键
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  //在数据块中增加该键值对
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  //判断如果当前数据块的大小大于配置的块大小（默认4KB）则调用Flush函数
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  //写入数据块
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    //将pending_index_entry置为true 表明下一次需要写入数据索引
    r->pending_index_entry = true;
    //将文件刷新到磁盘
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    //布隆过滤器保存
    r->filter_block->StartBlock(r->offset);
  }
}

/**
 *  {block} { restart_offset * n} {type} {crc}
 */
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // 压缩 raw类型 snappy
  // TODO(postrelease): Support more compression options: zlib?
  // 未来支持zlib？
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    //生成crc
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  //Flush函数会将数据块写入SSTable文件并且刷新到磁盘
  Flush();
  assert(!r->closed);
  r->closed = true;
  //filter_block_handle为元数据的BlockHandle，metaindex_block_handle为
  //元数据索引的BlockHandle,index_block_handle为数据索引的BlockHandle
  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  // 写入元数据块
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  //写入元数据块索引
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      //添加到布隆过滤器
      // Add mapping from "filter.Name" to location of filter data
      //元数据索引块的key为"filter."加上配置的过滤器名称,默认为
      //filter.leveldb.BuiltinBloomFilter2
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      //元数据索引块的值也为一个BlockHandle,该BlockHandle包括一个指向元数据块的偏移量
      //以及元数据块的大小
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  //写入数据块索引
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  //写入尾部
  if (ok()) {
    Footer footer;
    //将元数据索引区域的BlockHandle值设置到尾部
    footer.set_metaindex_handle(metaindex_block_handle);
    //将数据索引区域的BlockHandle值设置到尾部
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
