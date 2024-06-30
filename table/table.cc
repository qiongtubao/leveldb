// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
/**
 * sstable文件
 * 整体分为4部分
 * 1. 数据区域（具体键值对）
 *    a.块 （block_size 默认4KB）
 *      1. 键值对（n)
 *          a.数据结构
 *           1.共享字节长度
 *           2.非共享字节长度
 *           3.值的长度
 *           4.键的非共享部分数据长度
 *           5.值数据
 *          b.举例
 *           1.k:db:redis,  redis
 *              0,10,5,k:db:redis,redis
 *              共享长度为0     （重启点  block_restart_interval 默认16 每16个键值对需要开启新重启点）
 *           2.k:db:leveldb, leveldb
 *              5,7,7,leveldb,leveldb
 *              共享长度 (k:db:) 5
 *              非共享长度 (leveldb) 7
 *              值长度     （leveldb) 7
 *              非共享键    （leveldb）
 *              值          （leveldb）
 *      2. 重启点数据 
 *          a.重启点offset(n * 4字节)
 *          b.重启点个数（4字节）
 *      3. 压缩类型 （0x0 不压缩,0x1 snappy压缩）
 *      4. 校验数据（crc4）
 * 2. 元数据区域 （布隆过滤器）
 *      a. 布隆过滤器 每2KB键值对会生成一个过滤器，保存在filter
 *        1. 过滤器内容 （看成数组）
 *        2. 过滤器的偏移量 （4 * n）
 *        3. filter 内容大小（4字节）
 *        4. filter base （1字节）11 表示2的11次幂=2KB
 *        5. 块类型 type（1字节）不压缩 kNoCompression
 *        6. 块校验 crc（4字节）
 * 3. 索引区域 （数据索引和元数据索引）
 *    a.索引的数据块
 *      1. key 字符串 value是blockHandle  
 *          a. key是 数据块前一个的最大key值 和后一个块的最小key值的最小分隔字符
 *              1. 比如 abceg 和 abcqddh 取最短分隔字符  abcf
*           b. value 数据块在sstable中偏移量和大小
 * 4. 尾部  
 *    （40字节(2个BlockHandle 每个最多20字节。不足40字节padding填满)  
 *      + 8个字节固定魔数0xdb4775248b80fb57）
 *  
 *  读取sstable首先读取尾部
 *  1. 确定文件是sstable 根据魔数
 *  2. 确定数据和元数据索引区域 偏移量和大小
 *   
 * 
 */
namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }
  //读取48个字符
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  //随机读文件 读取最后48个字节
  //<meta index handle 最多20字节 可变64bit = （（64bit/（8-1） * 2 = 20）
  //<index handle最多20字节> 
  //<padding 不到40字节 填充满40字节> 
  //<magic 8字节>
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  //通过字符串解析成footer
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  //读取block
  //解析出来存放到index_block_contents 
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  //解析meta_index
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);
  //block迭代器
  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  //key 获得filter.{过滤内容}
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  //读取filter
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// 读取block reader
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  //arg参数为一个Table结构，options为读取时的参数结构，index_value为通过
  //第一层迭代器读取到的值，该值为一个BlockHandle,BlockHandle中的偏移量指向要
  //查找的块在SSTable中的偏移量,BlockHandle中的大小表明要查找的块的大小
  //将arg变量转换为一个Table结构
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  //将index_value解码到BlockHandle类型的变量handle中
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    //contents中保存一个块的内容
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      //创建key
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      //block_cache内寻找key 获得Cache
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        //在SSTable文件中，通过BlockHandle变量handle所指向的偏移量以及大小读取
        //一个块的内容到contents变量
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          //生成一个block结构
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    //生成一个block的迭代器 通过该迭代器读取数据
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      //第一层迭代器，为一个块迭代器
      rep_->index_block->NewIterator(rep_->options.comparator),
      //第二层迭代器，也是一个块迭代器
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
