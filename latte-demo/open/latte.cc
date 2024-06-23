

#include <leveldb/c.h>
#include <cassert>
#include <leveldb/options.h>
#include <leveldb/db.h>
using namespace leveldb;


int main() {
    leveldb::DB* db;
    leveldb::Options op;
    op.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(op, "./testdb", &db);
    assert(status.ok());
    delete(db);
    return 0;
}