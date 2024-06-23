



#include <leveldb/c.h>
#include <cassert>
#include <leveldb/options.h>
#include <leveldb/db.h>
#include <iostream>
#include <leveldb/slice.h>
#include <leveldb/write_batch.h>
#include <sstream>
using namespace std;
using namespace leveldb;


int main() {
    leveldb::DB* db;
    leveldb::Options op;
    op.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(op, "./testdb", &db);
    assert(status.ok());
    std::string key = "k1";
    std::stringstream ss;
    leveldb::WriteBatch batch;
    std::string value;
    char index[10];
    for (int i = 0; i < 10; i++) {
        std::string pre = "k";
        sprintf(index, "%d", i);
        key = pre + index;
        batch.Put(key, index);
    }
    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok());
    status = db->Get(leveldb::ReadOptions(), "k3", &value);
    assert(status.ok());
    std::cout<<"read the value of k3: " << value << std::endl;

    for(int i = 0; i < 10; i++) {
        std::string pre = "k";
        sprintf(index, "%d", i);
        key = pre + index;
        batch.Delete(key);
    }

    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok());

    std::cout<<"batch delete is finished" << std::endl;

    
    delete(db);
    return 0;
}