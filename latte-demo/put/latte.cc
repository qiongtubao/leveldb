



#include <leveldb/c.h>
#include <cassert>
#include <leveldb/options.h>
#include <leveldb/db.h>
#include <iostream>
#include <leveldb/slice.h>
using namespace std;
using namespace leveldb;


int main() {
    leveldb::DB* db;
    leveldb::Options op;
    op.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(op, "./testdb", &db);
    assert(status.ok());
    leveldb::Slice key("k1");
    leveldb::Slice value("v1");
    std::string value1;
    status = db->Get(leveldb::ReadOptions(), key, &value1);
    if (status.IsNotFound()) {
        std::cout<<"not find "<<key.data()<< std::endl;
    }
    status = db->Put(leveldb::WriteOptions(), key, value);
    assert(status.ok());
    std::cout<<"put "<< key.data() <<" " << value.data() << std::endl;
    status = db->Get(leveldb::ReadOptions(), key, &value1);
    assert(status.ok());
    
    std::cout<<"get "<< key.data()<<" "<< value1<< std::endl;
    
    status = db->Delete(leveldb::WriteOptions(), key);
    assert(status.ok());
    std::cout<<"del "<< key.data()  << std::endl;
    status = db->Get(leveldb::ReadOptions(), key, &value1);
    if (status.IsNotFound()) {
        std::cout<<"not find "<<key.data()<< std::endl;
    }
    delete(db);
    return 0;
}