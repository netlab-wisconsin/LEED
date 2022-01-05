//
//  redis_db.cc
//  YCSB-C
//

#include "fawn_db.h"

#include <cstring>

using namespace std;

namespace ycsbc {

int FawnDB::Read(const string &table, const string &key,
         const vector<string> *fields,
         vector<KVPair> &result) {
  return DB::kOK;
}

int FawnDB::Insert(const string &table, const string &key,
             vector<KVPair> &values) {
     return DB::kOK;
}

int FawnDB::Update(const string &table, const string &key,
           vector<KVPair> &values) {
  return DB::kOK;
}

} // namespace ycsbc