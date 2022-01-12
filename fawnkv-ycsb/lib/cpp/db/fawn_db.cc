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
    string data = client_.get(key);
    vector<string> values = stringSplit(data, ';');
    for (auto v: values) {
        vector<string> vs = stringSplit(v, ':');
        if (fields != NULL && (fields->size() == 0 || std::find(fields->begin(), fields->end(), *vs.begin()) != fields->end())) {
          KVPair p1 = KVPair(*vs.begin(), *vs.rbegin());
          result.push_back(p1);
        }
    }
    return DB::kOK;
}

int FawnDB::Insert(const string &table, const string &key,
             vector<KVPair> &values) {
    string data = "";
    for (auto v : values) {
      data = data + v.first + ":"  + v.second + ";";
    }
    client_.put(key, data);
    //cout << "Insert"  << ' ' << key << endl;
    return DB::kOK;
}

int FawnDB::Update(const string &table, const string &key,
           vector<KVPair> &values) {
  return DB::kOK;
}

} // namespace ycsbc