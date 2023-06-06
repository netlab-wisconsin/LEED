//
//  redis_db.h
//  YCSB-C
//

#ifndef YCSB_C_FAWN_DB_H_
#define YCSB_C_FAWN_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include "core/properties.h"

#include "FawnKV.h"
#include "TFawnKVRemote.h"


using std::cout;
using std::endl;

namespace ycsbc {

class FawnDB : public DB {
 public:
  FawnDB(const std::string& frontendIP, const int32_t port, const std::string& clientIP = "", const int32_t clientPort = 0):
  client_(frontendIP, port, clientIP, clientPort)
  {
  }

  void Init() {
      cout << "Init a ycsb client" << endl;
  }

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
      vector<KVPair> res;
      for (int i = 0; i < len; i++) {
         Read(table, key, fields, res);
         result.push_back(res);
      }
      return DB::kOK;
  }

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(const std::string &table, const std::string &key) {
    return DB::kOK;
  }

  std::vector<std::string> stringSplit(const std::string& str, char delim) {
    std::vector<std::string> elems;
    auto lastPos = str.find_first_not_of(delim, 0);
    auto pos = str.find_first_of(delim, lastPos);
    while (pos != std::string::npos || lastPos != std::string::npos) {
        elems.push_back(str.substr(lastPos, pos - lastPos));
        lastPos = str.find_first_not_of(delim, pos);
        pos = str.find_first_of(delim, lastPos);
    }
    return elems;
  }

 private:
  std::string frontendIP_;
  int32_t port_;
  std::string clientIP_;
  int32_t clientPort_;
  FawnKVClt client_;
};

} // ycsbc

#endif // YCSB_C_REDIS_DB_H_