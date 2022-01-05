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
  FawnDB(const std::string& frontendIP, const int32_t port, const std::string& clientIP = "", const int32_t clientPort = 0)
  : client_(frontendIP, port, clientIP, clientPort) {
  }

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
    throw "Scan: function not implemented!";
  }

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(const std::string &table, const std::string &key) {
    return DB::kOK;
  }

 private:
  FawnKVClt client_;
};

} // ycsbc

#endif // YCSB_C_REDIS_DB_H_