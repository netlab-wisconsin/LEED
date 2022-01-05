//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db_factory.h"

#include <string>
#include "basic_db.h"
#include "fawn_db.h"

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties &props) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "fawn") {
    //FawnDB(const std::string& frontendIP, const int32_t port, const std::string& clientIP = "", const int32_t clientPort = 0)
    return new FawnDB(props["feip"], stoi(props["feport"]), props["cip"], 4002);
  }
}

