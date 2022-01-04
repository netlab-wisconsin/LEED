#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"

#include "FawnKV.h"
#include "TFawnKVRemote.h"

#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

void usage()
{
    printf("Usage: ./tester_remote [-c clientIP] frontendIP [fePort]\n");
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
}


inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}


string ParseCommandLine(int argc, char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}



int main(int argc, char **argv)
{
    utils::Properties props;
    string file_name = ParseCommandLine(argc, argv, props);
    ycsbc::CoreWorkload wl;
    wl.Init(props);

    const int num_threads = stoi(props.GetProperty("threadcount", "1"));

    // Loads data
    vector<future<int>> actual_ops;
    int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    for (int i = 0; i < num_threads; ++i) {
        printf("123\n");
        // actual_ops.emplace_back(async(launch::async,
        //     DelegateClient, db, &wl, total_ops / num_threads, true));
    }
    // assert((int)actual_ops.size() == num_threads);

    // int sum = 0;
    // for (auto &n : actual_ops) {
    //     assert(n.valid());
    //     sum += n.get();
    // }
    // cerr << "# Loading records:\t" << sum << endl;
    
    // extern char *optarg;
    // extern int optind;
    // string myIP = "";
    // int myPort;
    // int ch;
    // int port = 4001;
    // while ((ch = getopt(argc, argv, "c:p:")) != -1) {
    //     switch (ch) {
    //     case 'c':
    //         myIP = optarg;
	//     break;
	// case 'p':
	//     myPort = atoi(optarg);
	//     break;
	// default:
    //         usage();
    //         exit(-1);
    //     }
    // }

    // argc -= optind;
    // argv += optind;

    // if (argc < 1) {
	// usage();
	// exit(-1);
    // } else if (argc == 2) {
	// port = atoi(argv[1]);
    // }

    // FawnKVClt client(argv[0], port, myIP, myPort);

    // for (int i = 0; i < 10000; i++) {
	// printf("putting..");
	// client.put("abc", "value");
	// string value = client.get("abc");
	// printf("%s\n", value.c_str());
    // }
    // return 0;
}
