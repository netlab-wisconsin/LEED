#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include "thread"
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"
#include <mutex>
#include "timing.h"

#include "FawnKV.h"
#include "TFawnKVRemote.h"

#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION(start, end) (std::chrono::duration<double>((end)-(start)).count() * 1000 * 1000)


#include <assert.h>

template <typename T1, typename T2> typename T1::value_type quant(const T1 &x, T2 q)
{
    assert(q >= 0.0 && q <= 1.0);
    
    const auto n  = x.size();
    const auto id = (n - 1) * q;
    const auto lo = floor(id);
    const auto hi = ceil(id);
    const auto qs = x[lo];
    const auto h  = (id - lo);
    return (1.0 - h) * qs + h * x[hi];
}

vector<double> search_times;
pthread_mutex_t count_lock;
vector<double> latencyList;


void usage()
{
    printf("Usage: /tester_remote_ycsbc -threads 15 -db fawn -P workloads/workloada.spec -feip 10.134.11.95 -cip 10.134.11.95 -feport 4001 -load 1\n");
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
  cout << "  -cip clientIP" << endl;
  cout << "  -feip frontendIP" << endl;
  cout << "  -feport fePort" << endl;
  cout << "  -load [1/0] if load database" << endl;
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
    } else if (strcmp(argv[argindex], "-cip") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("cip", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-feip") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("feip", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-feport") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("feport", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-load") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("load", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-operations") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("operations", argv[argindex]);
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

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading
    //, vector<double>& latencyTime
    ) {
  db->Init();
  ycsbc::Client client(*db, *wl);
  vector<double> latLst;
  int oks = 0;
  double transactionLatency = 0;
  for (int i = 0; i < num_ops; ++i) {
    if (i % 1000 == 0) {
      cout << "## DelegateClient runs " << i << "/" << num_ops << "ops" << endl; 
    }
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      timeval single_transaction_start, single_transaction_end;
      gettimeofday(&single_transaction_start, NULL);
      oks += client.DoTransaction();
      gettimeofday(&single_transaction_end, NULL);
      latLst.push_back(timeval_diff(&single_transaction_start, &single_transaction_end));
      transactionLatency += timeval_diff(&single_transaction_start, &single_transaction_end);
    }
  }
  db->Close();
  if (!is_loading) {
      pthread_mutex_lock(&count_lock);
      search_times.push_back(transactionLatency);
      latencyList.insert(latencyList.end(), latLst.begin(), latLst.end());
      pthread_mutex_unlock(&count_lock);
  }
  return oks;
}

int main(int argc, char **argv)
{   
    utils::Properties props;
    string file_name = ParseCommandLine(argc, argv, props);

    ycsbc::CoreWorkload wl;
    wl.Init(props);
    
    const int num_threads = stoi(props.GetProperty("threadcount", "1"));
    cerr << "# Thread Counts:\t" << num_threads << endl;

    ycsbc::DB **dbs = new ycsbc::DB *[num_threads];
    for (int i = 0; i < num_threads; ++i) {
      props.SetProperty("cport", std::to_string(8000 + i));
      ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
      if (!db) {
          cout << "Unknown database name " << props["dbname"] << endl;
          exit(0);
      }
      dbs[i] = db;
    }
    // Loads data
    vector<thread> threads;
    // vector<future<int>> actual_ops;
    int sum = 0;
    int total_ops = 0;

    if (stoi(props["load"]) == 1) {
        auto start = TIME_NOW;
        total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back(bind(
                DelegateClient, dbs[i], &wl, total_ops / num_threads, true));
        }
        assert((int)threads.size() == num_threads);

        cerr << "# Thread init complete!\t" << endl;

        for (auto &t : threads) {
            t.join();
        }
        double time = TIME_DURATION(start, TIME_NOW);
        printf("Finish %ld requests in %.3lf seconds.\n", total_ops, time/1000/1000);
        cerr << "# Loading records:\t" << sum << endl;
    } else {
        cerr << "# Not Loading records, just run"<< endl;
    }
    
    threads.clear();
    total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    
    utils::Timer<double> timer;
    timer.Start();
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(bind(
            DelegateClient, dbs[i], &wl, total_ops, false));
    }
    assert((int)threads.size() == num_threads);

    sum = 0;
    for (auto &t : threads) {
            t.join();
    }
    double duration = timer.End();
    cerr << "# Transaction throughput (OPS)" << endl;
    cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    cerr << num_threads * total_ops / duration << endl;

    double totalTime = 0;
    for (int i = 0; i < num_threads; i++) {
        totalTime += search_times[i];
    }
    cerr << "# Transaction Latency (us)" << endl;
    cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    cerr << totalTime / (num_threads * total_ops )* 1000 * 1000 << "us" << endl;

    cerr << "# Transaction p999 Latency (us)" << endl;
    cerr << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
    sort(latencyList.begin(), latencyList.end());
    cout << quant(latencyList, 0.999) * 1000 * 1000 << "us" << endl;
    return 0;
}
