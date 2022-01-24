/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "fawnds.h"
#include "fawnds_flash.h"
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <cmath>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include "print.h"
#include "dbid.h"
#include "hashutil.h"
#include "timing.h"

enum { SCAN_RANDOM, SCAN_SEQUENTIAL };

using namespace std;
using namespace fawn;
using fawn::HashUtil;

struct benchData {
    FawnDS<FawnDS_Flash>** f;
    u_int num_records;
    u_int num_to_scan;
    u_int offset;
    u_int numThreads;
    double putFra;
};

uint32_t readyCount = 0;
pthread_mutex_t count_lock;
vector<double> search_times;
vector<double> read_latency_times;
u_int max_record = 0;


void
usage()
{
    fprintf(stderr,
            "fawnds_bench [-hfsmw] [-r num_scan] [-n num_records] [-p num_db] [-t threads] [-b \"dir1 dir2 dir3\"] [-a num_procs] <dbfile>\n"
            );
}

void
help()
{
    usage();
    fprintf(stderr,
            "   -h      help (this text)\n"
            "   -f      fill\n"
            "   -s      sequential scan\n"
            "   -r #    number of entries to randomly scan\n"
            "   -n #    number of entries to fill or scan.\n"
            "   -m      use mmap hashtable\n"
            "   -b #    directories for multiple files\n"
            "   -t #    use # threads (default:  unthreaded)\n"
            "   -a #    Turn on thread affinity, specify number of processors\n"
            "   -w      random write test\n"
            "   -S #    set value size (bytes)\n"
            "   -F #    set Fraction of Put Requests"
            );
}

void *compactThread(void* p) {
    FawnDS<FawnDS_Flash>** dbs = (FawnDS<FawnDS_Flash>**)p;
    pthread_rwlock_t tempLock;
    pthread_rwlock_init(&tempLock, NULL);
    if (!dbs[0]->Rewrite(&tempLock)) {
        perror("Rewrite failed!\n");
    }
    pthread_rwlock_destroy(&tempLock);
    return NULL;
}

void *randomReadThread(void* p)
{
    benchData* bd = (benchData*)p;
    FawnDS<FawnDS_Flash>** dbs = bd->f;
    u_int num_records = bd->num_records;
    u_int num_to_scan = bd->num_to_scan;
    u_int offsetVal = bd->offset;
    FawnDS<FawnDS_Flash> *mydb = dbs[offsetVal];

    struct timeval tv_start, tv_end;
    char *l = (char *)malloc(sizeof(char) * num_to_scan * sizeof(uint32_t));

    for (u_int i = 0; i < num_to_scan; i++) {
        u_int val = (offsetVal * num_records) + (rand()%num_records);
        if (val < max_record) {
            string ps_key((const char *)&val, sizeof(val));
            uint32_t key_id = HashUtil::BobHash(ps_key);
            DBID key((char *)&key_id, sizeof(u_int32_t));
            memcpy(l + (i * sizeof(uint32_t)), key.data(), key.get_actual_size());
        }
        else {
            i--;
        }
    }

    pthread_mutex_lock(&count_lock);
    readyCount++;
    pthread_mutex_unlock(&count_lock);
    struct timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 20000;

    while (readyCount < bd->numThreads) {
        nanosleep(&req, NULL);
    }
    gettimeofday(&tv_start, NULL);
    string data;
    const char *key = l;
    double single_search_time = 0;
    struct timeval single_start, single_end;
    for (u_int i = 0; i < num_to_scan; ++i) {
        gettimeofday(&single_start, NULL);
        if(!mydb->Get(key, sizeof(uint32_t), data)) {
            perror("Get failed.\n");
        }
        gettimeofday(&single_end, NULL);
        single_search_time += timeval_diff(&single_start, &single_end);
        key += sizeof(uint32_t);
    }

    gettimeofday(&tv_end, NULL);
    double dbsearch_time = timeval_diff(&tv_start, &tv_end);
    //printf("Time to search DB: %f seconds\n", dbsearch_time);
    //printf("Query rate: %f\n", ((double)num_to_scan / dbsearch_time) );
    free(l);

    pthread_mutex_lock(&count_lock);
    search_times.push_back(dbsearch_time);
    read_latency_times.push_back(single_search_time);
    //printf("Query rate: %f\n", ((double)single_search_time / num_to_scan) );
    pthread_mutex_unlock(&count_lock);
    return NULL;
}


void bench(int argc, char** argv) {
    extern char *optarg;
    extern int optind;

    int ch;
    char* dbname = NULL;
    vector<string> fileBases;

    u_int num_to_scan = 0;
    int mode = SCAN_RANDOM;
    bool createdb = false;
    int writeTest = 0;
    int compactAt = -1;
    int numThreads = 1;
    bool useThreads = false;
    bool setAffinity = false;
    int numProcs = 1;
    int valuesize = 1024;
    int seqWriteTest = 0;

    double putFraction = -1;

    pthread_t compactThreadId_;
    while ((ch = getopt(argc, argv, "hfn:r:p:swcF:t:b:a:S:q")) != -1)
        switch (ch) {
        case 'n':
            max_record = atoi(optarg);
            break;
        case 'r':
            num_to_scan = atoi(optarg);
            break;
        case 'f':
            createdb = true;
            break;
        case 's':
            mode = SCAN_SEQUENTIAL;
            break;
        case 'w':
            writeTest = 1;
            break;
        case 'q':
            seqWriteTest = 1;
            break;
        case 'c':
            compactAt = atoi(optarg);
            break;
        case 't':
            useThreads = true;
            numThreads = atoi(optarg);
            break;
        case 'b':
            tokenize(optarg, fileBases, " ");
            break;
        case 'S':
            valuesize = atoi(optarg);
            break;
        case 'a':
            setAffinity = true;
            numProcs = atoi(optarg);
            break;
        case 'F':
            cout << optarg << endl;
            putFraction = atof(optarg);
            cout << putFraction << endl;
            break;
        case 'h':
            help();
            exit(0);
        default:
            usage();
            exit(-1);
        }
    argc -= optind;
    argv += optind;
    cout << putFraction << endl;
    if (fileBases.size() == 0 && argc != 1) {
        usage();
        exit(-1);
    }

    if (fileBases.size() == 0)
        dbname = argv[0];

    struct timeval tv_start, tv_end;
    gettimeofday(&tv_start, NULL);

    string value(valuesize, 'a');

    FawnDS<FawnDS_Flash> **dbs = (FawnDS<FawnDS_Flash>**)malloc(numThreads * sizeof(FawnDS<FawnDS_Flash>*));

    // size?  num_records / numThreads
    int num_recs_per_db = (int) (max_record / numThreads);

    int bucket = num_recs_per_db;
    if (max_record % numThreads != 0)
        bucket += 1;

    pthread_t* workerThreadIds_ = (pthread_t*) malloc (numThreads * sizeof(pthread_t));
    benchData* bd = (benchData*) malloc (numThreads * sizeof(benchData));

    u_int fileBaseOffset = 0;
    for (int i = 0; i < numThreads; i++) {
        ostringstream dbname_i;
        if (fileBases.size() > 0) {
            dbname_i << fileBases[fileBaseOffset] << "_" << i;
            fileBaseOffset++;
            if (fileBaseOffset == fileBases.size())
                fileBaseOffset = 0;

        } else {
            dbname_i << dbname << "_" << i;
        }
        if (createdb) {
            //remove(dbname_i.str().c_str());
            dbs[i] = FawnDS<FawnDS_Flash>::Create_FawnDS(dbname_i.str().c_str(), num_recs_per_db * 2, .9,
                                           .8);
            if(!dbs[i]) {
                perror("Create DB failed\n");
                exit(-1);
            }
        } else {
            printf("reading file %s\n", dbname_i.str().c_str());
            dbs[i] = FawnDS<FawnDS_Flash>::Open_FawnDS(dbname_i.str().c_str());
        }
    }

    if (createdb && !writeTest) {
        // timeval seq_write_start, seq_write_end;
        // timeval single_seq_write_start, single_seq_write_end;
        // double writeSeqLatency = 0;
        // gettimeofday(&seq_write_start, NULL);
        // Fill it sequentially if we're not testing writing
        for (u_int i = 0; i < max_record; ++i) {
            int num = i;
            string ps_key((const char *)&num, sizeof(num));
            u_int32_t key_id = HashUtil::BobHash(ps_key);
            DBID key((char *)&key_id, sizeof(u_int32_t));

            int dbi = (int)(i / bucket);
            // gettimeofday(&single_seq_write_start, NULL);
            if(!dbs[dbi]->Insert(key.data(), key.get_actual_size(), value.data(), valuesize)) {
                perror("Insert failed\n");
            }
            // gettimeofday(&single_seq_write_end, NULL);
            // writeSeqLatency += timeval_diff(&single_seq_write_start, &single_seq_write_end);
        }
        // gettimeofday(&seq_write_end, NULL);
        // cout << "Seq Insert Rate: " << num_to_scan / timeval_diff(&seq_write_start,&seq_write_end) << " inserts per second" << endl;
        // cout << "Seq Insert Latency: " << writeSeqLatency / num_to_scan * 1000 * 1000<< " us " << endl;
        // this is required since we're not splitting/merging/rewriting initially
        for (int i = 0; i < numThreads; i++) {
            if (!dbs[i]->WriteHashtableToFile()) {
                perror("Could not write hashtable.\n");
            }
        }
    }
    gettimeofday(&tv_end, NULL);


    if (createdb) {
        double dbcreate_time = timeval_diff(&tv_start, &tv_end);
        printf("Time to create DB: %f seconds\n", dbcreate_time);
    }

    srand((tv_end.tv_sec << 2) + tv_end.tv_usec);

    if (seqWriteTest) {
        timeval seq_write_start, seq_write_end;
        timeval single_seq_write_start, single_seq_write_end;
        double writeSeqLatency = 0;
        gettimeofday(&seq_write_start, NULL);
        for (u_int i = 0; i < num_to_scan; ++i) {
            u_int val = i%max_record;
            string ps_key((const char *)&val, sizeof(val));
            u_int32_t key_id = HashUtil::BobHash(ps_key);
            DBID key((char *)&key_id, sizeof(u_int32_t));
            if (i == compactAt) {
                cout << "Compacting..." << endl;
                pthread_create(&compactThreadId_, NULL,
                               compactThread, dbs);

            }
            int dbi = (int)(val / bucket);
            gettimeofday(&single_seq_write_start, NULL);
            if(!dbs[dbi]->Insert(key.data(), key.get_actual_size(), value.data(), valuesize)) {
                perror("Insert failed\n");
            }
            gettimeofday(&single_seq_write_end, NULL);
            writeSeqLatency += timeval_diff(&single_seq_write_start, &single_seq_write_end);
        }
        gettimeofday(&seq_write_end, NULL);
        cout << "Seq Insert Rate: " << num_to_scan / timeval_diff(&seq_write_start,&seq_write_end) << " inserts per second" << endl;
        cout << "Seq Insert Latency: " << writeSeqLatency / num_to_scan * 1000 * 1000<< " us " << endl;
    }
    
    // random write test
    double writeLatency = 0;
    if (writeTest) {
        vector<int> l;
        for (u_int i = 0; i < num_to_scan; i++) {
            l.push_back(rand()%max_record);
        }
        int n = l.size();
        timeval write_tv_start, write_tv_end;
        timeval single_write_start, single_write_end;
        gettimeofday(&write_tv_start, NULL);
        for (int i = 0; i < n; i++) {
            u_int val = l[i];
            string ps_key((const char *)&val, sizeof(val));
            u_int32_t key_id = HashUtil::BobHash(ps_key);
            DBID key((char *)&key_id, sizeof(u_int32_t));

            if (i == compactAt) {
                cout << "Compacting..." << endl;
                pthread_create(&compactThreadId_, NULL,
                               compactThread, dbs);

            }

            int dbi = (int)(val / bucket);
            gettimeofday(&single_write_start, NULL);
            if(!dbs[dbi]->Insert(key.data(), key.get_actual_size(), value.data(), valuesize)) {
                perror("Insert failed\n");
            }
            gettimeofday(&single_write_end, NULL);
            writeLatency += timeval_diff(&single_write_start, &single_write_end);
        }
        gettimeofday(&write_tv_end, NULL);
        cout << "Random Insert Rate: " << num_to_scan / timeval_diff(&write_tv_start,&write_tv_end) << " inserts per second" << endl;
        cout << "Random Insert Latency: " << writeLatency / num_to_scan * 1000 * 1000<< " us " << endl;

    } else {
        pthread_mutex_init(&count_lock, NULL);
        for (int i = 0; i < numThreads; i++) {
            bd[i].f = dbs;
            bd[i].num_to_scan = num_to_scan;
            bd[i].num_records = bucket;
            bd[i].offset = i;
            bd[i].numThreads = numThreads;
            bd[i].putFra = putFraction;
            if (useThreads) {
                pthread_attr_t attr;
                pthread_attr_init(&attr);
#ifdef cpu_set_t /* GNU/Linux-only! */
                if (setAffinity) {
                    cpu_set_t cpuset;
                    CPU_ZERO(&cpuset);
                    CPU_SET(i % numProcs, &cpuset);
                    pthread_attr_setaffinity_np(&attr, sizeof(cpuset), &cpuset);
                }
#endif
                pthread_create(&workerThreadIds_[i], &attr,
                               randomReadThread, &bd[i]);
            } else {
                randomReadThread(&bd[0]);
            }
        }
    }

    if (putFraction != -1) {
        vector<int> l;
        for (u_int i = 0; i < num_to_scan; i++) {
            l.push_back(rand()%max_record);
        }
        int n = l.size();
        timeval write_tv_start, write_tv_end;
        timeval single_write_start, single_write_end;
        gettimeofday(&write_tv_start, NULL);
        string data;
        for (int i = 0; i < n; i++) {
            u_int val = l[i];
            string ps_key((const char *)&val, sizeof(val));
            u_int32_t key_id = HashUtil::BobHash(ps_key);
            DBID key((char *)&key_id, sizeof(u_int32_t));

            if (i == compactAt) {
                cout << "Compacting..." << endl;
                pthread_create(&compactThreadId_, NULL,
                               compactThread, dbs);

            }

            int dbi = (int)(val / bucket);
            gettimeofday(&single_write_start, NULL);
            if (i < int(n * putFraction)) {
                if(!dbs[dbi]->Insert(key.data(), key.get_actual_size(), value.data(), valuesize)) {
                    perror("Insert failed\n");
                }
            } else {

                if (!dbs[dbi]->Get(key.data(), sizeof(uint32_t), data)) {
                    perror("Read failed\n");
                }
            }
            gettimeofday(&single_write_end, NULL);
            writeLatency += timeval_diff(&single_write_start, &single_write_end);
        }
        gettimeofday(&write_tv_end, NULL);
        cout << "Random WriteRead Rate: " << num_to_scan / timeval_diff(&write_tv_start,&write_tv_end) << " inserts per second" << endl;
        cout << "Random WriteRead Latency: " << writeLatency / num_to_scan * 1000 * 1000<< " us " << endl;
    } 


    if (useThreads) {
        for (int i = 0; i < numThreads; i++) {
            pthread_join(workerThreadIds_[i], NULL);
        }
    }

    if (compactAt != -1) {
        pthread_join(compactThreadId_, NULL);
    }
    pthread_mutex_destroy(&count_lock);
    free(workerThreadIds_);
    free(bd);
    if (!writeTest){
        double totalTime = 0;
        double totalReadLatency = 0;
        for (int i = 0; i < numThreads; i++) {
            totalTime = max(totalTime, search_times[i]);
            totalReadLatency += read_latency_times[i];
        }
        double totalQueries = num_to_scan * numThreads;
        cout << "Random Query Rate: " << totalQueries / totalTime << " queries per second" << endl;
        cout << "Random Query Latency: " << totalReadLatency / totalQueries * 1000 * 1000 << "us" << endl;
    }
}


int main(int argc, char** argv) {
    bench(argc, argv);
    return 0;
}