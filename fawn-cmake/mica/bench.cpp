#include <vector>
#include <sys/time.h>
#include "table.h"
#include "timing.h"
#include "util.h"

extern "C" {
#include "city.h"
}
void test(){
    mehcached_table table;
    uint32_t num_items=2048,valuesize=1024;
    num_items = num_items * MEHCACHED_ITEMS_PER_BUCKET / (MEHCACHED_ITEMS_PER_BUCKET -2);
    mehcached_table_init(&table,(num_items + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET,
                         true, false,"/root/test_db",10);
    uint64_t key=0;
    string value(valuesize, 'a'),value2;
    mehcached_set(&table, key, (uint8_t *)&key, 8, reinterpret_cast<const uint8_t *>(value.c_str()), value.length());
    mehcached_get(&table,key,(uint8_t *)&key,8,value2);
    mehcached_delete(&table,key,(uint8_t *)&key,8);
    bool r=mehcached_get(&table,key,(uint8_t *)&key,8,value2);
    printf("%d",r);
    mehcached_table_free(&table);
}
inline uint64_t index_to_key(uint64_t index){
    return CityHash64((char *)&index,sizeof(uint64_t));
}

void fill_db(mehcached_table * table,uint64_t num_items,uint32_t value_size){
    string value(value_size, 'a');
    for(uint64_t i=0;i<num_items;++i){
        uint64_t key=index_to_key(i);
        if(!mehcached_set(table,key,(uint8_t *)&key, 8,(uint8_t *)value.c_str(), value.length())) {
            fprintf(stderr, "set fail, index: %lu\n", i);
            exit(-1);
        }

    }
}

struct Option{
    uint64_t num_items,read_num_items;
    uint32_t value_size;
    uint8_t extra_buckets_percentage;
    char filename[1024]{};
    Option():num_items(1024),read_num_items(512),value_size(1024),extra_buckets_percentage(20){}
};
void help(){
    fprintf(stderr,
        "./mica [-h] [-n num_items] [-r read_num_items] [-s value_size] [-b extra_buckets_percentage] <-d \"db_file_path\">\n"
        );
    fprintf(stderr,
        "   -h      help (this text)\n"
        "   -n #    number of items to fill.\n"
        "   -r #    number of items to randomly query\n"
        "   -s #    set value size (bytes)\n"
        "   -b #    percentage of extra buckets (0~100, default: 20)\n"
        "   -d #    path to database file\n"
        );
}
void get_options(int argc, char** argv,Option *opt){
    int ch;
    while ((ch = getopt(argc, argv, "hn:r:s:b:d:")) != -1)
        switch (ch) {
            case 'h':
                help();
                exit(-1);
            case 'n':
                opt->num_items=stoull(optarg);
                break;
            case 'r':
                opt->read_num_items= stoull(optarg);
                break;
            case 's':
                opt->value_size=stoul(optarg);
                break;
            case 'b':
                opt->extra_buckets_percentage= stoi(optarg);
                break;
            case 'd':
                strcpy(opt->filename,optarg);
                break;
            default:
                help();
                exit(-1);
        }
}


int main(int argc, char** argv){
//    test();
    Option opt;
    get_options(argc,argv,&opt);
    mehcached_table table;
    uint64_t num_items_max = opt.num_items * MEHCACHED_ITEMS_PER_BUCKET / (MEHCACHED_ITEMS_PER_BUCKET -3);
    mehcached_table_init(&table,(num_items_max + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET,
                         true, false,opt.filename,opt.extra_buckets_percentage);
    fill_db(&table,opt.num_items,opt.value_size);
    puts("db created successfully.");
    std::vector<uint64_t> keys(opt.read_num_items);
    for(uint64_t i=0;i<opt.read_num_items;++i)
        keys[i]= index_to_key(random()%opt.num_items);
    puts("keys generated.");
    // mehcached_print_buckets(&table);
    timeval tv_start, tv_end;
    string value;
    gettimeofday(&tv_start, NULL);
    for(uint64_t i=0;i<opt.read_num_items;++i)
        if(!mehcached_get(&table,keys[i],(uint8_t *)&keys[i],8,value))
            fprintf(stderr,"get failed! index: %ld\n",i);
    gettimeofday(&tv_end, NULL);
    printf("Query rate: %f\n", ((double)opt.read_num_items / timeval_diff(&tv_start,&tv_end)));
    mehcached_table_free(&table);
    return 0;
}