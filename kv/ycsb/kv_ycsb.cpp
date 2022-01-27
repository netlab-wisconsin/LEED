#include "kv_ycsb.h"

#include <iostream>

#include "../kv_memory.h"
#include "../utils/city.h"
#include "core/core_workload.h"
using namespace std;
using namespace ycsbc;

void kv_ycsb_init(kv_ycsb_handle *self, char *propertyfile, uint64_t *record_cnt, uint64_t *operation_cnt,
                  uint32_t *value_size) {
    utils::Properties props;
    CoreWorkload *wl = new CoreWorkload();
    ifstream input(propertyfile);
    try {
        props.Load(input);
    } catch (const string &message) {
        cout << message << endl;
        exit(0);
    }
    wl->Init(props);
    assert(wl->read_all_fields() && wl->write_all_fields());
    assert(stoi(props[CoreWorkload::FIELD_COUNT_PROPERTY]) == 1);
    *self = wl;
    *record_cnt = stoull(props[CoreWorkload::RECORD_COUNT_PROPERTY]);
    *operation_cnt = stoull(props[CoreWorkload::OPERATION_COUNT_PROPERTY]);
    *value_size = stoi(props[CoreWorkload::FIELD_LENGTH_PROPERTY]);
}

void kv_ycsb_fini(kv_ycsb_handle self) { delete reinterpret_cast<CoreWorkload *>(self); }

enum kv_ycsb_operation kv_ycsb_next(kv_ycsb_handle self, bool is_seq, uint8_t *key, uint8_t *value) {
    CoreWorkload *wl = reinterpret_cast<CoreWorkload *>(self);
    enum kv_ycsb_operation op = is_seq ? YCSB_SEQ : (enum kv_ycsb_operation)(wl->NextOperation());
    assert(op != YCSB_SCAN);
    const std::string &key_str = is_seq ? wl->NextSequenceKey() : wl->NextTransactionKey();
    const uint128 &key_128 = CityHash128(key_str.c_str(), key_str.size());
    kv_memcpy(key, &key_128, 16);

    if (op != YCSB_READ && value) {
        std::vector<DB::KVPair> values;
        wl->BuildValues(values);
        assert(values.size() == 1);
        kv_memcpy(value, values[0].second.c_str(), values[0].second.size());
    }
    return op;
}