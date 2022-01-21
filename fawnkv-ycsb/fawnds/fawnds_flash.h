/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _FAWNDS_FLASH_H_
#define _FAWNDS_FLASH_H_

#include <sys/types.h>
#include <string>
#include <stdint.h>
#include "db_structures.h"

#ifdef __APPLE__
#define pread64 pread
#define pwrite64 pwrite
#endif // #ifdef __APPLE__


using namespace std;

namespace fawn {

    class FawnDS_Flash {
    public:
        FawnDS_Flash(int fd,off_t _tail=0) : fd_(fd),tail(_tail),open_file(false) {}
        explicit FawnDS_Flash(const char *filename,off_t _tail=0);
        ~FawnDS_Flash();
        bool Write(const char* key, uint32_t key_len, const char* data, uint32_t length, off_t offset);
        bool Delete(const char* key, uint32_t key_len, off_t offset);
        bool ReadIntoHeader(off_t offset, DataHeader &data_header, string &key) const;
        bool Read(const char* key, uint32_t key_len, off_t offset, string &data) const;
        bool ReadData(off_t offset, uint32_t key_len, uint32_t data_len,string &data) const;
        off_t GetTail() const{return tail;}
    private:
        int fd_;
        bool open_file;
        off_t tail;
    };

}  // namespace fawn

#endif  // #ifndef _FAWNDS_FLASH_H_
