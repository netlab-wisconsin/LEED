## Usage
To run mica, you need to specify the path to database file:

    ./mica -d mica.db

You may also want to change number of items to set/get:

    ./mica -n 1024 -r 512 -d mica.db

## Full Usage
```
$ ./mica -h
./mica [-h] [-n num_items] [-r read_num_items] [-s value_size] [-b extra_buckets_percentage] <-d "db_file_path">
   -h      help (this text)
   -n #    number of items to fill.
   -r #    number of items to randomly query
   -s #    set value size (bytes)
   -b #    percentage of extra buckets (0~100, default: 20)
   -d #    path to database file
```