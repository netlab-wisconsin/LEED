echo $1
echo '--- TEST START record_size: 256.0B db_size: 10.0KB ---'
./fawnds -f -r 1000000 -n 40 -S 256 $1
echo '--- TEST END record_size: 256.0B db_size: 10.0KB ---'
echo
echo '--- TEST START record_size: 1.0KB db_size: 10.0KB ---'
./fawnds -f -r 1000000 -n 10 -S 1024 $1
echo '--- TEST END record_size: 1.0KB db_size: 10.0KB ---'
echo
echo '--- TEST START record_size: 4.0KB db_size: 10.0KB ---'
./fawnds -f -r 1000000 -n 3 -S 4096 $1
echo '--- TEST END record_size: 4.0KB db_size: 10.0KB ---'
echo
echo '--- TEST START record_size: 256.0B db_size: 125.0MB ---'
./fawnds -f -r 1000000 -n 512000 -S 256 $1
echo '--- TEST END record_size: 256.0B db_size: 125.0MB ---'
echo
echo '--- TEST START record_size: 1.0KB db_size: 125.0MB ---'
./fawnds -f -r 1000000 -n 128000 -S 1024 $1
echo '--- TEST END record_size: 1.0KB db_size: 125.0MB ---'
echo
echo '--- TEST START record_size: 4.0KB db_size: 125.0MB ---'
./fawnds -f -r 1000000 -n 32000 -S 4096 $1
echo '--- TEST END record_size: 4.0KB db_size: 125.0MB ---'
echo
echo '--- TEST START record_size: 256.0B db_size: 250.0MB ---'
./fawnds -f -r 1000000 -n 1024000 -S 256 $1
echo '--- TEST END record_size: 256.0B db_size: 250.0MB ---'
echo
echo '--- TEST START record_size: 1.0KB db_size: 250.0MB ---'
./fawnds -f -r 1000000 -n 256000 -S 1024 $1
echo '--- TEST END record_size: 1.0KB db_size: 250.0MB ---'
echo
echo '--- TEST START record_size: 4.0KB db_size: 250.0MB ---'
./fawnds -f -r 1000000 -n 64000 -S 4096 $1
echo '--- TEST END record_size: 4.0KB db_size: 250.0MB ---'
echo
echo '--- TEST START record_size: 256.0B db_size: 500.0MB ---'
./fawnds -f -r 1000000 -n 2048000 -S 256 $1
echo '--- TEST END record_size: 256.0B db_size: 500.0MB ---'
echo
echo '--- TEST START record_size: 1.0KB db_size: 500.0MB ---'
./fawnds -f -r 1000000 -n 512000 -S 1024 $1
echo '--- TEST END record_size: 1.0KB db_size: 500.0MB ---'
echo
echo '--- TEST START record_size: 4.0KB db_size: 500.0MB ---'
./fawnds -f -r 1000000 -n 128000 -S 4096 $1
echo '--- TEST END record_size: 4.0KB db_size: 500.0MB ---'
echo
echo '--- TEST START record_size: 256.0B db_size: 1.0GB ---'
./fawnds -f -r 1000000 -n 4194304 -S 256 $1
echo '--- TEST END record_size: 256.0B db_size: 1.0GB ---'
echo
echo '--- TEST START record_size: 1.0KB db_size: 1.0GB ---'
./fawnds -f -r 1000000 -n 1048576 -S 1024 $1
echo '--- TEST END record_size: 1.0KB db_size: 1.0GB ---'
echo
echo '--- TEST START record_size: 4.0KB db_size: 1.0GB ---'
./fawnds -f -r 1000000 -n 262144 -S 4096 $1
echo '--- TEST END record_size: 4.0KB db_size: 1.0GB ---'
echo
echo '--- TEST START record_size: 256.0B db_size: 2.0GB ---'
./fawnds -f -r 1000000 -n 8388608 -S 256 $1
echo '--- TEST END record_size: 256.0B db_size: 2.0GB ---'
echo
echo '--- TEST START record_size: 1.0KB db_size: 2.0GB ---'
./fawnds -f -r 1000000 -n 2097152 -S 1024 $1
echo '--- TEST END record_size: 1.0KB db_size: 2.0GB ---'
echo
echo '--- TEST START record_size: 4.0KB db_size: 2.0GB ---'
./fawnds -f -r 1000000 -n 524288 -S 4096 $1
echo '--- TEST END record_size: 4.0KB db_size: 2.0GB ---'
echo
echo '--- TEST START record_size: 256.0B db_size: 3.5GB ---'
./fawnds -f -r 1000000 -n 14680064 -S 256 $1
echo '--- TEST END record_size: 256.0B db_size: 3.5GB ---'
echo
echo '--- TEST START record_size: 1.0KB db_size: 3.5GB ---'
./fawnds -f -r 1000000 -n 3670016 -S 1024 $1
echo '--- TEST END record_size: 1.0KB db_size: 3.5GB ---'
echo
echo '--- TEST START record_size: 4.0KB db_size: 3.5GB ---'
./fawnds -f -r 1000000 -n 917504 -S 4096 $1
echo '--- TEST END record_size: 4.0KB db_size: 3.5GB ---'
echo
echo '--- TEST START record_size: 256.0B db_size: 10.0GB ---'
./fawnds -f -r 1000000 -n 41943040 -S 256 $1
echo '--- TEST END record_size: 256.0B db_size: 10.0GB ---'
echo
echo '--- TEST START record_size: 1.0KB db_size: 10.0GB ---'
./fawnds -f -r 1000000 -n 10485760 -S 1024 $1
echo '--- TEST END record_size: 1.0KB db_size: 10.0GB ---'
echo
echo '--- TEST START record_size: 4.0KB db_size: 10.0GB ---'
./fawnds -f -r 1000000 -n 2621440 -S 4096 $1
echo '--- TEST END record_size: 4.0KB db_size: 10.0GB ---'
echo
echo '--- TEST START record_size: 4.0KB db_size: 50.0GB ---'
./fawnds -f -r 1000000 -n 13107200 -S 4096 $1
echo '--- TEST END record_size: 4.0KB db_size: 50.0GB ---'
echo
echo '--- TEST START record_size: 4.0KB db_size: 100.0GB ---'
./fawnds -f -r 1000000 -n 26214400 -S 4096 $1
echo '--- TEST END record_size: 4.0KB db_size: 100.0GB ---'
echo

