
threads=(1 4 8 16 32 50 100 160 200)

cd ./rocksdb/tools
for th in "${threads[@]}"
do
  nums=$((5000000 / th))
  ./db_bench \
  --benchmarks=fillrandom \
  --num=$nums  \
  --memtablerep=skip_list  \
  --threads="$th"  \
  --key_size=12 \
  --value_size=500 \
  --use_existing_db=0 \
  --open_files=-1  \
  --db=./test_dir1/db  \
  --wal_dir=./test_dir1/wal  \
  --disable_wal=1  \
  --block_size=4194304  \
  --cache_size=0  \
  --write_buffer_size=268435456  \
  --target_file_size_base=268435456  \
  --compression_type=none  \
  --sync=0 \
  --num_levels=8  \
  --max_background_jobs=16  \
  --cache_numshardbits=6  \
  --compression_max_dict_bytes=0  \
  --bytes_per_sync=0  \
  --benchmark_write_rate_limit=0  \
  --verify_checksum=0  \
  --max_bytes_for_level_multiplier=8  \
  --statistics=0  \
  --stats_per_interval=1  \
  --report_interval_seconds=1  \
  --histogram=1  \
  --bloom_bits=10  \
  --subcompactions=1  \
  --compaction_style=0  \
  --min_level_to_compress=-1  \
  --level_compaction_dynamic_level_bytes=true  \
  --disable_auto_compactions=1 \
  --allow_concurrent_memtable_write=true  \
  --pin_l0_filter_and_index_blocks_in_cache=0  \
  --seed=1706688635  \
  --max_write_buffer_number=8 \
  --max_background_flushes=8 \
  > "./test_dir1/res_write/${th}_$(date +"%T")"
done