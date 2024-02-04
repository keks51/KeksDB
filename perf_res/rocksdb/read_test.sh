
threads=(1 4 8 16 32 50 100 160 200)

cd ./rocksdb/tools
for th in "${threads[@]}"
do

  nums=$((1000000 / th))
  ./db_bench \
  --benchmarks=filluniquerandom,readrandom \
  --use_existing_db=0 \
  --open_files=-1 \
  --num="$nums" \
  --numdistinct="$nums" \
  --memtablerep=skip_list \
  --threads="$th" \
  --compression_type=none \
  --key_size=12 \
  --value_size=500 \
  --db=./test_dir1/data/db \
  --wal_dir=./test_dir1/data/wal \
  --num_levels=8 \
  --level0_file_num_compaction_trigger=4 \
  --level0_slowdown_writes_trigger=20 \
  --level0_stop_writes_trigger=30 \
  --max_background_jobs=16 \
  --max_write_buffer_number=8 \
  --block_size=4194304 \
  --cache_size=0 \
  --cache_numshardbits=0 \
  --compression_max_dict_bytes=0 \
  --compression_ratio=0.5 \
  --bytes_per_sync=512000000 \
  --benchmark_write_rate_limit=0 \
  --write_buffer_size=0 \
  --target_file_size_base=134217728 \
  --max_bytes_for_level_base=1073741824 \
  --verify_checksum=1 \
  --delete_obsolete_files_period_micros=62914560 \
  --max_bytes_for_level_multiplier=8 \
  --statistics=0 \
  --stats_per_interval=1 \
  --report_interval_seconds=1 \
  --histogram=1 \
  --bloom_bits=10 \
  --subcompactions=1 \
  --compaction_style=0 \
  --min_level_to_compress=-1 \
  --level_compaction_dynamic_level_bytes=true \
  --seed=0 \
  --use_shared_block_and_blob_cache=0 \
  --cache_index_and_filter_blocks=false \
  --cache_high_pri_pool_ratio=0 \
  --pin_l0_filter_and_index_blocks_in_cache=0 \
  --read_cache_size=0 \
  > "./test_dir1/res_read/${th}_$(date +"%T")"

done
