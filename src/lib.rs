extern crate byteorder;
extern crate memmap;
extern crate rng;
extern crate sharedmem;

extern crate rand;

pub mod shared;

use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian, BigEndian};
use memmap::{Mmap, Protection};
use rng::xorshift::{Xorshiftplus128Rng};

use rand::{Rng, thread_rng};
use std::cmp::{min};
use std::collections::{BinaryHeap};
use std::fs::{File, OpenOptions, remove_file};
use std::io::{Read, Write, Seek, SeekFrom, Cursor};
use std::mem::{size_of};
use std::path::{Path, PathBuf};
use std::ptr::{read_volatile};

const BIG_MAGIC:    u64   = 0x5641_5252_4159_4442;
const VERSION:      u64   = 0x01;
const CHUNK_HEADER: usize = 16;
const CHUNK_SIZE:   usize = 64 * 1024 * 1024;
const PAGE_SIZE:    usize = 4 * 1024;

struct BufChunk {
  data:     Mmap,
  count:    usize,
  used_sz:  usize,
}

pub struct IndexEntry {
  chunk_idx:    usize,
  chunk_offset: usize,
  value_size:   usize,
}

pub struct VarrayDb {
  buf_path:     PathBuf,
  buf_file:     File,
  index_path:   PathBuf,
  index_file:   File,

  read_only:    bool,

  buf_chunks:       Vec<BufChunk>,
  index_entries:    Vec<IndexEntry>,
}

impl Drop for VarrayDb {
  fn drop(&mut self) {
    let last_chunk_idx = self.buf_chunks.len()-1;
    self.buf_chunks[last_chunk_idx].data.flush().unwrap();
  }
}

impl VarrayDb {
  pub fn create(prefix: &Path) -> Result<VarrayDb, ()> {
    let buf_path = PathBuf::from(prefix);
    assert_eq!("varraydb", buf_path.extension().unwrap().to_str().unwrap());

    let buf_file = match OpenOptions::new()
      .read(true).write(true).create(true).truncate(true)
      .open(&buf_path)
    {
      Err(e) => panic!("failed to create buffer file: {:?}", e),
      Ok(file) => file,
    };

    let mut index_path = PathBuf::from(prefix);
    index_path.set_extension("varraydb_index");

    let mut index_file = match OpenOptions::new()
      .read(true).write(true).create(true).truncate(true)
      .open(&index_path)
    {
      Err(e) => panic!("failed to create index file: {:?}", e),
      Ok(file) => file,
    };
    index_file.write_u64::<BigEndian>(BIG_MAGIC).unwrap();
    index_file.write_u64::<LittleEndian>(VERSION).unwrap();
    index_file.write_u64::<LittleEndian>(CHUNK_SIZE as u64).unwrap();
    index_file.write_u64::<LittleEndian>(0).unwrap();
    index_file.write_u64::<LittleEndian>(0).unwrap();

    Ok(VarrayDb{
      buf_path:     buf_path,
      buf_file:     buf_file,
      index_path:   index_path,
      index_file:   index_file,
      read_only:    false,
      buf_chunks:       vec![],
      index_entries:    vec![],
    })
  }

  pub fn open(prefix: &Path) -> Result<VarrayDb, ()> {
    let buf_path = PathBuf::from(prefix);
    assert_eq!("varraydb", buf_path.extension().unwrap().to_str().unwrap());

    let buf_file = match OpenOptions::new()
      .read(true).write(false).create(false).truncate(false)
      .open(&buf_path)
    {
      Err(e) => panic!("failed to open buffer file: {:?}", e),
      Ok(file) => file,
    };

    let mut index_path = PathBuf::from(prefix);
    index_path.set_extension("varraydb_index");

    let mut index_file = match OpenOptions::new()
      .read(true).write(false).create(false).truncate(false)
      .open(&index_path)
    {
      Err(e) => panic!("failed to open index file: {:?}", e),
      Ok(file) => file,
    };

    let magic = index_file.read_u64::<BigEndian>().unwrap();
    assert_eq!(BIG_MAGIC, magic);
    let version = index_file.read_u64::<LittleEndian>().unwrap();
    assert_eq!(VERSION, version);
    let chunk_cap = index_file.read_u64::<LittleEndian>().unwrap() as usize;
    assert_eq!(CHUNK_SIZE, chunk_cap);
    let chunks_count = index_file.read_u64::<LittleEndian>().unwrap() as usize;
    let entries_count = index_file.read_u64::<LittleEndian>().unwrap() as usize;

    let mut buf_chunks = vec![];
    for chunk_idx in 0 .. chunks_count {
      let data = match Mmap::open_with_offset(
          &buf_file,
          Protection::Read,
          chunk_idx * CHUNK_SIZE,
          CHUNK_SIZE)
      {
        Err(e) => panic!("failed to memmap chunk for reading: {:?}", e),
        Ok(map) => map,
      };
      let (count, used_sz) = {
        let mut chunk_head = Cursor::new(unsafe { &data.as_slice()[ .. CHUNK_HEADER] });
        let count = chunk_head.read_u64::<LittleEndian>().unwrap() as usize;
        let used_sz = chunk_head.read_u64::<LittleEndian>().unwrap() as usize;
        (count, used_sz)
      };
      buf_chunks.push(BufChunk{
        data:       data,
        count:      count,
        used_sz:    used_sz,
      });
    }

    let mut index_entries = vec![];
    for i in 0 .. entries_count {
      let chunk_idx = index_file.read_u64::<LittleEndian>().unwrap() as usize;
      let chunk_offset = index_file.read_u64::<LittleEndian>().unwrap() as usize;
      let value_size = index_file.read_u64::<LittleEndian>().unwrap() as usize;
      index_entries.push(IndexEntry{
        chunk_idx:      chunk_idx,
        chunk_offset:   chunk_offset,
        value_size:     value_size,
      });
      if i >= 1 {
        if index_entries[i-1].chunk_idx == chunk_idx {
          assert_eq!(index_entries[i-1].chunk_offset + index_entries[i-1].value_size, chunk_offset);
        }
      }
    }

    Ok(VarrayDb{
      buf_path:     buf_path,
      buf_file:     buf_file,
      index_path:   index_path,
      index_file:   index_file,
      read_only:    true,
      buf_chunks:       buf_chunks,
      index_entries:    index_entries,
    })
  }

  pub fn len(&self) -> usize {
    self.index_entries.len()
  }

  pub fn prefetch(&mut self) {
    let length = self.len();
    self.prefetch_range(0, length);
  }

  pub fn prefetch_range(&mut self, start_idx: usize, end_idx: usize) {
    assert!(start_idx <= end_idx);
    let start_entry = &self.index_entries[start_idx];
    let last_entry = &self.index_entries[end_idx-1];
    //unsafe { &self.buf_chunks[entry.chunk_idx].data.as_slice()[(CHUNK_HEADER + entry.chunk_offset) .. (CHUNK_HEADER + entry.chunk_offset + entry.value_size)] }
    let start_chunk_idx = start_entry.chunk_idx;
    let last_chunk_idx = last_entry.chunk_idx;
    assert!(start_chunk_idx <= last_chunk_idx);
    for chunk_idx in start_chunk_idx .. last_chunk_idx + 1 {
      for p in 0 .. CHUNK_SIZE / PAGE_SIZE {
        let offset = (p * PAGE_SIZE) as isize;
        let _  = unsafe { read_volatile(self.buf_chunks[chunk_idx].data.as_slice().as_ptr().offset(offset)) };
      }
    }
    /*let mut sz = 0;
    let mut buf = vec![];
    for idx in start_idx .. end_idx {
      let value = self.get(idx);
      buf.clear();
      buf.extend_from_slice(value);
      sz += buf.len();
    }
    assert!(sz >= 0);
    sz*/
  }

  pub fn external_shuffle(&mut self, out_prefix: &Path, num_partitions: usize) {
    let mut rng = Xorshiftplus128Rng::new(&mut thread_rng());

    // Create a total shuffled order.
    println!("DEBUG: shuffle: create total order");
    let length = self.len();
    let mut shuf_idxs: Vec<_> = (0 .. length).collect();
    rng.shuffle(&mut shuf_idxs);
    {
      let mut idxs_path = PathBuf::from(out_prefix);
      let idxs_filename = format!(".tmp_shuf_idxs.{}", out_prefix.file_name().unwrap().to_str().unwrap());
      idxs_path.set_file_name(idxs_filename);
      let mut idxs_file = File::create(&idxs_path).unwrap();
      for &idx in shuf_idxs.iter() {
        writeln!(&mut idxs_file, "{}", idx);
      }
    }

    self.external_shuffle_with_order(out_prefix, num_partitions, &shuf_idxs);
  }

  pub fn external_shuffle_with_order(&mut self, out_prefix: &Path, num_partitions: usize, shuf_idxs: &[usize]) {
    let length = self.len();

    let cache_flush_limit = 64 * 1024 * 1024;
    let mut write_cache_size = 0;
    let mut write_cache = vec![];
    let mut buf = vec![];

    // Create and shuffle a set of temporary partition databases.
    println!("DEBUG: shuffle: create partitions");
    let padded_part_len = (length + num_partitions - 1) / num_partitions;
    let mut part_paths = vec![];
    let mut part_bounds = vec![];
    let mut part_idx_ptrs_list = vec![];
    let mut part_tmp_dbs = vec![];
    for part in 0 .. num_partitions {
      println!("DEBUG: shuffle: create partition: {}", part);
      let mut part_path = PathBuf::from(out_prefix);
      if num_partitions > 1 {
        let part_filename = format!(".tmp_shuf_{}.{}", part, out_prefix.file_name().unwrap().to_str().unwrap());
        part_path.set_file_name(part_filename);
      }
      part_paths.push(part_path);

      let start_i = part * padded_part_len;
      let end_i = min(length, (part+1) * padded_part_len);
      part_bounds.push((start_i, end_i));

      // Order the elements in the current partition by their indices in the
      // total shuffled order.
      let mut part_shuf_idxs: Vec<_> = (&shuf_idxs[start_i .. end_i])
        .iter()
        .enumerate()
        .map(|(k, &idx)| (idx, k))
        .collect();
      part_shuf_idxs.sort();

      // Also save the relative position of shuffled indices. Convert the
      // indices into _negative integers_ because later we use a max-heap.
      let part_idx_ptrs: Vec<_> = part_shuf_idxs
        .iter()
        .enumerate()
        .map(|(j, &(idx, _))| (-(idx as isize), j))
        .collect();
      part_idx_ptrs_list.push(part_idx_ptrs);

      /*println!("DEBUG: shuffle: prefetch: {}", part);
      let pref_sz = self.prefetch_range(start_i, end_i);
      println!("DEBUG: shuffle: prefetched sz: {}", pref_sz);*/

      println!("DEBUG: shuffle: append: {}", part);
      let mut part_tmp_db = VarrayDb::create(&part_paths[part]).unwrap();
      let part_len = end_i - start_i;
      for &(_, k) in part_shuf_idxs.iter() {
        assert!(k < part_len);
        let i = start_i + k;
        //part_tmp_db.append(self.get(i));
        let value = self.get(i);
        buf.clear();
        buf.extend_from_slice(value);
        write_cache_size += buf.len();
        write_cache.push(buf.clone());
        if write_cache_size >= cache_flush_limit{
          for buf in write_cache.drain(..) {
            part_tmp_db.append(&buf);
          }
          write_cache_size = 0;
        }
      }
      for buf in write_cache.drain(..) {
        part_tmp_db.append(&buf);
      }
      write_cache_size = 0;
      part_tmp_db.flush();
      part_tmp_dbs.push(part_tmp_db);
    }

    if num_partitions == 1 {
      return;
    }

    // Create an empty output database.
    println!("DEBUG: shuffle: create output db");
    // FIXME(20160426): with capacity?
    let mut out_db = VarrayDb::create(&out_prefix).unwrap();

    // Set the merge traversal order.
    println!("DEBUG: shuffle: create merge heap");
    let mut idx_ptr_heap = BinaryHeap::new();
    for part in 0 .. num_partitions {
      for &(neg_idx, j) in part_idx_ptrs_list[part].iter() {
        idx_ptr_heap.push((neg_idx, part, j));
      }
    }

    // Initialize the merging by building initial blocks from the partitions.
    println!("DEBUG: shuffle: init merge");
    let mut part_block_counters = vec![];
    for part in 0 .. num_partitions {
      part_block_counters.push(0);
    }
    /*let num_blocks = num_partitions;
    let mut part_block_bounds = vec![];
    let mut part_block_counters = vec![];
    for part in 0 .. num_partitions {
      let (start_i, end_i) = part_bounds[part];
      let part_len = end_i - start_i;
      let block_len = (part_len + num_blocks - 1) / num_blocks;

      let (block_start_j, block_end_j) = (0, block_len);
      part_block_bounds.push((block_start_j, block_end_j));
      part_block_counters.push(0);

      part_tmp_dbs[part].prefetch_range(block_start_j, block_end_j);
    }*/

    // Merge the temporary partition databases and flush the results to the
    // output database.
    println!("DEBUG: shuffle: begin merge");
    while !idx_ptr_heap.is_empty() {
      let (_, part, j) = idx_ptr_heap.pop().unwrap();
      assert_eq!(j, part_block_counters[part]);

      /*let (pre_block_start_j, pre_block_end_j) = part_block_bounds[part];
      if j >= pre_block_end_j {
        let (start_i, end_i) = part_bounds[part];
        let part_len = end_i - start_i;
        let block_len = (part_len + num_blocks - 1) / num_blocks;

        let post_block_start_j = pre_block_end_j;
        let post_block_end_j = min(part_len, pre_block_end_j + block_len);
        part_block_bounds[part] = (post_block_start_j, post_block_end_j);

        part_tmp_dbs[part].prefetch_range(post_block_start_j, post_block_end_j);
      }

      let (block_start_j, block_end_j) = part_block_bounds[part];
      assert!(j >= block_start_j);
      assert!(j < block_end_j);*/

      let value = part_tmp_dbs[part].get(j);
      buf.clear();
      buf.extend_from_slice(value);
      write_cache_size += buf.len();
      write_cache.push(buf.clone());

      if write_cache_size >= cache_flush_limit {
        for buf in write_cache.drain(..) {
          out_db.append(&buf);
        }
        write_cache_size = 0;
      }

      part_block_counters[part] += 1;
    }
    for buf in write_cache.drain(..) {
      out_db.append(&buf);
    }
    write_cache_size = 0;
    out_db.flush();

    // Remove the temporary partition databases.
    println!("DEBUG: shuffle: done merge");
    part_tmp_dbs.clear();
    for part in 0 .. num_partitions {
      remove_file(&part_paths[part]).unwrap();
      // FIXME(20160427): remove index files too.
    }
  }

  pub fn get_chunk_mem(&self, chunk_idx: usize) -> &[u8] {
    unsafe { &self.buf_chunks[chunk_idx].data.as_slice()[CHUNK_HEADER ..] }
  }

  pub fn get_entry(&self, idx: usize) -> &IndexEntry {
    let entry = &self.index_entries[idx];
    entry
  }

  pub fn get(&mut self, idx: usize) -> &[u8] {
    let entry = &self.index_entries[idx];
    unsafe { &self.buf_chunks[entry.chunk_idx].data.as_slice()[(CHUNK_HEADER + entry.chunk_offset) .. (CHUNK_HEADER + entry.chunk_offset + entry.value_size)] }
  }

  pub fn append(&mut self, value: &[u8]) {
    assert!(!self.read_only);
    assert!(value.len() <= CHUNK_SIZE - CHUNK_HEADER);

    let mut expand = false;
    if self.buf_chunks.is_empty() {
      expand = true;
    } else if self.buf_chunks[self.buf_chunks.len()-1].used_sz + value.len() > CHUNK_SIZE - CHUNK_HEADER {
      expand = true;
      let last_chunk_idx = self.buf_chunks.len()-1;
      self.buf_chunks[last_chunk_idx].data.flush().unwrap();
    }

    if expand {
      let old_size = self.buf_chunks.len() * CHUNK_SIZE;
      let new_size = (self.buf_chunks.len() + 1) * CHUNK_SIZE;
      match self.buf_file.set_len(new_size as u64) {
        Err(e) => panic!("failed to expand buffer file: {:?}", e),
        Ok(_) => {}
      }
      let mut data = match Mmap::open_with_offset(
          &self.buf_file,
          Protection::ReadWrite,
          old_size,
          CHUNK_SIZE)
      {
        Err(e) => panic!("failed to map new chunk: {:?}", e),
        Ok(map) => map,
      };
      {
        let mut chunk_head = Cursor::new(unsafe { &mut data.as_mut_slice()[ .. CHUNK_HEADER] });
        chunk_head.write_u64::<LittleEndian>(0).unwrap();
        chunk_head.write_u64::<LittleEndian>(0).unwrap();
      }
      data.flush();

      self.buf_chunks.push(BufChunk{
        data:     data,
        count:    0,
        used_sz:  0,
      });

      self.index_file.seek(
          SeekFrom::Start(3 * size_of::<u64>() as u64),
      ).unwrap();
      self.index_file.write_u64::<LittleEndian>(
          self.buf_chunks.len() as u64,
      ).unwrap();
    }

    let curr_chunk_idx = self.buf_chunks.len() - 1;
    let pre_entries_count = self.index_entries.len();

    let mut curr_chunk = &mut self.buf_chunks[curr_chunk_idx];
    let curr_offset = curr_chunk.used_sz;

    curr_chunk.count += 1;
    curr_chunk.used_sz += value.len();

    {
      let mut chunk_head = Cursor::new(unsafe { &mut curr_chunk.data.as_mut_slice()[ .. CHUNK_HEADER] });
      chunk_head.write_u64::<LittleEndian>(curr_chunk.count as u64).unwrap();
      chunk_head.write_u64::<LittleEndian>(curr_chunk.used_sz as u64).unwrap();
    }

    (unsafe { &mut curr_chunk.data.as_mut_slice()[(CHUNK_HEADER + curr_offset) .. (CHUNK_HEADER + curr_offset + value.len())] })
      .copy_from_slice(value);

    self.index_entries.push(IndexEntry{
      chunk_idx:    curr_chunk_idx,
      chunk_offset: curr_offset,
      value_size:   value.len(),
    });

    self.index_file.seek(
        SeekFrom::Start(4 * size_of::<u64>() as u64),
    ).unwrap();
    self.index_file.write_u64::<LittleEndian>(
        (pre_entries_count + 1) as u64,
    ).unwrap();

    self.index_file.seek(
        SeekFrom::Start(((5 + 3 * pre_entries_count) * size_of::<u64>()) as u64),
    ).unwrap();
    self.index_file.write_u64::<LittleEndian>(curr_chunk_idx as u64).unwrap();
    self.index_file.write_u64::<LittleEndian>(curr_offset as u64).unwrap();
    self.index_file.write_u64::<LittleEndian>(value.len() as u64).unwrap();
  }

  pub fn flush(&mut self) {
    if !self.buf_chunks.is_empty() {
      let last_chunk_idx = self.buf_chunks.len()-1;
      self.buf_chunks[last_chunk_idx].data.flush().unwrap();
    }
  }
}
