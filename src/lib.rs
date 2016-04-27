#![feature(slice_bytes)]

extern crate byteorder;
extern crate memmap;

use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian, BigEndian};
use memmap::{Mmap, Protection};

use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom, Cursor};
use std::mem::{size_of};
use std::path::{Path, PathBuf};
use std::slice::bytes::{copy_memory};

const BIG_MAGIC:    u64   = 0x5641_5252_4159_4442;
const VERSION:      u64   = 0x01;
const CHUNK_HEADER: usize = 16;
const CHUNK_SIZE:   usize = 64 * 1024 * 1024;

struct BufChunk {
  data:     Mmap,
  count:    usize,
  used_sz:  usize,
}

struct IndexEntry {
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
      Err(e) => panic!("failed to create buffer file: {:?}", e),
      Ok(file) => file,
    };

    let mut index_path = PathBuf::from(prefix);
    index_path.set_extension("varraydb_index");

    let mut index_file = match OpenOptions::new()
      .read(true).write(false).create(false).truncate(false)
      .open(&index_path)
    {
      Err(e) => panic!("failed to create index file: {:?}", e),
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
    for _ in 0 .. entries_count {
      let chunk_idx = index_file.read_u64::<LittleEndian>().unwrap() as usize;
      let chunk_offset = index_file.read_u64::<LittleEndian>().unwrap() as usize;
      let value_size = index_file.read_u64::<LittleEndian>().unwrap() as usize;
      index_entries.push(IndexEntry{
        chunk_idx:      chunk_idx,
        chunk_offset:   chunk_offset,
        value_size:     value_size,
      });
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
    let mut buf = vec![];
    for idx in start_idx .. end_idx {
      let value = self.get(idx);
      buf.clear();
      buf.extend_from_slice(value);
    }
  }

  pub fn get(&mut self, idx: usize) -> &[u8] {
    let entry = &self.index_entries[idx];
    unsafe { &self.buf_chunks[entry.chunk_idx].data.as_slice()[CHUNK_HEADER + entry.chunk_offset .. CHUNK_HEADER + entry.chunk_offset + entry.value_size] }
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
    let curr_entries_count = self.index_entries.len();

    let mut curr_chunk = &mut self.buf_chunks[curr_chunk_idx];
    let curr_offset = curr_chunk.used_sz;

    curr_chunk.count += 1;
    curr_chunk.used_sz += value.len();

    {
      let mut chunk_head = Cursor::new(unsafe { &mut curr_chunk.data.as_mut_slice()[ .. CHUNK_HEADER] });
      chunk_head.write_u64::<LittleEndian>(curr_chunk.count as u64).unwrap();
      chunk_head.write_u64::<LittleEndian>(curr_chunk.used_sz as u64).unwrap();
    }

    copy_memory(
        value,
        unsafe { &mut curr_chunk.data.as_mut_slice()[CHUNK_HEADER + curr_offset .. CHUNK_HEADER + curr_offset + value.len()] },
    );

    self.index_entries.push(IndexEntry{
      chunk_idx:    curr_chunk_idx,
      chunk_offset: curr_offset,
      value_size:   value.len(),
    });
    self.index_file.seek(
        SeekFrom::Start(4 * size_of::<u64>() as u64),
    ).unwrap();
    self.index_file.write_u64::<LittleEndian>(
        (curr_entries_count + 1) as u64,
    ).unwrap();
    self.index_file.seek(
        SeekFrom::Start(((5 + 3 * curr_entries_count) * size_of::<u64>()) as u64),
    ).unwrap();
    self.index_file.write_u64::<LittleEndian>(curr_chunk_idx as u64).unwrap();
    self.index_file.write_u64::<LittleEndian>(curr_offset as u64).unwrap();
    self.index_file.write_u64::<LittleEndian>(value.len() as u64).unwrap();
  }
}
