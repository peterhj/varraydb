use super::{IndexEntry};
use sharedmem::{MemoryMap, SharedMem, SharedSlice};

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

pub struct SharedBufChunk {
  data:     SharedMem<u8>,
  count:    usize,
  used_sz:  usize,
}

pub struct SharedVarrayDb {
  buf_path:     PathBuf,
  index_path:   PathBuf,
  buf_chunks:       Vec<SharedBufChunk>,
  index_entries:    Vec<IndexEntry>,
}

impl SharedVarrayDb {
  pub fn open(prefix: &Path) -> Result<SharedVarrayDb, ()> {
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
      //let data = match Mmap::open_with_offset(
      let data = match MemoryMap::open_with_offset(
          buf_file.try_clone().unwrap(),
          //Protection::Read,
          chunk_idx * CHUNK_SIZE,
          CHUNK_SIZE)
      {
        Err(e) => panic!("failed to memmap chunk for reading: {:?}", e),
        Ok(map) => map,
      };
      let (count, used_sz) = {
        let mut chunk_head = Cursor::new(unsafe { &(*data)[ .. CHUNK_HEADER] });
        let count = chunk_head.read_u64::<LittleEndian>().unwrap() as usize;
        let used_sz = chunk_head.read_u64::<LittleEndian>().unwrap() as usize;
        (count, used_sz)
      };
      buf_chunks.push(SharedBufChunk{
        data:       SharedMem::new(data),
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

    Ok(SharedVarrayDb{
      buf_path:     buf_path,
      index_path:   index_path,
      buf_chunks:       buf_chunks,
      index_entries:    index_entries,
    })
  }

  pub fn len(&self) -> usize {
    self.index_entries.len()
  }

  pub fn get(&mut self, idx: usize) -> SharedSlice<u8> {
    let entry = &self.index_entries[idx];
    self.buf_chunks[entry.chunk_idx].data.slice((CHUNK_HEADER + entry.chunk_offset), (CHUNK_HEADER + entry.chunk_offset + entry.value_size))
  }
}
