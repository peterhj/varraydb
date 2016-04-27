extern crate varraydb;

use varraydb::{VarrayDb};

use std::env;
use std::fs::{File};
use std::io::{BufRead, BufReader};
use std::path::{PathBuf};

fn main() {
  let args: Vec<_> = env::args().collect();
  let in_prefix = PathBuf::from(&args[1]);
  let out_prefix = PathBuf::from(&args[2]);
  let num_partitions: usize = args[3].parse().unwrap();
  assert!(num_partitions >= 1);
  let shuf_idxs = if args.len() > 4 {
    let shuf_idxs_path = PathBuf::from(&args[4]);
    let shuf_idxs_file = File::open(&shuf_idxs_path).unwrap();
    let reader = BufReader::new(shuf_idxs_file);
    let mut shuf_idxs = vec![];
    for line in reader.lines() {
      let line = line.unwrap();
      let idx: usize = line.parse().unwrap();
      shuf_idxs.push(idx);
    }
    Some(shuf_idxs)
  } else {
    None
  };
  let mut in_db = VarrayDb::open(&in_prefix).unwrap();
  if let Some(ref shuf_idxs) = shuf_idxs {
    in_db.external_shuffle_with_order(&out_prefix, num_partitions, shuf_idxs);
  } else {
    in_db.external_shuffle(&out_prefix, num_partitions);
  }
}
