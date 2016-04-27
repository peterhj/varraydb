extern crate varraydb;

use varraydb::{VarrayDb};

use std::env;
use std::path::{PathBuf};

fn main() {
  let args: Vec<_> = env::args().collect();
  let in_prefix = PathBuf::from(&args[1]);
  let out_prefix = PathBuf::from(&args[2]);
  let num_partitions: i32 = args[3].parse().unwrap();
  assert!(num_partitions >= 1);
  let mut in_db = VarrayDb::open(&in_prefix).unwrap();
  in_db.shuffle(&out_prefix, num_partitions as usize);
}
