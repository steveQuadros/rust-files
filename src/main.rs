use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter, SeekFrom};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Foo<'a> {
    size: u64,
    msg: &'a [u8],
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Bar {
    size: u32,
    msg: String,
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct BufReadWrite {
    w: BufWriter<File>,
    r: BufReader<File>,
}

fn main() -> std::io::Result<()> {
    let mut f = File::create("foo.txt")?;
    let s = "Hello, my world!";
    println!("{}", s.len());
    let foo = Foo {
        size: s.len() as u64,
        msg: &s.as_bytes(),
    };
    // let options = bincode::DefaultOptions::new();
    let encoded = bincode::serialize(&foo);
    println!("{:?}", encoded);
    f.write_all(&encoded.unwrap())?;
    f.sync_data()?;

    let mut f = File::create("bar.txt")?;
    let s = "Hello, my world bar!";
    println!("{}", s.len());
    let bar = Bar {
        size: s.len() as u32,
        msg: s.to_string(),
    };
    // let options = bincode::DefaultOptions::new();
    let encoded = bincode::serialize(&bar);
    println!("{:?}", encoded);
    f.write_all(&encoded.unwrap())?;
    f.sync_data()?;

    Ok(())
}

// we are not writing the file correctly :Qa
fn my_write(path: &str, data: &[u8], from: u64) -> Result<(u64, u64)> {
    println!("{:?}, {}", data, data.len());
    let mut file = File::create(path)?;
    let msg_size = data.len() as u64;
    let record = Foo {
        size: msg_size,
        msg: data,
    };
    let encoded = bincode::serialize(&record)?;
    file.seek(SeekFrom::Start(from))?;
    let written_count = file.write(&encoded)?;
    file.sync_data()?;

    let new_offset = msg_size + 8;
    Ok((written_count as u64, new_offset))
}

// fn read(path: &str, from: u64) -> Result<String> {
//     let mut file = File::create(path)?;
//     file.seek(SeekFrom::Start(from))?;
//     let mut msg_length_bytes: Vec<u8>;
//     file.read_exact(& mut msg_length_bytes)?;
//     let msg_length =
//     file.seek(SeekFrom::Start(from + msg_length))?;
//     file.read_exact(
// }

// read and write to file from threads in rust
//   - https://stackoverflow.com/questions/65235821/how-do-i-write-to-a-file-from-different-threads-in-rust

struct Store<'a> {
    file: &'a std::fs::File,
    r: BufReader<&'a File>,
    w: BufWriter<&'a File>,
    size: u64,
}

impl<'a> Store<'a> {
    fn new(f: &'a File) -> Self {
        Store {
            file: f,
            r: BufReader::new(f),
            w: BufWriter::new(f),
            size: 0,
        }
    }

    fn append(&mut self, d: &[u8]) -> std::result::Result<(u64, u64), std::io::Error> {
        let res = self.w.write(d);
        match res {
            Ok(byte_count) => {
                let start = self.size;
                self.size += byte_count as u64;
                Ok((start, byte_count as u64))
            }
            Err(e) => Err(e),
        }
    }

    fn read(&mut self, offset: u64, into: &mut [u8]) -> std::io::Result<usize> {
        self.w.flush()?;
        self.r.seek(SeekFrom::Start(offset))?;
        self.r.read(into)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write() {
        let filename = String::from("writetest");
        let mut offset = 0;
        let to_write = vec!["foo", "barbar", "bazquok"];
        for s in to_write {
            let write_res = my_write(&filename, s.as_bytes(), offset);
            offset += match write_res {
                Ok((o, _)) => o,
                Err(err) => panic!("{}", err),
            };
        }

        // need to read the file back using the offsets above and make sure it's writing properly

        let msg = format!("error removing file: {}", filename);
        std::fs::remove_file(filename).expect(&msg);
    }

    #[test]
    fn store() {
        let filename = "storetest";
        let f = File::options()
            .create(true)
            .append(true)
            .read(true)
            .open(filename)
            .unwrap();
        let mut store = Store::new(&f);
        let s = b"test";
        let (offset, c) = store.append(s).unwrap();
        let mut buf: Vec<u8> = vec![0; c as usize];
        let num_read = store.read(offset, &mut buf).unwrap();
        assert_eq!(buf, b"test");
        assert_eq!(num_read, 4);
        std::fs::remove_file(filename).unwrap();
    }
}
