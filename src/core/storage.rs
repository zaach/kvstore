use bytes::Bytes;
use lru::LruCache;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use thiserror;
use crate::util;

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct AsStdError(#[from] anyhow::Error);

type Error = anyhow::Error;

#[derive(Serialize, Deserialize, Debug)]
struct DataFileEntry {
    tstamp: i64,
    key: Bytes,
    value: Option<Bytes>,
}

struct LogIndex {
    len: u64,
    pos: u64,
}

trait LogWriter {
    fn append<T: Serialize>(&mut self, entry: &T) -> Result<LogIndex, Error>;
}

struct FileLogWriter {
    writer: File,
}
impl FileLogWriter {
    fn new(path: &str) -> Result<FileLogWriter, Error> {
        let writer = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        println!("FileLogWriter::new: path: {:?}", path);
        Ok(FileLogWriter { writer })
    }
}
impl LogWriter for FileLogWriter {
    fn append<T: Serialize>(&mut self, entry: &T) -> Result<LogIndex, Error> {
        println!("Serialized size: {:?}", bincode::serialized_size(entry)?);
        let buf = bincode::serialize(entry)?;
        println!("buf: {:?}", buf);
        let len = buf.len() as u64;
        println!("Writing size: {:?}", len);
        self.writer.write_all(&buf)?;
        self.writer.flush()?;
        let pos = self.writer.seek(SeekFrom::Current(0))? - len as u64;
        Ok(LogIndex {
            len: len as u64,
            pos,
        })
    }
}

trait LogReader {
    unsafe fn at<T: DeserializeOwned>(&mut self, len: u64, pos: u64) -> Result<T, Error>;
}

struct FileLogReader {
    reader: File,
}
impl FileLogReader {
    fn new(path: &str) -> Result<FileLogReader, Error> {
        println!("FileLogReader::new: path: {:?}", path);
        let reader = std::fs::OpenOptions::new().read(true).open(path)?;
        Ok(FileLogReader { reader })
    }
}
impl LogReader for FileLogReader {
    unsafe fn at<T: DeserializeOwned>(&mut self, len: u64, pos: u64) -> Result<T, Error> {
        println!("FileLogReader::at: len: {:?}, pos: {:?}", len, pos);
        self.reader.seek(SeekFrom::Start(pos))?;
        let mut buf = vec![0; len as usize];
        println!("buf len: {:?}", buf.len());
        self.reader.read_exact(&mut buf)?;
        println!("buf: {:?}", buf);
        Ok(bincode::deserialize(&buf)?)
    }
}

struct LogDir<T: LogReader = FileLogReader>(LruCache<u64, T>);

impl LogDir<FileLogReader> {
    unsafe fn read<T, P>(&mut self, path: P, fileid: u64, len: u64, pos: u64) -> Result<T, Error>
    where
        T: DeserializeOwned,
        P: AsRef<Path>,
    {
        let opt = self.0.get_mut(&fileid);
        let reader = if opt.is_none() {
            let reader =
                FileLogReader::new(path.as_ref().join(fileid.to_string()).to_str().unwrap())?;
            self.0.put(fileid, reader);
            self.0.get_mut(&fileid).unwrap()
        } else {
            opt.unwrap()
        };
        reader.at(len, pos)
    }
}

type KeyDir = HashMap<Bytes, KeyDirEntry>;

#[derive(Clone, Debug)]
struct KeyDirEntry {
    fileid: u64,
    len: u64,
    pos: u64,
    tstamp: i64,
}

pub trait KeyValueStorage: 'static {
    type Error: std::error::Error;

    fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), Self::Error>;

    fn get(&mut self, key: Bytes) -> Result<Option<Bytes>, Self::Error>;

    fn del(&mut self, key: Bytes) -> Result<bool, Self::Error>;
}

pub struct KVContext {
    logdir: LogDir,
    keydir: KeyDir,
    writer: FileLogWriter,
    fileid: u64,
    path: PathBuf,
}

const MAX_READER_CACHE = std::num::NonZeroUsize::new(1024).unwrap();

impl KVContext {
    pub fn new() -> Result<KVContext, Error> {
        let mut path = std::env::current_dir()?;
        path.push(".kvstore/data");
        // create directory if not exists
        std::fs::create_dir_all(&path)?;
        println!("path: {:?}", path);
        let fileid = 0;
        let filepath = path.clone().join(fileid.to_string());
        let writer = FileLogWriter::new(filepath.to_str().unwrap())?;
        let keydir = HashMap::new();
        let limit = MAX_READER_CACHE;
        let logdir = LogDir(LruCache::new(limit));

        Ok(KVContext {
            logdir,
            keydir,
            writer,
            fileid,
            path,
        })
    }

    pub fn from_dir(data_dir: Option<&str>) -> Result<KVContext, Error> {
        let path = if let Some(p) = data_dir {
            // normalize relative path
            util::normalize_path(p)
        } else {
            std::env::current_dir()?.join(".kvstore/data")
        };
        println!("path: {:?}", path);
        std::fs::create_dir_all(&path)?;
        let mut fileid = 0;
        let mut keydir = HashMap::new();
        let mut files = std::fs::read_dir(&path)?;
        while let Some(file) = files.next() {
            let file = file?;
            let p = file.path();
            let parsed_filename = p.file_name().unwrap().to_str().unwrap().parse::<u64>();
            if let Ok(id) = parsed_filename {
                if id > fileid {
                    fileid = id;
                }
            } else {
                continue;
            }
            let mut bufreader = std::io::BufReader::new(std::fs::File::open(p)?);
            while let Ok(entry) = bincode::deserialize_from::<_, DataFileEntry>(&mut bufreader) {
                let len = bincode::serialized_size(&entry)?;
                let key = entry.key;
                let value = entry.value;
                if value.is_none() {
                    keydir.remove(&key);
                    continue;
                }
                let tstamp = entry.tstamp;
                let pos = bufreader.seek(SeekFrom::Current(0))?;
                let pos = pos - len;
                keydir.insert(
                    key,
                    KeyDirEntry {
                        fileid,
                        len,
                        pos,
                        tstamp,
                    },
                );
            }
        }

        println!("fileid: {:?}", fileid);
        println!("keydir: {:?}", keydir);

        let filepath = path.clone().join(fileid.to_string());
        let writer = FileLogWriter::new(filepath.to_str().unwrap())?;

        let logdir = LogDir(LruCache::new(MAX_READER_CACHE));

        Ok(KVContext {
            logdir,
            keydir,
            writer,
            fileid,
            path,
        })
    }

    // TODO copaction
}

impl KeyValueStorage for KVContext {
    type Error = AsStdError;

    // TODO increase fileid when file size is too big

    fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), Self::Error> {
        let tstamp = chrono::Utc::now().timestamp();
        let entry = DataFileEntry {
            tstamp,
            key: key.clone(),
            value: Some(value),
        };
        let index = self.writer.append(&entry)?;
        self.keydir.insert(
            key,
            KeyDirEntry {
                fileid: self.fileid,
                len: index.len,
                pos: index.pos,
                tstamp,
            },
        );
        Ok(())
    }

    fn get(&mut self, key: Bytes) -> Result<Option<Bytes>, Self::Error> {
        let entry = self.keydir.get(&key);
        if entry.is_none() {
            return Ok(None);
        }
        let entry = entry.unwrap();
        let data: DataFileEntry = unsafe {
            self.logdir
                .read(&self.path, entry.fileid, entry.len, entry.pos)?
        };
        Ok(data.value)
    }

    fn del(&mut self, key: Bytes) -> Result<bool, Self::Error> {
        let old_entry = self.keydir.get(&key);
        if old_entry.is_none() {
            return Ok(false);
        }
        let tstamp = chrono::Utc::now().timestamp();
        let entry = DataFileEntry {
            tstamp,
            key: key.clone(),
            value: None,
        };
        self.writer.append(&entry)?;
        self.keydir.remove(&key);
        Ok(true)
    }
}
