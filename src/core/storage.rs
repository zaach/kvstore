use crate::util;
use bytes::Bytes;
use lru::LruCache;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use thiserror;

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

struct FileLogWriter {
    writer: File,
}
impl FileLogWriter {
    fn new(path: &str) -> Result<FileLogWriter, Error> {
        let writer = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(FileLogWriter { writer })
    }

    fn append<T: Serialize>(&mut self, entry: &T) -> Result<LogIndex, Error> {
        let buf = bincode::serialize(entry)?;
        let len = buf.len() as u64;
        self.writer.write_all(&buf)?;
        self.writer.flush()?;
        let pos = self.writer.seek(SeekFrom::Current(0))? - len as u64;
        Ok(LogIndex {
            len: len as u64,
            pos,
        })
    }
}

struct FileLogReader {
    reader: File,
}
impl FileLogReader {
    fn new(path: &str) -> Result<FileLogReader, Error> {
        let reader = std::fs::OpenOptions::new().read(true).open(path)?;
        Ok(FileLogReader { reader })
    }

    // TODO memmapped reader
    unsafe fn at<T: DeserializeOwned>(&mut self, len: u64, pos: u64) -> Result<T, Error> {
        self.reader.seek(SeekFrom::Start(pos))?;
        let mut buf = vec![0; len as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(bincode::deserialize(&buf)?)
    }
}

struct LogDir(LruCache<u64, FileLogReader>);

impl LogDir {
    fn new() -> LogDir {
        LogDir(LruCache::new(
            std::num::NonZeroUsize::new(MAX_READER_CACHE).unwrap(),
        ))
    }
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
impl Clone for LogDir {
    fn clone(&self) -> Self {
        LogDir::new()
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

pub trait KeyValueStorage: Clone + Send + 'static {
    type Error: std::error::Error + Send + Sync;

    fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), Self::Error>;

    fn get(&mut self, key: Bytes) -> Result<Option<Bytes>, Self::Error>;

    fn del(&mut self, key: Bytes) -> Result<bool, Self::Error>;
}

#[derive(Clone)]
pub struct KVContext {
    logdir: LogDir,
    keydir: Arc<RwLock<KeyDir>>,
    writer: Arc<Mutex<FileLogWriter>>,
    fileid: u64,
    path: PathBuf,
}

const MAX_READER_CACHE: usize = 32;

impl KVContext {
    pub fn from_dir(data_dir: Option<PathBuf>) -> Result<KVContext, Error> {
        let path = if let Some(p) = data_dir {
            // normalize relative path
            util::normalize_path(p)
        } else {
            std::env::current_dir()?.join(".kvstore/data")
        };
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

        let filepath = path.clone().join(fileid.to_string());
        let writer = Arc::new(Mutex::new(FileLogWriter::new(filepath.to_str().unwrap())?));

        let logdir = LogDir::new();

        Ok(KVContext {
            logdir,
            keydir: Arc::new(RwLock::new(keydir)),
            writer,
            fileid,
            path,
        })
    }

    // TODO compaction
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
        let mut writer = self.writer.lock().unwrap();
        let index = (*writer).append(&entry)?;
        let mut keydir = self.keydir.write().unwrap();
        keydir.insert(
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
        let keydir = self.keydir.read().unwrap();
        let entry = keydir.get(&key);
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
        let keydir = self.keydir.read().unwrap();
        let old_entry = keydir.get(&key);
        if old_entry.is_none() {
            return Ok(false);
        }
        drop(keydir);
        let tstamp = chrono::Utc::now().timestamp();
        let entry = DataFileEntry {
            tstamp,
            key: key.clone(),
            value: None,
        };
        let mut writer = self.writer.lock().unwrap();
        (*writer).append(&entry)?;
        let mut keydir = self.keydir.write().unwrap();
        keydir.remove(&key);
        Ok(true)
    }
}
