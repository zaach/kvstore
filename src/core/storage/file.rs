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
use super::KeyValueStorage;

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

const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024;

struct FileLogWriter {
    writer: File,
    fileid: u64,
    filepos: u64,
}
impl FileLogWriter {
    fn new(path: &str, fileid: u64, filepos: u64) -> Result<FileLogWriter, Error> {
        let writer = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(FileLogWriter {
            writer,
            fileid,
            filepos,
        })
    }

    fn append<T: Serialize>(&mut self, entry: &T) -> Result<LogIndex, Error> {
        let buf = bincode::serialize(entry)?;
        let len = buf.len() as u64;
        self.writer.write_all(&buf)?;
        self.writer.flush()?;
        let pos = self.writer.seek(SeekFrom::Current(0))? - len as u64;
        self.filepos = pos + len;
        Ok(LogIndex {
            len: len as u64,
            pos,
        })
    }
    fn serialized_size<T: Serialize>(entry: &T) -> Result<u64, Error> {
        Ok(bincode::serialized_size(entry)?)
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
    #[allow(dead_code)]
    tstamp: i64,
}

#[derive(Clone)]
pub struct KvContext {
    logdir: LogDir,
    keydir: Arc<RwLock<KeyDir>>,
    writer: Arc<Mutex<FileLogWriter>>,
    path: PathBuf,
}

const MAX_READER_CACHE: usize = 32;

impl KvContext {
    pub fn from_dir(data_dir: Option<PathBuf>) -> Result<Self, Error> {
        let path = if let Some(p) = data_dir {
            // normalize relative path
            util::normalize_path(p)
        } else {
            std::env::current_dir()?.join(".kvstore/data")
        };
        std::fs::create_dir_all(&path)?;
        let mut fileid = 0;
        let mut filepos = 0;
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
                let len = FileLogWriter::serialized_size(&entry)?;
                let key = entry.key;
                let value = entry.value;
                if value.is_none() {
                    keydir.remove(&key);
                    continue;
                }
                let tstamp = entry.tstamp;
                let pos = bufreader.seek(SeekFrom::Current(0))?;
                filepos = pos;
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
        let writer = Arc::new(Mutex::new(FileLogWriter::new(
            filepath.to_str().unwrap(),
            fileid,
            filepos,
        )?));

        let logdir = LogDir::new();

        Ok(Self {
            logdir,
            keydir: Arc::new(RwLock::new(keydir)),
            writer,
            path,
        })
    }

    fn write_entry(&self, writer: &mut FileLogWriter, entry: DataFileEntry) -> Result<(u64, u64, u64), Error> {
        let entry_size = FileLogWriter::serialized_size(&entry)?;
        if writer.filepos + entry_size > MAX_FILE_SIZE {
            *writer = FileLogWriter::new(
                self.path
                    .clone()
                    .join((writer.fileid + 1).to_string())
                    .to_str()
                    .unwrap(),
                writer.fileid + 1,
                0,
            )?;
        }
        let index = (*writer).append(&entry)?;
        Ok((writer.fileid, index.pos, index.len))
    }

    // TODO compaction
    // TODO hint files
}

impl KeyValueStorage for KvContext {
    type Error = AsStdError;

    fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), Self::Error> {
        let tstamp = chrono::Utc::now().timestamp();
        let entry = DataFileEntry {
            tstamp,
            key: key.clone(),
            value: Some(value),
        };

        let mut writer = self.writer.lock().unwrap();
        let (fileid, pos, len) = self.write_entry(&mut writer, entry)?;

        let mut keydir = self.keydir.write().unwrap();
        keydir.insert(
            key,
            KeyDirEntry {
                fileid,
                len,
                pos,
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
        self.write_entry(&mut writer, entry)?;
        let mut keydir = self.keydir.write().unwrap();
        keydir.remove(&key);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::path::PathBuf;

    #[test]
    fn it_gets_none_for_empty() {
        let mut kv = KvContext::from_dir(Some(PathBuf::from(".test/data"))).unwrap();

        assert_eq!(kv.get("empty".into()).unwrap(), None);
    }

    #[test]
    fn it_sets_and_gets() {
        let mut kv = KvContext::from_dir(Some(PathBuf::from(".test/data"))).unwrap();

        let value = Bytes::from("value");
        kv.set("key".into(), value).unwrap();
        assert_eq!(kv.get("key".into()).unwrap().unwrap(), Bytes::from("value"));

        kv.set("key".into(), Bytes::from("updated")).unwrap();
        assert_eq!(
            kv.get("key".into()).unwrap().unwrap(),
            Bytes::from("updated")
        );
    }

    #[test]
    fn it_del() {
        let mut kv = KvContext::from_dir(Some(PathBuf::from(".test/data"))).unwrap();

        kv.set("new key".into(), Bytes::from("new val")).unwrap();
        assert_eq!(
            kv.get("new key".into()).unwrap().unwrap(),
            Bytes::from("new val")
        );
        kv.del("new key".into()).unwrap();
        assert_eq!(kv.get("new key".into()).unwrap(), None);

        // remove the test directory
        let test_dir = std::env::current_dir().unwrap().join(".test/data");
        std::fs::remove_dir_all(test_dir).unwrap();
    }
}
