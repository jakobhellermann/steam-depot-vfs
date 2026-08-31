//! Projects a tiny static tree and reads it back, exercising every
//! callback. Run: `cargo run -p projfs --example static_demo`

#![cfg(windows)]

use std::path::Path;

use projfs::{DirEntry, Metadata, ProjFS};

struct Demo;

const FILE_BODY: &[u8] = b"hello from projfs\n";

impl projfs::Filesystem for Demo {
    fn list_directory(&self, path: &Path) -> Vec<DirEntry> {
        if path.as_os_str().is_empty() {
            vec![
                DirEntry {
                    name: "sub".into(),
                    metadata: Metadata {
                        is_dir: true,
                        size: 0,
                        creation_time: 0,
                        last_write_time: 0,
                    },
                },
                DirEntry {
                    name: "hello.txt".into(),
                    metadata: Metadata {
                        is_dir: false,
                        size: FILE_BODY.len() as u64,
                        creation_time: 0,
                        last_write_time: 0,
                    },
                },
            ]
        } else {
            vec![]
        }
    }

    fn get_metadata(&self, path: &Path) -> Option<Metadata> {
        match path.to_str()? {
            "" => Some(Metadata {
                is_dir: true,
                size: 0,
                creation_time: 0,
                last_write_time: 0,
            }),
            "sub" => Some(Metadata {
                is_dir: true,
                size: 0,
                creation_time: 0,
                last_write_time: 0,
            }),
            "hello.txt" => Some(Metadata {
                is_dir: false,
                size: FILE_BODY.len() as u64,
                creation_time: 0,
                last_write_time: 0,
            }),
            _ => None,
        }
    }

    fn read_file(&self, path: &Path, offset: u64, length: u32) -> Result<Vec<u8>, std::io::Error> {
        if path.to_str() != Some("hello.txt") {
            return Ok(vec![]);
        }
        let start = (offset as usize).min(FILE_BODY.len());
        let end = (start + length as usize).min(FILE_BODY.len());
        Ok(FILE_BODY[start..end].to_vec())
    }
}

fn main() -> Result<(), std::io::Error> {
    let root = std::env::temp_dir().join("projfs_static_demo");
    let _ = std::fs::remove_dir_all(&root);

    let _pfs = ProjFS::new(&root, Demo).map_err(|e| {
        eprintln!(
            "PrjStartVirtualizing failed: {e}\n\
             Is ProjFS enabled? Run (admin):\n  \
             Enable-WindowsOptionalFeature -Online -FeatureName Client-ProjFS -NoRestart"
        );
        e
    })?;

    println!("projected at {}", root.display());

    let mut names: Vec<_> = std::fs::read_dir(&root)?
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    println!("entries: {names:?}");
    assert_eq!(names, vec!["hello.txt", "sub"]);

    let body = std::fs::read(root.join("hello.txt"))?;
    println!("hello.txt = {:?}", String::from_utf8_lossy(&body));
    assert_eq!(body, FILE_BODY);

    println!("OK");
    Ok(())
}
