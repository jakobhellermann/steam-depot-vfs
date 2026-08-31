//! Safe-ish binding for the Windows Projected File System (ProjFS) API.
//! Reference: https://learn.microsoft.com/en-us/windows/win32/projfs/projected-file-system

#![cfg(windows)]

use std::collections::HashMap;
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use windows::Win32::Storage::ProjectedFileSystem::*;
use windows::Win32::System::Com::CoCreateGuid;
use windows::core::{GUID, HRESULT, HSTRING, PCWSTR};

// Win32 error → HRESULT is `0x8007_0000 | code`; kept local to avoid an
// extra `windows` feature just for the constants.
const S_OK: HRESULT = HRESULT(0);
const HR_FILE_NOT_FOUND: HRESULT = HRESULT(0x8007_0002u32 as i32);
const HR_INSUFFICIENT_BUFFER: HRESULT = HRESULT(0x8007_007Au32 as i32);
const HR_OUT_OF_MEMORY: HRESULT = HRESULT(0x8007_000Eu32 as i32);
const HR_FAIL: HRESULT = HRESULT(0x8000_4005u32 as i32);

const FILE_ATTRIBUTE_READONLY: u32 = 0x1;
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x10;

/// Read-only source backing a projection. Paths are relative to the root
/// (root = empty path) and backslash-separated, as ProjFS hands them out.
pub trait Filesystem: Send + Sync + 'static {
    fn list_directory(&self, path: &Path) -> Vec<DirEntry>;
    /// `None` = the path does not exist.
    fn get_metadata(&self, path: &Path) -> Option<Metadata>;
    fn read_file(&self, path: &Path, offset: u64, length: u32) -> Result<Vec<u8>, std::io::Error>;
}

pub struct DirEntry {
    /// Last path component only.
    pub name: String,
    pub metadata: Metadata,
}

/// Times are Windows `FILETIME` (100 ns ticks since 1601); `0` = unknown.
pub struct Metadata {
    pub is_dir: bool,
    pub size: u64,
    pub creation_time: i64,
    pub last_write_time: i64,
}

/// Unix seconds → Windows `FILETIME`. `0` stays `0` ("unknown").
pub fn unix_to_filetime(unix_secs: i64) -> i64 {
    if unix_secs == 0 {
        return 0;
    }
    // 11_644_473_600 s between 1601-01-01 and the Unix epoch.
    (unix_secs + 11_644_473_600) * 10_000_000
}

/// An active projection. Dropping it stops the virtualization.
pub struct ProjFS {
    nctx: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    ctx: *mut Context,
}

// `ctx` is solely owned by this handle and the source is Send + Sync.
unsafe impl Send for ProjFS {}
unsafe impl Sync for ProjFS {}

/// Whether ProjectedFSLib.dll can be loaded, i.e. the optional ProjFS
/// feature is enabled. The handle is intentionally leaked — it's a system
/// DLL we want resident once loaded.
fn projfs_available() -> bool {
    use windows::Win32::System::LibraryLoader::LoadLibraryW;
    use windows::core::w;
    unsafe { LoadLibraryW(w!("ProjectedFSLib.dll")).is_ok() }
}

/// Outcome of an attempt to enable ProjFS
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnableOutcome {
    /// Feature enabled and ready to use.
    Enabled,
    /// Feature enabled, but Windows needs a restart before it works.
    EnabledRestartNeeded,
    /// The user dismissed the UAC prompt.
    Cancelled,
}

/// Prompt to enable ProjFS
pub fn enable_feature_elevated() -> Result<EnableOutcome, std::io::Error> {
    use std::os::windows::process::CommandExt;
    use std::process::Command;

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    let script = "try { \
         $p = Start-Process dism.exe -Verb RunAs -Wait -PassThru -ArgumentList \
         '/online','/enable-feature','/featurename:Client-ProjFS','/norestart'; \
         exit $p.ExitCode \
       } catch { exit 1223 }";
    let status = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .creation_flags(CREATE_NO_WINDOW)
        .status()?;
    match status.code() {
        Some(0) => Ok(EnableOutcome::Enabled),
        Some(3010) => Ok(EnableOutcome::EnabledRestartNeeded),
        Some(1223) => Ok(EnableOutcome::Cancelled),
        other => Err(std::io::Error::other(format!(
            "Failed to enable ProjFS (exit code {other:?})"
        ))),
    }
}

impl ProjFS {
    pub fn new(root: &Path, source: impl Filesystem) -> Result<Self, std::io::Error> {
        // ProjectedFSLib.dll is delay-loaded (see build.rs), so probe before
        // the first `Prj*` call: if the optional ProjFS feature is off the
        // DLL is absent and calling into it would abort the process.
        if !projfs_available() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Windows ProjFS feature not enabled; enable it (admin):\n\
                 Enable-WindowsOptionalFeature -Online -FeatureName Client-ProjFS -NoRestart",
            ));
        }

        std::fs::create_dir_all(root)?;
        let root_h = HSTRING::from(root.as_os_str());

        let guid = unsafe { CoCreateGuid() }.map_err(to_io)?;

        // Per MS docs this need only run once per virtualization root; we
        // don't track prior runs, so we call it every time and ignore the
        // result.
        unsafe {
            let _ =
                PrjMarkDirectoryAsPlaceholder(PCWSTR(root_h.as_ptr()), PCWSTR::null(), None, &guid);
        }

        let ctx = Box::into_raw(Box::new(Context {
            source: Box::new(source),
            enums: Mutex::new(HashMap::new()),
        }));

        let callbacks = PRJ_CALLBACKS {
            StartDirectoryEnumerationCallback: Some(start_dir_enum),
            EndDirectoryEnumerationCallback: Some(end_dir_enum),
            GetDirectoryEnumerationCallback: Some(get_dir_enum),
            GetPlaceholderInfoCallback: Some(get_placeholder_info),
            GetFileDataCallback: Some(get_file_data),
            QueryFileNameCallback: None,
            NotificationCallback: None,
            CancelCommandCallback: None,
        };

        let started = unsafe {
            PrjStartVirtualizing(
                PCWSTR(root_h.as_ptr()),
                &callbacks,
                Some(ctx as *const c_void),
                None,
            )
        };

        match started {
            Ok(nctx) => Ok(ProjFS { nctx, ctx }),
            Err(e) => {
                drop(unsafe { Box::from_raw(ctx) });
                Err(to_io(e))
            }
        }
    }
}

impl Drop for ProjFS {
    fn drop(&mut self) {
        unsafe {
            PrjStopVirtualizing(self.nctx);
            drop(Box::from_raw(self.ctx));
        }
    }
}

struct Context {
    source: Box<dyn Filesystem>,
    enums: Mutex<HashMap<u128, EnumSession>>,
}

struct EnumSession {
    entries: Vec<(HSTRING, PRJ_FILE_BASIC_INFO)>,
    index: usize,
    search: Option<HSTRING>,
    search_captured: bool,
}

fn to_io(e: windows::core::Error) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

fn guid_key(g: &GUID) -> u128 {
    ((g.data1 as u128) << 96)
        | ((g.data2 as u128) << 80)
        | ((g.data3 as u128) << 64)
        | (u64::from_be_bytes(g.data4) as u128)
}

/// Null/empty PCWSTR = root (empty path).
unsafe fn path_from_pcwstr(p: PCWSTR) -> PathBuf {
    if p.is_null() {
        return PathBuf::new();
    }
    match unsafe { p.to_string() } {
        Ok(s) => PathBuf::from(s),
        Err(_) => PathBuf::new(),
    }
}

fn basic_info(m: &Metadata) -> PRJ_FILE_BASIC_INFO {
    PRJ_FILE_BASIC_INFO {
        IsDirectory: m.is_dir,
        FileSize: m.size as i64,
        CreationTime: m.creation_time,
        LastAccessTime: m.last_write_time,
        LastWriteTime: m.last_write_time,
        ChangeTime: m.last_write_time,
        FileAttributes: if m.is_dir {
            FILE_ATTRIBUTE_DIRECTORY
        } else {
            FILE_ATTRIBUTE_READONLY
        },
    }
}

/// Turn a panic into `E_FAIL`; a panic must not unwind out of an `extern`
/// callback (Rust aborts the process if it does).
fn guard(f: impl FnOnce() -> HRESULT) -> HRESULT {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(hr) => hr,
        Err(_) => HR_FAIL,
    }
}

unsafe fn context_of(cbdata: *const PRJ_CALLBACK_DATA) -> &'static Context {
    unsafe { &*((*cbdata).InstanceContext as *const Context) }
}

unsafe extern "system" fn start_dir_enum(
    cbdata: *const PRJ_CALLBACK_DATA,
    enumerationid: *const GUID,
) -> HRESULT {
    guard(|| unsafe {
        let ctx = context_of(cbdata);
        let path = path_from_pcwstr((*cbdata).FilePathName);

        let mut entries: Vec<(HSTRING, PRJ_FILE_BASIC_INFO)> = ctx
            .source
            .list_directory(&path)
            .into_iter()
            .map(|e| (HSTRING::from(e.name), basic_info(&e.metadata)))
            .collect();

        // ProjFS requires the entries in PrjFileNameCompare's sort order.
        entries
            .sort_by(|a, b| PrjFileNameCompare(PCWSTR(a.0.as_ptr()), PCWSTR(b.0.as_ptr())).cmp(&0));

        let key = guid_key(&*enumerationid);
        ctx.enums.lock().unwrap().insert(
            key,
            EnumSession {
                entries,
                index: 0,
                search: None,
                search_captured: false,
            },
        );
        S_OK
    })
}

unsafe extern "system" fn get_dir_enum(
    cbdata: *const PRJ_CALLBACK_DATA,
    enumerationid: *const GUID,
    searchexpression: PCWSTR,
    direntrybufferhandle: PRJ_DIR_ENTRY_BUFFER_HANDLE,
) -> HRESULT {
    guard(|| unsafe {
        let ctx = context_of(cbdata);
        let key = guid_key(&*enumerationid);
        let mut map = ctx.enums.lock().unwrap();
        let Some(sess) = map.get_mut(&key) else {
            return HR_FILE_NOT_FOUND;
        };

        let restart = ((*cbdata).Flags.0 & PRJ_CB_DATA_FLAG_ENUM_RESTART_SCAN.0) != 0;
        if restart {
            sess.index = 0;
            sess.search = None;
            sess.search_captured = false;
        }

        // Capture the search expression on a scan's first call; the restart
        // branch above forces a re-capture. It is reused on later calls.
        if !sess.search_captured {
            sess.search = if searchexpression.is_null() {
                None
            } else {
                match searchexpression.to_string() {
                    Ok(s) if !s.is_empty() => Some(HSTRING::from(s)),
                    _ => None,
                }
            };
            sess.search_captured = true;
        }

        let mut added = false;
        while sess.index < sess.entries.len() {
            let (name, info) = &sess.entries[sess.index];
            let matches = match &sess.search {
                Some(pat) => PrjFileNameMatch(PCWSTR(name.as_ptr()), PCWSTR(pat.as_ptr())),
                None => true,
            };
            if matches {
                if let Err(e) = PrjFillDirEntryBuffer(
                    PCWSTR(name.as_ptr()),
                    Some(info as *const _),
                    direntrybufferhandle,
                ) {
                    if e.code() != HR_INSUFFICIENT_BUFFER {
                        return e.code();
                    }
                    // Buffer full: keep `index` and resume on the next call.
                    // If not even one entry fit this call, ProjFS wants the
                    // error returned rather than S_OK.
                    return if added { S_OK } else { HR_INSUFFICIENT_BUFFER };
                }
                added = true;
            }
            sess.index += 1;
        }
        S_OK
    })
}

unsafe extern "system" fn end_dir_enum(
    cbdata: *const PRJ_CALLBACK_DATA,
    enumerationid: *const GUID,
) -> HRESULT {
    guard(|| unsafe {
        let ctx = context_of(cbdata);
        ctx.enums.lock().unwrap().remove(&guid_key(&*enumerationid));
        S_OK
    })
}

unsafe extern "system" fn get_placeholder_info(cbdata: *const PRJ_CALLBACK_DATA) -> HRESULT {
    guard(|| unsafe {
        let ctx = context_of(cbdata);
        let path = path_from_pcwstr((*cbdata).FilePathName);

        let Some(meta) = ctx.source.get_metadata(&path) else {
            return HR_FILE_NOT_FOUND;
        };

        let info = PRJ_PLACEHOLDER_INFO {
            FileBasicInfo: basic_info(&meta),
            ..Default::default()
        };

        match PrjWritePlaceholderInfo(
            (*cbdata).NamespaceVirtualizationContext,
            (*cbdata).FilePathName,
            &info,
            std::mem::size_of::<PRJ_PLACEHOLDER_INFO>() as u32,
        ) {
            Ok(()) => S_OK,
            Err(e) => e.code(),
        }
    })
}

unsafe extern "system" fn get_file_data(
    cbdata: *const PRJ_CALLBACK_DATA,
    byteoffset: u64,
    length: u32,
) -> HRESULT {
    guard(|| unsafe {
        let ctx = context_of(cbdata);
        let path = path_from_pcwstr((*cbdata).FilePathName);

        let data = match ctx.source.read_file(&path, byteoffset, length) {
            Ok(d) => d,
            Err(_) => return HR_FAIL,
        };
        if data.is_empty() {
            return S_OK;
        }

        let nctx = (*cbdata).NamespaceVirtualizationContext;
        let buf = PrjAllocateAlignedBuffer(nctx, data.len());
        if buf.is_null() {
            return HR_OUT_OF_MEMORY;
        }
        std::ptr::copy_nonoverlapping(data.as_ptr(), buf as *mut u8, data.len());

        let r = PrjWriteFileData(
            nctx,
            &(*cbdata).DataStreamId,
            buf,
            byteoffset,
            data.len() as u32,
        );
        PrjFreeAlignedBuffer(buf);

        match r {
            Ok(()) => S_OK,
            Err(e) => e.code(),
        }
    })
}
