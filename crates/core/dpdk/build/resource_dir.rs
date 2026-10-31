//! Which clang resource directory (the compiler's own headers: `stddef.h`,
//! the SIMD intrinsics) bindgen's libclang parses the DPDK headers with.
//!
//! A resource directory belongs to one LLVM release: its intrinsics call
//! builtins only a libclang of that release knows, and a libclang handed
//! another release's fails with "invalid conversion between vector type
//! '__m128' and integer type 'int'". So a directory is accepted only when
//! its name carries the loaded libclang's major version.
//!
//! Pure over paths, so the build script and the crate's unit tests both take
//! it with `#[path]` (a build script cannot host tests).

use std::path::{Path, PathBuf};

/// The LLVM major version a resource directory's name carries: `21` for
/// `.../clang/21` (the layout since LLVM 16), `14` for `.../clang/14.0.6`
/// (before it). `None` for any other name.
pub fn dir_major(dir: &Path) -> Option<u32> {
    let name = dir.file_name()?.to_str()?;
    let all_digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let mut parts = name.split('.');
    let major = parts.next()?;
    if !all_digits(major) || !parts.all(all_digits) {
        return None;
    }
    major.parse().ok()
}

/// The directories whose subdirectories are searched, in order: `clang`
/// beside the loaded libclang, as loaded and with its symlinks resolved
/// (Debian and Ubuntu reach `/usr/lib/llvm-N/lib/libclang*` through a
/// multiarch symlink, in either direction), then the Debian/Ubuntu tree of
/// that major, then `/usr/lib/clang`, where Fedora installs the headers
/// while its libclang sits in `/usr/lib64`. A root already listed is not
/// listed again.
pub fn roots(libclang: &Path, canonical: &Path, major: u32) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    for root in [libclang, canonical]
        .into_iter()
        .filter_map(Path::parent)
        .map(|libdir| libdir.join("clang"))
        .chain([
            PathBuf::from(format!("/usr/lib/llvm-{major}/lib/clang")),
            PathBuf::from("/usr/lib/clang"),
        ])
    {
        // A `Vec` searched linearly: there are at most four roots.
        if !roots.contains(&root) {
            roots.push(root);
        }
    }
    roots
}

/// The subdirectories of each root, root by root, each root's sorted so the
/// choice does not depend on listing order. A root that is absent or cannot
/// be listed, and an entry that cannot be read, contribute nothing: neither
/// can be a candidate, and a miss overall lists what was examined.
pub fn subdirectories(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for root in roots {
        let Ok(entries) = std::fs::read_dir(root) else {
            continue;
        };
        let mut dirs: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect();
        dirs.sort();
        candidates.extend(dirs);
    }
    candidates
}

/// The first of `primary` named after `major` and holding `include/stddef.h`;
/// only if there is none, `fallback`'s directory under the same rule. `Err`
/// lists each directory skipped with why, for the build's warning.
pub fn select(
    major: u32,
    primary: impl IntoIterator<Item = PathBuf>,
    fallback: impl FnOnce() -> Option<PathBuf>,
) -> Result<String, Vec<String>> {
    let mut skipped = Vec::new();
    let mut accept = |dir: PathBuf| {
        let reason = match dir_major(&dir) {
            None => "not a version".to_string(),
            Some(other) if other != major => format!("LLVM {other}"),
            Some(_) if !dir.join("include/stddef.h").is_file() => "no include/stddef.h".to_string(),
            // bindgen's arguments are strings.
            Some(_) => match dir.to_str() {
                Some(dir) => return Some(dir.to_string()),
                None => "not UTF-8".to_string(),
            },
        };
        // The fallback may name a directory already skipped as a primary.
        let entry = format!("{} ({reason})", dir.display());
        if !skipped.contains(&entry) {
            skipped.push(entry);
        }
        None
    };
    let chosen = primary
        .into_iter()
        .find_map(&mut accept)
        .or_else(|| fallback().and_then(&mut accept));
    chosen.ok_or(skipped)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `root/name`, with `include/stddef.h` when `headers`.
    fn install(root: &Path, name: &str, headers: bool) -> PathBuf {
        let dir = root.join(name);
        std::fs::create_dir_all(dir.join("include")).expect("mkdir");
        if headers {
            std::fs::write(dir.join("include/stddef.h"), "").expect("write");
        }
        dir
    }

    fn no_fallback() -> Option<PathBuf> {
        None
    }

    #[test]
    fn dir_major_reads_major_only_and_full_version_names() {
        assert_eq!(
            dir_major(Path::new("/usr/lib/llvm-21/lib/clang/21")),
            Some(21)
        );
        assert_eq!(dir_major(Path::new("/usr/lib/clang/14.0.6")), Some(14));
    }

    #[test]
    fn dir_major_refuses_names_that_are_not_versions() {
        for name in [
            "/c/include",
            "/c/21git",
            "/c/21.",
            "/c/.21",
            "/c/21.0-rc1",
            "/c/99999999999",
            "/",
        ] {
            assert_eq!(dir_major(Path::new(name)), None, "{name}");
        }
    }

    #[test]
    fn select_takes_the_first_installed_directory_of_the_major() {
        let root = tempfile::tempdir().expect("tempdir");
        let full = install(root.path(), "14.0.6", true);
        let short = install(root.path(), "14", true);
        assert_eq!(
            select(14, [full.clone(), short.clone()], no_fallback),
            Ok(full.display().to_string())
        );
        let dir = install(root.path(), "21", true);
        assert_eq!(
            select(21, [dir.clone()], no_fallback),
            Ok(dir.display().to_string())
        );
    }

    /// The CI failure: an LLVM 18 libclang with LLVM 21 headers installed.
    #[test]
    fn select_skips_another_major_and_names_without_headers_or_version() {
        let root = tempfile::tempdir().expect("tempdir");
        let other = install(root.path(), "21", true);
        let not_version = install(root.path(), "include", true);
        let headerless = install(root.path(), "18", false);
        let chosen = install(root.path(), "18.1.3", true);
        assert_eq!(
            select(
                18,
                [
                    other.clone(),
                    not_version.clone(),
                    headerless.clone(),
                    chosen.clone()
                ],
                no_fallback
            ),
            Ok(chosen.display().to_string())
        );
        assert_eq!(
            select(
                18,
                [other.clone(), not_version.clone(), headerless.clone()],
                no_fallback
            ),
            Err(vec![
                format!("{} (LLVM 21)", other.display()),
                format!("{} (not a version)", not_version.display()),
                format!("{} (no include/stddef.h)", headerless.display()),
            ])
        );
    }

    #[test]
    fn select_without_a_candidate_chooses_nothing() {
        assert_eq!(select(18, [], no_fallback), Err(Vec::new()));
    }

    #[cfg(unix)]
    #[test]
    fn select_skips_a_path_bindgen_cannot_carry() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let root = tempfile::tempdir().expect("tempdir");
        let dir = install(&root.path().join(OsStr::from_bytes(b"\xff")), "18", true);
        assert_eq!(
            select(18, [dir.clone()], no_fallback),
            Err(vec![format!("{} (not UTF-8)", dir.display())])
        );
    }

    #[test]
    fn select_consults_the_fallback_only_without_a_primary_match() {
        let root = tempfile::tempdir().expect("tempdir");
        let primary = install(root.path(), "18", true);
        let fallback = install(&root.path().join("system"), "18", true);
        let unexpected = || -> Option<PathBuf> { panic!("fallback consulted despite a match") };
        assert_eq!(
            select(18, [primary.clone()], unexpected),
            Ok(primary.display().to_string())
        );

        let other = install(root.path(), "21", true);
        let fallback_dir = fallback.clone();
        assert_eq!(
            select(18, [other.clone()], move || Some(fallback_dir)),
            Ok(fallback.display().to_string())
        );
        // The fallback is held to the same rule, and listed when it fails it.
        let wrong = install(&root.path().join("system"), "21", true);
        let wrong_dir = wrong.clone();
        assert_eq!(
            select(18, [other.clone()], move || Some(wrong_dir)),
            Err(vec![
                format!("{} (LLVM 21)", other.display()),
                format!("{} (LLVM 21)", wrong.display())
            ])
        );
        // A fallback naming a directory already skipped is listed once.
        let other_dir = other.clone();
        assert_eq!(
            select(18, [other.clone()], move || Some(other_dir)),
            Err(vec![format!("{} (LLVM 21)", other.display())])
        );
    }

    #[test]
    fn roots_look_beside_the_library_its_target_and_the_install_trees() {
        assert_eq!(
            roots(
                Path::new("/usr/lib/x86_64-linux-gnu/libclang-18.so.18"),
                Path::new("/usr/lib/llvm-18/lib/libclang.so.18.1"),
                18,
            ),
            [
                PathBuf::from("/usr/lib/x86_64-linux-gnu/clang"),
                PathBuf::from("/usr/lib/llvm-18/lib/clang"),
                PathBuf::from("/usr/lib/clang"),
            ]
        );
        // Fedora: a library that is not a link is not searched beside
        // twice, and the headers' `/usr/lib/clang` is searched after it.
        let lib = Path::new("/usr/lib64/libclang.so.18");
        assert_eq!(
            roots(lib, lib, 18),
            [
                PathBuf::from("/usr/lib64/clang"),
                PathBuf::from("/usr/lib/llvm-18/lib/clang"),
                PathBuf::from("/usr/lib/clang"),
            ]
        );
        // Nor is the major tree, when the library is in it.
        let lib = Path::new("/usr/lib/llvm-18/lib/libclang.so.1");
        assert_eq!(
            roots(lib, lib, 18),
            [
                PathBuf::from("/usr/lib/llvm-18/lib/clang"),
                PathBuf::from("/usr/lib/clang"),
            ]
        );
        // Nor is `/usr/lib/clang`, when the library is in `/usr/lib`.
        let lib = Path::new("/usr/lib/libclang.so.18");
        assert_eq!(
            roots(lib, lib, 18),
            [
                PathBuf::from("/usr/lib/clang"),
                PathBuf::from("/usr/lib/llvm-18/lib/clang"),
            ]
        );
        // Loaded from the major tree through a link resolving elsewhere.
        assert_eq!(
            roots(
                Path::new("/usr/lib/llvm-18/lib/libclang.so.1"),
                Path::new("/opt/llvm/lib/libclang.so.18.1"),
                18
            ),
            [
                PathBuf::from("/usr/lib/llvm-18/lib/clang"),
                PathBuf::from("/opt/llvm/lib/clang"),
                PathBuf::from("/usr/lib/clang"),
            ]
        );
    }

    #[test]
    fn subdirectories_lists_each_root_sorted_and_skips_absent_ones() {
        let first = tempfile::tempdir().expect("tempdir");
        let second = tempfile::tempdir().expect("tempdir");
        for name in ["21", "14.0.6"] {
            std::fs::create_dir(first.path().join(name)).expect("mkdir");
        }
        std::fs::write(first.path().join("not-a-dir"), "").expect("write");
        std::fs::create_dir(second.path().join("18")).expect("mkdir");
        let roots = [
            first.path().to_path_buf(),
            first.path().join("absent"),
            second.path().to_path_buf(),
        ];
        assert_eq!(
            subdirectories(&roots),
            [
                first.path().join("14.0.6"),
                first.path().join("21"),
                second.path().join("18")
            ]
        );
    }
}
