//! Which clang resource directory (the compiler's own headers: `stddef.h`,
//! the SIMD intrinsics) bindgen's libclang parses the DPDK headers with.
//!
//! A resource directory is tied to one LLVM release: its intrinsics headers
//! call `__builtin_ia32_*` builtins that only a libclang of that release
//! knows. A libclang handed a newer release's headers declares the unknown
//! builtins implicitly, returning `int`, and fails with "invalid conversion
//! between vector type '__m128' and integer type 'int'". So the directory is
//! accepted only when its version is the loaded libclang's, wherever it was
//! found; nothing is trusted for being first in `PATH`.
//!
//! The logic is pure over paths (and a directory listing), so it lives here
//! rather than in `build.rs`: the build script takes it with `#[path]`, and
//! the crate's unit tests take it the same way, since a build script cannot
//! host tests.

use std::fmt;
use std::path::{Path, PathBuf};

/// Why a candidate resource directory was not chosen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    /// Its name is not a clang version (`21`, `14.0.6`), so its version is
    /// unknown.
    NotAVersion,
    /// The LLVM major version its name carries, which is not the loaded
    /// libclang's.
    OtherVersion(u32),
    /// A header the parse needs is missing: not a resource directory, or one
    /// whose headers are not installed.
    MissingHeader(&'static str),
    /// Not valid UTF-8, which bindgen's string arguments cannot carry.
    NotUtf8,
}

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAVersion => write!(f, "not named after a clang version"),
            Self::OtherVersion(major) => write!(f, "LLVM {major}"),
            Self::MissingHeader(header) => write!(f, "no include/{header}"),
            Self::NotUtf8 => write!(f, "path is not UTF-8"),
        }
    }
}

/// The outcome of [`select`]: the directory to pass as `-resource-dir`, if
/// any, and every candidate examined before it with the reason it was
/// passed over.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Selection {
    pub chosen: Option<PathBuf>,
    pub rejected: Vec<(PathBuf, Rejection)>,
}

/// The LLVM major version a resource directory's name carries: `21` for
/// `.../clang/21` (the layout since LLVM 16), `14` for `.../clang/14.0.6`
/// (the full version, before it). `None` for any other name.
pub fn dir_major(dir: &Path) -> Option<u32> {
    let name = dir.file_name()?.to_str()?;
    let mut parts = name.split('.');
    let major = parts.next()?;
    let all_digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !all_digits(major) || !parts.all(all_digits) {
        return None;
    }
    major.parse().ok()
}

/// The headers, relative to `<resource dir>/include`, that the DPDK parse
/// needs from a resource directory on `target_arch` (cargo's
/// `CARGO_CFG_TARGET_ARCH`): `stddef.h` on every target, and the header
/// DPDK's vector code reaches the intrinsics through on the targets it has
/// one for.
pub fn required_headers(target_arch: &str) -> &'static [&'static str] {
    match target_arch {
        "x86_64" | "x86" => &["stddef.h", "immintrin.h"],
        "aarch64" | "arm" => &["stddef.h", "arm_neon.h"],
        _ => &["stddef.h"],
    }
}

/// The first header of `required` missing from `dir/include`, if any.
pub fn missing_header(dir: &Path, required: &[&'static str]) -> Option<&'static str> {
    let include = dir.join("include");
    required
        .iter()
        .copied()
        .find(|header| !include.join(header).is_file())
}

/// Picks, in order, the first candidate whose version is `libclang_major`
/// and whose headers are all present (`missing_header` returns the first
/// one absent). The candidates examined before it are returned with the
/// reason each was passed over; none chosen means no `-resource-dir` at
/// all, and libclang's own default, which is of its own version.
pub fn select(
    libclang_major: u32,
    candidates: impl IntoIterator<Item = PathBuf>,
    missing_header: impl Fn(&Path) -> Option<&'static str>,
) -> Selection {
    let mut selection = Selection::default();
    for dir in candidates {
        let rejection = match dir_major(&dir) {
            None => Some(Rejection::NotAVersion),
            Some(major) if major != libclang_major => Some(Rejection::OtherVersion(major)),
            Some(_) => match missing_header(&dir) {
                Some(header) => Some(Rejection::MissingHeader(header)),
                None if dir.to_str().is_none() => Some(Rejection::NotUtf8),
                None => None,
            },
        };
        match rejection {
            Some(rejection) => selection.rejected.push((dir, rejection)),
            None => {
                selection.chosen = Some(dir);
                break;
            }
        }
    }
    selection
}

/// The directories, in order of preference, whose subdirectories are
/// resource directories installed beside the loaded libclang:
/// `<libdir>/clang` next to the library as loaded and next to the file its
/// symlinks resolve to (`canonical`). Debian and Ubuntu install
/// `/usr/lib/llvm-N/lib/libclang*.so*` beside `/usr/lib/llvm-N/lib/clang/N`,
/// with the library also reachable through a symlink in the multiarch
/// directory, in either direction depending on the release; Fedora installs
/// `/usr/lib64/libclang.so*` beside `/usr/lib64/clang/N`.
pub fn adjacent_roots(libclang: &Path, canonical: &Path) -> Vec<PathBuf> {
    unique(
        [libclang, canonical]
            .into_iter()
            .filter_map(Path::parent)
            .map(|libdir| libdir.join("clang")),
    )
}

/// `paths` in order with each repeat dropped, the first occurrence kept.
/// The places searched overlap (clang-sys loads libclang from
/// `/usr/lib/llvm-N/lib`, whose `clang` root is also the first well-known
/// one, and the system clang may report one of its subdirectories), and a
/// directory examined twice would be listed twice in the no-match warning.
/// A `Vec` with a linear scan: a handful of paths, built once per build.
pub fn unique(paths: impl IntoIterator<Item = PathBuf>) -> Vec<PathBuf> {
    let mut seen: Vec<PathBuf> = Vec::new();
    for path in paths {
        if !seen.contains(&path) {
            seen.push(path);
        }
    }
    seen
}

/// The well-known directories holding resource directories for any LLVM
/// release: the Debian/Ubuntu per-version tree for `libclang_major` and its
/// compatibility links, and the Fedora and `/usr/local` locations. A
/// directory found here is accepted only for its version, like any other.
pub fn well_known_roots(libclang_major: u32) -> Vec<PathBuf> {
    vec![
        PathBuf::from(format!("/usr/lib/llvm-{libclang_major}/lib/clang")),
        PathBuf::from("/usr/lib/clang"),
        PathBuf::from("/usr/lib64/clang"),
        PathBuf::from("/usr/local/lib/clang"),
    ]
}

/// The subdirectories of each root, root by root, each root's sorted by
/// name so the choice does not depend on the directory listing's order.
/// A root that does not exist, or cannot be listed, contributes nothing:
/// most of the well-known roots are absent on any given host, and a
/// directory that cannot be listed cannot be a candidate.
pub fn subdirectories(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for root in roots {
        // Absent or unreadable: no candidates here (see above). What was
        // examined is reported when nothing matches, so the gap is visible.
        let Ok(entries) = std::fs::read_dir(root) else {
            continue;
        };
        let mut dirs: Vec<PathBuf> = entries
            // An entry that cannot be read is skipped for the same reason.
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect();
        dirs.sort();
        candidates.extend(dirs);
    }
    candidates
}

/// The `cargo:warning` text for a build where no candidate matched the
/// loaded libclang, so bindgen runs with libclang's default headers.
pub fn no_match_warning(
    libclang: &Path,
    libclang_version: &str,
    rejected: &[(PathBuf, Rejection)],
) -> String {
    let examined = if rejected.is_empty() {
        "none found".to_string()
    } else {
        rejected
            .iter()
            .map(|(dir, why)| format!("{} ({why})", dir.display()))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "bindgen loads libclang {libclang_version} from {}, and no clang resource \
         directory of that version was found (examined: {examined}); generating the \
         DPDK bindings with libclang's built-in headers. If that fails, install the \
         clang headers of that LLVM version, or point LIBCLANG_PATH at a libclang \
         whose headers are installed.",
        libclang.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(names: &[&str]) -> Vec<PathBuf> {
        names.iter().map(PathBuf::from).collect()
    }

    #[test]
    fn dir_major_reads_major_only_and_full_version_names() {
        assert_eq!(
            dir_major(Path::new("/usr/lib/llvm-21/lib/clang/21")),
            Some(21)
        );
        assert_eq!(
            dir_major(Path::new("/usr/lib/llvm-14/lib/clang/14.0.6")),
            Some(14)
        );
        assert_eq!(dir_major(Path::new("/usr/lib64/clang/18")), Some(18));
    }

    #[test]
    fn dir_major_refuses_names_that_are_not_versions() {
        for name in [
            "/usr/lib/clang/include",
            "/usr/lib/clang/21git",
            "/usr/lib/clang/21.",
            "/usr/lib/clang/.21",
            "/usr/lib/clang/21.0-rc1",
            "/usr/lib/clang/99999999999",
            "/",
        ] {
            assert_eq!(dir_major(Path::new(name)), None, "{name}");
        }
    }

    #[test]
    fn select_takes_the_first_candidate_of_the_libclang_major() {
        let selection = select(14, paths(&["/a/clang/14.0.6", "/b/clang/14"]), |_| None);
        assert_eq!(selection.chosen, Some(PathBuf::from("/a/clang/14.0.6")));
        assert!(selection.rejected.is_empty());

        let selection = select(21, paths(&["/usr/lib/llvm-21/lib/clang/21"]), |_| None);
        assert_eq!(
            selection.chosen,
            Some(PathBuf::from("/usr/lib/llvm-21/lib/clang/21"))
        );
    }

    /// The CI failure: an LLVM 18 libclang and `clang` from LLVM 21. The 21
    /// headers are passed over, whatever their position, for the 18 ones.
    #[test]
    fn select_rejects_another_major_and_keeps_looking() {
        let selection = select(
            18,
            paths(&[
                "/usr/lib/llvm-21/lib/clang/21",
                "/usr/lib/clang/include",
                "/usr/lib/llvm-18/lib/clang/18",
            ]),
            |_| None,
        );
        assert_eq!(
            selection.chosen,
            Some(PathBuf::from("/usr/lib/llvm-18/lib/clang/18"))
        );
        assert_eq!(
            selection.rejected,
            vec![
                (
                    PathBuf::from("/usr/lib/llvm-21/lib/clang/21"),
                    Rejection::OtherVersion(21)
                ),
                (
                    PathBuf::from("/usr/lib/clang/include"),
                    Rejection::NotAVersion
                ),
            ]
        );
    }

    #[test]
    fn select_rejects_the_right_version_without_its_headers() {
        let selection = select(18, paths(&["/a/clang/18", "/b/clang/18"]), |dir| {
            dir.starts_with("/a").then_some("stddef.h")
        });
        assert_eq!(selection.chosen, Some(PathBuf::from("/b/clang/18")));
        assert_eq!(
            selection.rejected,
            vec![(
                PathBuf::from("/a/clang/18"),
                Rejection::MissingHeader("stddef.h")
            )]
        );
    }

    #[test]
    fn select_without_a_match_chooses_nothing() {
        let selection = select(18, Vec::new(), |_| None);
        assert_eq!(selection, Selection::default());

        let selection = select(18, paths(&["/usr/lib/llvm-21/lib/clang/21"]), |_| None);
        assert_eq!(selection.chosen, None);
        assert_eq!(selection.rejected.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn select_rejects_a_path_bindgen_cannot_carry() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let dir = Path::new(OsStr::from_bytes(b"/opt/\xff/clang/18")).to_path_buf();
        let selection = select(18, vec![dir.clone()], |_| None);
        assert_eq!(selection.chosen, None);
        assert_eq!(selection.rejected, vec![(dir, Rejection::NotUtf8)]);
    }

    #[test]
    fn required_headers_follow_the_target() {
        assert_eq!(required_headers("x86_64"), ["stddef.h", "immintrin.h"]);
        assert_eq!(required_headers("aarch64"), ["stddef.h", "arm_neon.h"]);
        assert_eq!(required_headers("riscv64"), ["stddef.h"]);
    }

    #[test]
    fn missing_header_reports_the_first_absent_one() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = root.path().join("18");
        std::fs::create_dir_all(dir.join("include")).expect("mkdir");
        let required = required_headers("x86_64");
        assert_eq!(missing_header(&dir, required), Some("stddef.h"));
        std::fs::write(dir.join("include/stddef.h"), "").expect("write");
        assert_eq!(missing_header(&dir, required), Some("immintrin.h"));
        std::fs::write(dir.join("include/immintrin.h"), "").expect("write");
        assert_eq!(missing_header(&dir, required), None);
    }

    #[test]
    fn adjacent_roots_look_beside_the_library_and_its_target() {
        assert_eq!(
            adjacent_roots(
                Path::new("/usr/lib/x86_64-linux-gnu/libclang-18.so.18"),
                Path::new("/usr/lib/llvm-18/lib/libclang.so.18.1"),
            ),
            paths(&[
                "/usr/lib/x86_64-linux-gnu/clang",
                "/usr/lib/llvm-18/lib/clang"
            ])
        );
        // Not a symlink: one root, not the same one twice.
        assert_eq!(
            adjacent_roots(
                Path::new("/usr/lib64/libclang.so.18"),
                Path::new("/usr/lib64/libclang.so.18"),
            ),
            paths(&["/usr/lib64/clang"])
        );
    }

    #[test]
    fn unique_keeps_the_first_of_each_path_in_order() {
        assert_eq!(
            unique(paths(&[
                "/usr/lib/llvm-18/lib/clang",
                "/usr/lib/clang",
                "/usr/lib/llvm-18/lib/clang",
                "/usr/lib64/clang",
                "/usr/lib/clang",
            ])),
            paths(&[
                "/usr/lib/llvm-18/lib/clang",
                "/usr/lib/clang",
                "/usr/lib64/clang"
            ])
        );
        assert_eq!(unique(Vec::new()), Vec::<PathBuf>::new());
    }

    #[test]
    fn well_known_roots_include_the_per_version_tree() {
        let roots = well_known_roots(18);
        assert_eq!(roots[0], PathBuf::from("/usr/lib/llvm-18/lib/clang"));
        assert!(roots.contains(&PathBuf::from("/usr/lib64/clang")));
    }

    #[test]
    fn subdirectories_lists_each_root_in_order_and_skips_absent_ones() {
        let first = tempfile::tempdir().expect("tempdir");
        let second = tempfile::tempdir().expect("tempdir");
        for name in ["21", "14.0.6"] {
            std::fs::create_dir(first.path().join(name)).expect("mkdir");
        }
        std::fs::write(first.path().join("not-a-dir"), "").expect("write");
        std::fs::create_dir(second.path().join("18")).expect("mkdir");
        let roots = vec![
            first.path().to_path_buf(),
            first.path().join("absent"),
            second.path().to_path_buf(),
        ];
        assert_eq!(
            subdirectories(&roots),
            vec![
                first.path().join("14.0.6"),
                first.path().join("21"),
                second.path().join("18"),
            ]
        );
    }

    /// The whole search over a real tree, as the build script runs it: in
    /// listing order, a directory of another version and one of the right
    /// version without its headers are skipped for the installed match.
    #[test]
    fn select_over_a_tree_finds_the_matching_installed_headers() {
        let root = tempfile::tempdir().expect("tempdir");
        for (version, headers) in [("17", true), ("18", false), ("18.1.3", true)] {
            let include = root.path().join(version).join("include");
            std::fs::create_dir_all(&include).expect("mkdir");
            if headers {
                std::fs::write(include.join("stddef.h"), "").expect("write");
            }
        }
        let required = required_headers("riscv64");
        let selection = select(18, subdirectories(&[root.path().to_path_buf()]), |dir| {
            missing_header(dir, required)
        });
        assert_eq!(selection.chosen, Some(root.path().join("18.1.3")));
        assert_eq!(
            selection.rejected,
            vec![
                (root.path().join("17"), Rejection::OtherVersion(17)),
                (root.path().join("18"), Rejection::MissingHeader("stddef.h")),
            ]
        );

        let selection = select(19, subdirectories(&[root.path().to_path_buf()]), |dir| {
            missing_header(dir, required)
        });
        assert_eq!(selection.chosen, None);
        assert_eq!(
            selection.rejected,
            vec![
                (root.path().join("17"), Rejection::OtherVersion(17)),
                (root.path().join("18"), Rejection::OtherVersion(18)),
                (root.path().join("18.1.3"), Rejection::OtherVersion(18)),
            ]
        );
    }

    #[test]
    fn no_match_warning_names_the_library_and_every_rejection() {
        let warning = no_match_warning(
            Path::new("/usr/lib/x86_64-linux-gnu/libclang-18.so.18"),
            "18.1",
            &[
                (
                    PathBuf::from("/usr/lib/llvm-21/lib/clang/21"),
                    Rejection::OtherVersion(21),
                ),
                (
                    PathBuf::from("/usr/lib/clang/18"),
                    Rejection::MissingHeader("immintrin.h"),
                ),
                (
                    PathBuf::from("/usr/lib/clang/include"),
                    Rejection::NotAVersion,
                ),
                (PathBuf::from("/opt/x"), Rejection::NotUtf8),
            ],
        );
        assert!(warning.contains("libclang 18.1 from /usr/lib/x86_64-linux-gnu/libclang-18.so.18"));
        assert!(warning.contains("/usr/lib/llvm-21/lib/clang/21 (LLVM 21)"));
        assert!(warning.contains("/usr/lib/clang/18 (no include/immintrin.h)"));
        assert!(warning.contains("/usr/lib/clang/include (not named after a clang version)"));
        assert!(warning.contains("/opt/x (path is not UTF-8)"));
        assert!(warning.contains("LIBCLANG_PATH"));

        let warning = no_match_warning(Path::new("/l/libclang.so"), "19.0", &[]);
        assert!(warning.contains("examined: none found"));
    }
}
