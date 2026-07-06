use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Default)]
struct LockPackage {
    name: Option<String>,
    version: Option<String>,
    source: Option<String>,
    dependencies: Vec<String>,
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join("Cargo.toml").display()
    );
    for candidate in lockfile_candidates(&manifest_dir) {
        println!("cargo:rerun-if-changed={}", candidate.display());
    }

    let driver_version = find_lockfile_driver_version(&manifest_dir)
        .or_else(|| find_exact_manifest_driver_version(&manifest_dir))
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=VELR_DRIVER_VERSION={driver_version}");
}

fn lockfile_candidates(manifest_dir: &Path) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    let mut dir = Some(manifest_dir);
    for _ in 0..3 {
        let Some(current) = dir else {
            break;
        };
        candidates.push(current.join("Cargo.lock"));
        dir = current.parent();
    }
    candidates
}

fn find_lockfile_driver_version(manifest_dir: &Path) -> Option<String> {
    let lockfile = lockfile_candidates(manifest_dir)
        .into_iter()
        .find(|candidate| candidate.exists())?;
    let text = fs::read_to_string(lockfile).ok()?;
    let packages = parse_lock_packages(&text);
    let registry_velr = packages
        .iter()
        .filter(|package| {
            package.name.as_deref() == Some("velr")
                && package
                    .source
                    .as_deref()
                    .map(|source| source.starts_with("registry+"))
                    .unwrap_or(false)
        })
        .collect::<Vec<_>>();

    if let Some(cli) = find_cli_lock_package(&packages) {
        for dependency in &cli.dependencies {
            if dependency == "velr" && registry_velr.len() == 1 {
                return registry_velr[0].version.clone();
            }
            if let Some(rest) = dependency.strip_prefix("velr ") {
                let version = rest.split_whitespace().next()?;
                if let Some(package) = registry_velr
                    .iter()
                    .find(|package| package.version.as_deref() == Some(version))
                {
                    return package.version.clone();
                }
            }
        }
    }

    if registry_velr.len() == 1 {
        registry_velr[0].version.clone()
    } else {
        None
    }
}

fn find_cli_lock_package(packages: &[LockPackage]) -> Option<&LockPackage> {
    let package_name = env::var("CARGO_PKG_NAME").ok()?;
    let package_version = env::var("CARGO_PKG_VERSION").ok()?;
    packages.iter().find(|package| {
        package.name.as_deref() == Some(package_name.as_str())
            && package.version.as_deref() == Some(package_version.as_str())
    })
}

fn parse_lock_packages(text: &str) -> Vec<LockPackage> {
    let mut packages = Vec::new();
    let mut current = None;
    let mut in_dependencies = false;

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed == "[[package]]" {
            if let Some(package) = current.take() {
                packages.push(package);
            }
            current = Some(LockPackage::default());
            in_dependencies = false;
            continue;
        }

        let Some(package) = current.as_mut() else {
            continue;
        };

        if in_dependencies {
            if trimmed == "]" {
                in_dependencies = false;
            } else if let Some(value) = parse_quoted_array_item(trimmed) {
                package.dependencies.push(value);
            }
            continue;
        }

        if trimmed == "dependencies = [" {
            in_dependencies = true;
        } else if let Some(value) = parse_key_string(trimmed, "name") {
            package.name = Some(value);
        } else if let Some(value) = parse_key_string(trimmed, "version") {
            package.version = Some(value);
        } else if let Some(value) = parse_key_string(trimmed, "source") {
            package.source = Some(value);
        }
    }

    if let Some(package) = current {
        packages.push(package);
    }

    packages
}

fn find_exact_manifest_driver_version(manifest_dir: &Path) -> Option<String> {
    let manifest = fs::read_to_string(manifest_dir.join("Cargo.toml")).ok()?;
    let velr_line = manifest
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("velr = "))?;
    let version = inline_table_string_field(velr_line, "version")?;
    let exact = version.strip_prefix('=')?;
    if is_semver_like(exact) {
        Some(exact.to_string())
    } else {
        None
    }
}

fn inline_table_string_field(line: &str, key: &str) -> Option<String> {
    let key_pos = line.find(key)?;
    let after_key = &line[key_pos + key.len()..];
    let eq_pos = after_key.find('=')?;
    parse_quoted(after_key[eq_pos + 1..].trim())
}

fn parse_key_string(line: &str, key: &str) -> Option<String> {
    let prefix = format!("{key} = ");
    line.strip_prefix(&prefix)
        .and_then(|value| parse_quoted(value.trim()))
}

fn parse_quoted_array_item(line: &str) -> Option<String> {
    parse_quoted(line.trim_end_matches(',').trim())
}

fn parse_quoted(value: &str) -> Option<String> {
    let value = value.strip_prefix('"')?;
    let end = value.find('"')?;
    Some(value[..end].to_string())
}

fn is_semver_like(value: &str) -> bool {
    let mut parts = value.split('.');
    let Some(major) = parts.next() else {
        return false;
    };
    let Some(minor) = parts.next() else {
        return false;
    };
    let Some(patch_and_suffix) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    let patch = patch_and_suffix
        .split(['-', '+'])
        .next()
        .unwrap_or(patch_and_suffix);
    [major, minor, patch]
        .iter()
        .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
}
