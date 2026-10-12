/// Recognize unsupported Windows switches without rejecting drive, UNC or
/// rooted directory paths. Both executables accept their documented `--flags`.
pub fn is_windows_style_flag(arg: &str) -> bool {
    let Some(value) = arg.strip_prefix('/') else {
        return false;
    };
    let name = value.split([':', '=']).next().unwrap_or("");
    !name.is_empty() && !name.contains(['/', '\\'])
}
