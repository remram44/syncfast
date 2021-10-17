use tokio::fs::File;

#[cfg(unix)]
pub fn take_stdout() -> Result<File, ()> {
    use std::os::unix::io::FromRawFd;

    // Make a copy of stdout
    let new_stdout = nix::unistd::dup(1).map_err(|_| ())?;
    let new_stdout = unsafe { File::from_raw_fd(new_stdout) };

    // Open /dev/null
    let mode = {
        use nix::sys::stat::Mode;
        Mode::S_IRUSR | Mode::S_IWUSR
        | Mode::S_IRGRP | Mode::S_IWGRP
        | Mode::S_IROTH | Mode::S_IWOTH
    };
    let dev_null = nix::fcntl::open(
        "/dev/null",
        nix::fcntl::OFlag::O_APPEND | nix::fcntl::OFlag::O_WRONLY,
        mode,
    ).map_err(|_| ())?;

    // Replace fd 1 with /dev/null
    nix::unistd::dup2(dev_null, 1).map_err(|_| ())?;

    Ok(new_stdout)
}
