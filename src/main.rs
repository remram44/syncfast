extern crate clap;
extern crate env_logger;
extern crate rrsync;

use clap::{App, Arg, SubCommand};
use std::env;
use std::path::Path;

use rrsync::{Error, Index};

/// Command-line entrypoint
fn main() {
    // Parse command line
    let cli = App::new("rrsync")
        .bin_name("rrsync")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .help("Augment verbosity (print more details)")
                .multiple(true),
        )
        .subcommand(
            SubCommand::with_name("index")
                .about("Index a file or directory")
                .arg(
                    Arg::with_name("path")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("index-file")
                        .short("x")
                        .takes_value(true)
                        .default_value("rrsync.idx"),
                ),
        );

    let mut cli = cli;
    let matches = match cli.get_matches_from_safe_borrow(env::args_os()) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(2);
        }
    };

    // Set up logging
    {
        let level = match matches.occurrences_of("verbose") {
            0 => log::LevelFilter::Warn,
            1 => log::LevelFilter::Info,
            2 => log::LevelFilter::Debug,
            _ => log::LevelFilter::Trace,
        };
        let mut logger_builder = env_logger::builder();
        logger_builder.filter(None, level);
        if let Ok(val) = env::var("RRSYNC_LOG") {
            logger_builder.parse_filters(&val);
        }
        if let Ok(val) = env::var("RRSYNC_LOG_STYLE") {
            logger_builder.parse_write_style(&val);
        }
        logger_builder.init();
    }

    let res = match matches.subcommand_name() {
        Some("index") => || -> Result<(), Error> {
            let s_matches = matches.subcommand_matches("index").unwrap();
            let index_filename = s_matches.value_of_os("index-file").unwrap();
            let index_filename = Path::new(index_filename);
            let path = Path::new(s_matches.value_of_os("path").unwrap());

            let mut index = Index::open(index_filename.into())?;
            let mut index_tx = index.transaction()?;
            index_tx.index_path(path)?;
            index_tx.remove_missing_files(path)?;
            index_tx.commit()?;

            Ok(())
        }(),
        _ => {
            cli.print_help().expect("Can't print help");
            std::process::exit(2);
        }
    };

    if let Err(e) = res {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
