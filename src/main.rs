extern crate clap;
extern crate env_logger;
extern crate syncfast;

use clap::{App, Arg, SubCommand};
use std::env;
use std::path::Path;

use syncfast::{Error, Index};
use syncfast::locations::Location;
use syncfast::sync::do_sync;

/// Command-line entrypoint
fn main() {
    // Parse command line
    let cli = App::new("syncfast")
        .bin_name("syncfast")
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
                        .default_value(".syncfast.idx"),
                ),
        )
        .subcommand(
            SubCommand::with_name("sync")
                .about("Copy files")
                .arg(
                    Arg::with_name("source")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("destination")
                        .required(true)
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("remote-recv")
                .about(
                    "Internal - process started on the remote to receive \
                     files. Expects stdin and stdout to be connected to the \
                     sender process",
                )
                .arg(
                    Arg::with_name("destination")
                        .required(true)
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("remote-send")
                .about(
                    "Internal - process started on the remote to send \
                     files. Expects stdin and stdout to be connected to \
                     the receiver process",
                )
                .arg(
                    Arg::with_name("source")
                        .required(true)
                        .takes_value(true),
                ),
        );

    let mut cli = cli;
    let matches = match cli.get_matches_from_safe_borrow(env::args_os()) {
        Ok(m) => m,
        Err(e) => {
            e.exit();
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
        if let Ok(val) = env::var("SYNCFAST_LOG") {
            logger_builder.parse_filters(&val);
        }
        if let Ok(val) = env::var("SYNCFAST_LOG_STYLE") {
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

            let mut index = Index::open(index_filename)?;
            let mut index_tx = index.transaction()?;
            index_tx.index_path(path)?;
            index_tx.remove_missing_files(path)?;
            index_tx.commit()?;

            Ok(())
        }(),
        Some("sync") => {
            let s_matches = matches.subcommand_matches("sync").unwrap();
            let source = s_matches.value_of_os("source").unwrap();
            let dest = s_matches.value_of_os("destination").unwrap();

            let source = match source.to_str().and_then(Location::parse) {
                Some(s) => s,
                None => {
                    eprintln!("Invalid source");
                    std::process::exit(2);
                }
            };
            let dest = match dest.to_str().and_then(Location::parse) {
                Some(Location::Http(_)) => {
                    eprintln!("Can't write to HTTP destination, only read");
                    std::process::exit(2);
                }
                Some(s) => s,
                None => {
                    eprintln!("Invalid destination");
                    std::process::exit(2);
                }
            };

            let mut source_wrapper: Box<dyn syncfast::sync::SourceWrapper> =
                match source.open_source() {
                    Ok(o) => o,
                    Err(e) => {
                        eprintln!("Failed to open source: {}", e);
                        std::process::exit(1);
                    }
                };
            let source_obj: Box<dyn syncfast::sync::Source> =
                match source_wrapper.open() {
                    Ok(o) => o,
                    Err(e) => {
                        eprintln!("Failed to prepare source: {}", e);
                        std::process::exit(1);
                    }
                };
            let mut sink_wrapper: Box<dyn syncfast::sync::SinkWrapper> =
                match dest.open_sink() {
                    Ok(o) => o,
                    Err(e) => {
                        eprintln!("Failed to open destination: {}", e);
                        std::process::exit(1);
                    }
                };
            let sink_obj: Box<dyn syncfast::sync::Sink> =
                match sink_wrapper.open() {
                    Ok(o) => o,
                    Err(e) => {
                        eprintln!("Failed to prepare destination: {}", e);
                        std::process::exit(1);
                    }
                };
            do_sync(source_obj, sink_obj)
        }
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
