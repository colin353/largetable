/*
    logger.rs

    An implementation of the logger, which prints out info/debug
    information.
*/

use time;
use log;
use log::{LogRecord, LogMetadata, SetLoggerError, LogLevelFilter};

pub struct ApplicationLogger;

impl log::Log for ApplicationLogger {
    fn enabled(&self, _: &LogMetadata) -> bool { true }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            println!("[{}] {}", time::strftime("%b %d, %I:%M:%S%P", &time::now()).unwrap(), record.args());
        }
    }
}

impl ApplicationLogger {
    pub fn init() -> Result<(), SetLoggerError> {
        log::set_logger(|max_log_level| {
            max_log_level.set(LogLevelFilter::Info);
            Box::new(ApplicationLogger)
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn can_use_logger() {
        super::ApplicationLogger::init().unwrap();
        info!("I hope that this log works!");
        warn!("Warning message!");
        error!("Error message!");
    }
}
