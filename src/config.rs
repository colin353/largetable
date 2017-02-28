/*
    config.rs

    This file contains methods relating to loading
    configuration files.
*/

use std::io;
use std::fmt;
use std::env;
use std::fs::File;
use serde_yaml;
use serde_json;

#[derive(Debug, Deserialize)]
pub enum Mode {
    Production,
    Testing
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                Mode::Production => "production",
                Mode::Testing => "testing"
            }
        )
    }
}

#[derive(Debug, Deserialize)]
pub struct ApplicationConfig {
    #[serde(default="default_mode")]
    pub mode: Mode,
    #[serde(default="default_port")]
    pub port: u32,
    #[serde(default="default_directory")]
    pub datadirectory: String,
    #[serde(default="default_memtable_size_limit")]
    pub memtable_size_limit: usize,
    #[serde(default="default_disktable_limit")]
    pub disktable_limit: usize
}

// These three functions set the default values of the config
// values.
fn default_mode() -> Mode { Mode::Production }
fn default_port() -> u32 { 8080 }
fn default_directory() -> String { String::from("./data") }
fn default_memtable_size_limit() -> usize { 32 * (1 << 20) }
fn default_disktable_limit() -> usize { 2 }

impl ApplicationConfig {
    // This function will try to read the given filename, decode the
    // contents as YAML, and read it into an ApplicationConfig struct.
    pub fn from_yaml(filename: &str) -> Result<ApplicationConfig, io::Error> {
        let mut config: ApplicationConfig = match File::open(filename) {
            Ok(f)   => serde_yaml::from_reader(f).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("failed to parse YAML in config file: {}", e)))?,

            // If the config.yml file doesn't exist, we'll just use a default-valued
            // configuration struct.
            Err(_)  => serde_json::from_str("{}").unwrap()
        };

        // We also want to override the parameters with environment
        // variables, so here we'll do that.
        if let Ok(value) = env::var("LARGETABLE_MODE") {
            if value.to_lowercase() == "production" {
                config.mode = Mode::Production
            }
            else if value.to_lowercase() == "testing" {
                config.mode = Mode::Testing
            }
            else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid value specified for LARGETABLE_MODE."));
            }
        }

        if let Ok(value) = env::var("LARGETABLE_PORT") {
            config.port = value.parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid value specified for LARGETABLE_PORT."))?;
        }

        if let Ok(value) = env::var("LARGETABLE_DATADIRECTORY") {
            config.datadirectory = value;
        }

        if let Ok(value) = env::var("LARGETABLE_DISKTABLE_LIMIT") {
            config.disktable_limit = value.parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid value specified for LARGETABLE_DISKTABLE_LIMIT."))?;
        }

        if let Ok(value) = env::var("LARGETABLE_MEMTABLE_SIZE_LIMIT") {
            config.memtable_size_limit = value.parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid value specified for LARGETABLE_MEMTABLE_SIZE_LIMIT."))?;
        }

        Ok(config)
    }
}
