use std::{env, net::SocketAddr, path::PathBuf};

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub data_dir: PathBuf,
}

impl Config {
    pub fn from_env() -> Self {
        let bind_addr = env::var("SERVERLESS_DB_BIND")
            .ok()
            .and_then(|value| value.parse().ok())
            .or_else(|| {
                env::var("PORT")
                    .ok()
                    .and_then(|value| value.parse::<u16>().ok())
                    .map(|port| SocketAddr::from(([0, 0, 0, 0], port)))
            })
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 8080)));

        let data_dir = env::var("SERVERLESS_DB_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data"));

        Self {
            bind_addr,
            data_dir,
        }
    }
}
