use std::net::{UdpSocket, SocketAddr};
use std::io;
use std::net;
use std::str;


#[derive(Debug)]
pub enum StatsdError {
    IoError(io::Error),
    AddrParseError(String),
}

pub type StatsdResult<T> = std::result::Result<T, StatsdError>;


impl From<net::AddrParseError> for StatsdError {
    fn from(_: net::AddrParseError) -> StatsdError {
        StatsdError::AddrParseError("Address parsing error".to_string())
    }
}

impl From<io::Error> for StatsdError {
    fn from(err: io::Error) -> StatsdError {
        StatsdError::IoError(err)
    }
}

#[derive(Debug)]
pub struct StatsdClient {
	socket: UdpSocket,
	prefix: String,
}

#[derive(Debug, Copy, Clone)]
pub enum MetricType {
    Counter,
}


impl StatsdClient {
    pub fn new(host: SocketAddr) -> StatsdResult<StatsdClient> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.connect(host)?;

        Ok(StatsdClient {
            socket,
            prefix: String::new(),
        })
    }

    pub fn set_prefix(&mut self, prefix: &str) {
    	self.prefix = format!("{}.", prefix);
    }

    pub fn incr_by(&self, bucket: &str, value: f64) -> StatsdResult<()> {
        self.datagram(bucket, value, MetricType::Counter)
    }

    pub fn incr(&self, bucket: &str) -> StatsdResult<()> {
    	self.incr_by(bucket, 1.)
    }

    pub fn decr(&self, bucket: &str) -> StatsdResult<()> {
    	self.incr_by(bucket, -1.)
    }

    pub fn decr_by(&self, bucket: &str, value: f64) -> StatsdResult<()> {
    	self.incr_by(bucket, value * -1.)
    }

    pub fn datagram(&self, bucket: &str, value: f64, metric_type: MetricType) -> StatsdResult<()> {
        let metric_type_string = match metric_type {
            MetricType::Counter => 'c',
        };

        let data = format!("{}{}:{}|{}", self.prefix, bucket, value, metric_type_string);

        match self.socket.send(data.as_bytes()) {
            Ok(_) => Ok(()),
            Err(e) => Err(StatsdError::from(e)),
        }
    }
}


#[cfg(test)]
mod statsd_client_tests {
	use super::*;

    fn get_socket_and_client() -> (UdpSocket, StatsdClient) {
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let mut client = StatsdClient::new(socket.local_addr().unwrap()).unwrap();
        client.set_prefix("pfx");
        (socket, client)
    }

    fn get_message(s: &UdpSocket) -> String {
        let mut buf = [0; 100];
        let n_bytes = s.recv(&mut buf).unwrap();
        str::from_utf8(&buf[..n_bytes]).unwrap().to_owned()
    }

    #[test]
    fn incr() {
        let (socket, client) = get_socket_and_client();

        client.incr("bkt").unwrap();

        assert!(get_message(&socket) == "pfx.bkt:1|c");
    }

    #[test]
    fn incr_by() {
        let (socket, client) = get_socket_and_client();

        client.incr_by("bkt", 12.).unwrap();

        assert!(get_message(&socket) == "pfx.bkt:12|c");
    }

    #[test]
    fn decr() {
        let (socket, client) = get_socket_and_client();

        client.decr("bkt").unwrap();

        assert!(get_message(&socket) == "pfx.bkt:-1|c");
    }

    #[test]
    fn decr_by() {
        let (socket, client) = get_socket_and_client();

        client.decr_by("bkt", 12.).unwrap();

        assert!(get_message(&socket) == "pfx.bkt:-12|c");
    }
}
