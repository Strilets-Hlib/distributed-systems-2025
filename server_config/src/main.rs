use std::fs;
use serde::Deserialize;

use std::{
    io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}
};
use http_reader::HttpReader;
use rand::seq::SliceRandom;


use clap::Parser;
#[derive(Parser,Default,Debug)]
struct Arguments {
    #[arg(short = 'p', long)]
    pub port : i32,
    #[arg(short = 'f', long, value_name = "Filepath of a tolm file which includes an array of logging addresses and a single message adress")]
    pub filepath: Option<String>,
    #[arg(long, value_delimiter = ' ', num_args = 1..)]
    pub logging_list: Option<Vec<String>>,
    #[arg(long, num_args = 1..)]
    pub messages: Option<Vec<String>>,
    #[arg(long, num_args = 1..)]
    pub distributed_queue: Option<Vec<String>>,
    #[arg(long, short = 'd', action)]
    pub debug: bool
}
#[derive(Deserialize)]
struct Config{pub logging_adresses: Vec<String>, pub messages: Vec<String>, pub distributed_queue: Vec<String>}
fn main() {

    let args = Arguments::parse();
    if args.debug{ print!("{:?}", args); }
    if args.filepath.is_some() && (args.logging_list.is_some() || args.messages.is_some() || args.distributed_queue.is_some()){
            panic!("Filepath forbids usage of logging-list and message arguments because it reads both from a file");
    }

    if args.filepath.is_none() && (args.logging_list.is_none() || args.messages.is_none()|| args.distributed_queue.is_none()){
        panic!("Either provide --filepath -f with the tolm file or provide both --logging-list and --message");
}
    //return;
    
    let config =
    match (args.logging_list, args.messages, args.distributed_queue){
        (Some(logging_adresses),Some(messages), Some(distributed_queue)) => Config{logging_adresses, messages, distributed_queue},
        (None, None, None) => {
            match args.filepath{
                Some(path) => {
                    let contents = fs::read_to_string(&path)
                    .expect("Something went wrong reading the file");
                    toml::from_str(&contents)
                    .expect("Failed to parse TOML of the server configuration file")
                },
                None => panic!("No IPs for logging service were provided!"),
            }
        },
        _ =>{
            panic!("Violation of group rules of Arguments Parser");
        }
        
    };

    let port = args.port;
    let debug = args.debug;
    
    let config_adress: String = format!("127.0.0.1:{}", &port);
    let listener = TcpListener::bind(config_adress).unwrap();

    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream,  &config.logging_adresses, &config.messages, &config.distributed_queue, debug);
    }
}

fn handle_connection(mut stream: TcpStream, logging_adresses: &Vec<String>, messages: &Vec<String>, distributed_queue: &Vec<String>, debug:bool){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if debug{println!("{:?}", request);}
    
    let response;
    match *request.method(){
        http::Method::GET => {
            let collected: Vec<&str> = request.uri().path().split("/").collect();
            let target = collected.get(2);
            if debug{
                println!("{:?}",target);
            }
            if target.is_none(){
                response =  "HTTP/1.1 400 Bad Request\r\n\r\n".to_owned();
                stream.write_all(response.as_bytes()).unwrap();
                return;
            }
            let target = *(target.unwrap());
            match target{
                "logging" => {

                    let mut adresses_shuffled = logging_adresses.clone();
                    adresses_shuffled.shuffle(&mut rand::rng());
                    let sent_string = serde_json::to_string(&adresses_shuffled).unwrap();
                    if debug{
                        println!("Sending logging adresses {}", &sent_string);
                    }
                    //.unwrap_or("".to_owned());

                    response = format!("HTTP/1.1 200 OK\nContent-Type: plain/text\nContent-Length: {}\n\n{}", sent_string.len(), sent_string);
                }
                "message" => {
                    let mut adresses_shuffled = messages.clone();
                    adresses_shuffled.shuffle(&mut rand::rng());
                    let sent_string = serde_json::to_string(&adresses_shuffled).unwrap();
                    if debug{
                        println!("Sending message adresses {}", &sent_string);
                    }
                    //.unwrap_or("".to_owned());
                    response = format!("HTTP/1.1 200 OK\nContent-Type: plain/text\nContent-Length: {}\n\n{}", sent_string.len(), sent_string);
                },
                "queue" => {
                    let mut adresses_shuffled = distributed_queue.clone();
                    adresses_shuffled.shuffle(&mut rand::rng());
                    let sent_string = serde_json::to_string(&adresses_shuffled).unwrap();
                    if debug{
                        println!("Sending message adresses {}", &sent_string);
                    }
                    //.unwrap_or("".to_owned());
                    response = format!("HTTP/1.1 200 OK\nContent-Type: plain/text\nContent-Length: {}\n\n{}", sent_string.len(), sent_string);
                }
                _ =>{
                    response =  "HTTP/1.1 400 Bad Request\r\n\r\n".to_owned();
                }
            }
            let _ = stream.write_all(response.as_bytes());
        }
        _ =>{
            response = "HTTP/1.1 401 Not Implemented\r\n\r\n".to_owned();
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

    
}
