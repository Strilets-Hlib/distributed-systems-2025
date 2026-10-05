use std::{env, io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}, str::FromStr};

use http_reader::HttpReader;

use uuid::Uuid;
use hazelcast_rest::HazelcastRestClient;
use std::process::Command;

use rand::seq::SliceRandom;
use clap::Parser;


use consulrs::api::kv::requests::ReadKeyRequestBuilder;
use consulrs::api::service::requests::RegisterServiceRequest;
use consulrs::service;
use std::convert::TryInto;
use consulrs::kv;
use consulrs::client::{ConsulClient, ConsulClientSettingsBuilder};


#[derive(Parser,Default,Debug)]
struct Arguments {
    #[arg(short = 'p', long = "port", value_name = "PORT of logging service")]
    pub port : u16,
    #[arg(value_name = "IP of logging service")]
    pub ip: Option<String>,
    #[arg(long, short = 'd', action)]
    pub debug: bool,
    
    #[arg(long, value_name = "consul adress OPTIONAL")]
    pub consul_address: Option<String>
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();
    let consul_adress = (args.consul_address).map_or("http://127.0.0.1:8500".to_owned(), |v| v);


        let client = ConsulClient::new(
            ConsulClientSettingsBuilder::default()
                .address(&consul_adress)
                .build()
                .unwrap()
        ).unwrap();
    let service_name = "logging-service"; //service name
    let logging_service_port = args.port;
    let logging_service_ip = args.ip.as_ref().map_or("127.0.0.1", |v| v);
    let logging_adress: String = format!("{}:{}", logging_service_ip, logging_service_port);
    let listener = TcpListener::bind(logging_adress).unwrap();
    service::register(
        &client,
        service_name,
        Some(
            RegisterServiceRequest::builder()
                .id(format!("{service_name}-{logging_service_port}"))
                .name(format!("{service_name}-{logging_service_port}"))
                .address(logging_service_ip)
                .port(logging_service_port)
                
                .check(
                    consulrs::api::check::common::AgentServiceCheckBuilder::default()
                        .name("health_check")
                        .interval("10s")
                        .http(format!("http://{logging_service_ip}:{logging_service_port}/get/health"))
                        .status("passing")
                        .build()
                        .unwrap(),
                )
        ),
    )
    .await.expect("messages service relies on consul agent registration");
    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, args.debug, &client).await;
    }
}

async fn handle_connection(mut stream: TcpStream, show_debug: bool, client: &ConsulClient){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();

    if request.method() == &http::Method::GET && request.uri().path().split("/").collect::<Vec<_>>().get(2) == Some(&"health"){
        stream.write_all("HTTP/1.1 200 OK\r\n\r\n".to_owned().as_bytes()).unwrap();
        //stream.write_all("HTTP/1.1 429\r\n\r\n".to_owned().as_bytes()).unwrap();
        return;
    }
    let response = "HTTP/1.1 401 Not Implemented\r\n\r\n".to_owned();


    let (hazelcast_map, hazelcast_ip, hazelcast_port) =
    match *request.method() {

    http::Method::POST | http::Method::GET =>{
        let hazelcast_adress = kv::read(client, "hazelcast_adress", 
        Some(&mut ReadKeyRequestBuilder::default().recurse(true))).await;
        
        let hazelcast_adress =
        match hazelcast_adress{
            Ok(hazelcast_adress) => {
                let mut adresses = hazelcast_adress.response.iter().filter_map(|response| response.value.clone())
                .flat_map(TryInto::<String>::try_into)
                .collect::<Vec<String>>();
                adresses.shuffle(&mut rand::rng());
                adresses.first().unwrap_or(&"".to_owned()).clone()
            },
            Err(_) => "".to_owned()
        };
        let (hazelcast_ip, hazelcast_port) ={
            let split:Vec<String> = hazelcast_adress.split(":").map(|str_| str_.to_owned()).collect();
            (split.first().map(|val| val.to_owned()), split.get(1).map(|val| val.to_owned()))
        };

        let hazelcast_map = 
        match kv::read(client, "hazelcast_map", 
                Some(&mut ReadKeyRequestBuilder::default().recurse(true))).await{
    
                    Ok(hazelcast_map) => {
                        let mut adresses = hazelcast_map.response.iter().filter_map(|response| response.value.clone())
                        .flat_map(TryInto::<String>::try_into)
                        .collect::<Vec<String>>();
                        adresses.shuffle(&mut rand::rng());
                        adresses.first().cloned()
                    },
                    Err(_) => None,
                };
        
        (hazelcast_map, hazelcast_ip, hazelcast_port)
    }
    _ =>{
        stream.write_all(response.as_bytes()).unwrap();
        return;
    }
    };

    match *request.method(){
        http::Method::POST => {
            if let (Some(body), Some(hazelcast_map), Some(hazelcast_ip), Some(hazelcast_port)) = (request.body(), hazelcast_map, hazelcast_ip, hazelcast_port){
            
                let values: Vec<&str> = body.split(": ").take(2).collect();
                if values.len() == 2{
                    if Uuid::from_str(values[0]).is_ok(){
                        
                        let client = HazelcastRestClient::new(&hazelcast_ip, hazelcast_port);

                        let res = client.map_put(&hazelcast_map,&Into::<String>::into(values[0]), &vec![("Content-Type", "plain/text"), ("factoryId", "-1")], values[1]);
                        if show_debug {
                            println!("{res:?}");
                        }

                        match res{
                            Ok(response) => {
                                stream.write_all((format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}",response.len(), response)).as_bytes()).unwrap();
                            },
                            Err(_) => {
                                let response = "HTTP/1.1 500 Internal Server Error\r\n\r\n";
                                stream.write_all(response.as_bytes()).unwrap();
                            },
                        }
                        
                    }
                }else{
                    let response = "HTTP/1.1 500 Internal Server Error\r\n\r\n";
                    stream.write_all(response.as_bytes()).unwrap();
                }
            }else{
                
                let response = "HTTP/1.1 401\r\n\r\n";
                stream.write_all(response.as_bytes()).unwrap();
            }
        }
        http::Method::GET => {
            if let (Some(hazelcast_map), Some(hazelcast_ip), Some(hazelcast_port)) = (hazelcast_map, hazelcast_ip, hazelcast_port){
                
                let path = format!("{}:{}",&hazelcast_ip,  &hazelcast_port);
                let cluster_name = 
                match kv::read(client, "hazelcast_cluster_name", 
                        Some(&mut ReadKeyRequestBuilder::default().recurse(true))).await{
            
                            Ok(cluster_name) => {
                                cluster_name.response.iter().filter_map(|response| response.value.clone())
                                .flat_map(TryInto::<String>::try_into)
                                .collect::<Vec<String>>().first().cloned()
                            },
                            Err(_) => None,
                        };
                
                let output = Command::new("zsh")

                .args(["run_get_data.sh", "--ip", &path, "--cluster_name", &cluster_name.map_or("".to_owned(), |v| v.to_owned()), "--map_name", &hazelcast_map])
                .output();
                if show_debug{
                    let path = env::current_dir().unwrap();
                    println!("The current directory is {}", path.display());
                    println!("Output of runnig the script is: {:?}", output);
                }
                if let Ok(output) = output{
                    match core::str::from_utf8(&output.stdout){
                        Ok(output) =>{
                            let response = format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", output.len(), output);
                            stream.write_all(response.as_bytes()).unwrap();
                        }
                        Err(er) =>{
                            let response = format!("HTTP/1.1 500 Internal Server Error\nContent-Length: {}\n\n{}", &er.to_string().len(), &er.to_string());
                            stream.write_all(response.as_bytes()).unwrap();
                        }
                    }
                }
            }
        }
        _ =>{
            
            let response = "HTTP/1.1 501 Not Implemented\r\n\r\n";
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

}

