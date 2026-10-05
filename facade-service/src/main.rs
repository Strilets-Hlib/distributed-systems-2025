use std::{
    io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}
};

use http_reader::HttpReader;
use rand::seq::SliceRandom;
//use reqwest::blocking::{Request, RequestBuilder};
use reqwest::blocking::Client;
use uuid::Uuid;

use clap::Parser;
//use rs_consul::{types::*, Config, Consul};

use consulrs::api::{check::common::AgentServiceCheckBuilder, kv::requests::ReadKeyRequestBuilder};
use consulrs::api::service::requests::RegisterServiceRequest;
use consulrs::service;
use std::convert::TryInto;
use consulrs::kv;
use consulrs::client::{ConsulClient, ConsulClientSettingsBuilder};

#[derive(Parser, Default,Debug)]
struct Arguments {
    #[arg(short = 'p', long)]
    pub port : Option<u64>,
    
    #[arg(value_name = "IP of facade service")]
    pub ip: Option<String>,
    
    #[arg(long, short = 'd', action)]
    pub debug: bool,

    #[arg(long, value_name = "consul adress OPTIONAL")]
    pub consul_address: Option<String>,
    
}
#[tokio::main]
async fn main() {
    let args = Arguments::parse();
    let port = args.port.unwrap_or(7878);
    if args.debug{
        println!("using port {port}")
    }

    let consul_adress = (args.consul_address).map_or("http://127.0.0.1:8500".to_owned(), |v| v);
        let client = ConsulClient::new(
            ConsulClientSettingsBuilder::default()
                .address(&consul_adress)
                .build()
                .unwrap()
        ).unwrap();

    let consul_config = rs_consul::Config {
            address: consul_adress.to_string(), 
            token: None, // No token required in development mode
            ..Default::default() // Uses default values for other settings
    };
    let rs_consul_loc = rs_consul::Consul::new(consul_config);
        
    let service_name = "facade-service"; //service names

    let facade_service_port = args.port.unwrap_or(8362);
    let facade_service_ip = args.ip.as_ref().map_or("127.0.0.1", |v| v);

    let facade_address: String = format!("127.0.0.1:{}", &port);
    let listener = TcpListener::bind(facade_address).unwrap();

    service::register(
        &client,
        service_name,
        Some(
            RegisterServiceRequest::builder()
                
                //.features(FeaturesBuilder::default().filter("not Checks.Status != passing".to_string()).build().unwrap())
                .id(format!("{}",facade_service_port))
                .address(facade_service_ip)
                .port(facade_service_port)
                .check(
                    AgentServiceCheckBuilder::default()
                        
                        .name("health_check")
                        .interval("10s")
                        .http(format!("http://{facade_service_ip}:{facade_service_port}/get/health"))
                        .status("passing")
                        .build()
                        .unwrap(),
                )
        ),
    )
    .await.expect("messages service relies on consul agent registration");

    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream,  args.debug, &client, &rs_consul_loc).await//&args.server_config, &args.kafka_topic);
    }
}

async fn handle_connection(mut stream: TcpStream, debug: bool, client: &ConsulClient,  rs_consul_loc: &rs_consul::Consul){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if request.method() == &http::Method::GET && request.uri().path().split("/").collect::<Vec<_>>().get(2) == Some(&"health"){
        stream.write_all("HTTP/1.1 200 OK\r\n\r\n".to_owned().as_bytes()).unwrap();
        //stream.write_all("HTTP/1.1 429\r\n\r\n".to_owned().as_bytes()).unwrap();
        return;
    }

    if debug{println!("Request aquired:\n{:?}", request);}
    
    let mut response = "HTTP/1.1 401 Not Implemented\r\n\r\n".to_owned();

    let http_client = Client::new();
    let kafka_topic = 
    match request.method(){
        &http::Method::POST | &http::Method::GET =>{

            let kafka_topic = kv::read(client, "kafka_topic", 
            Some(&mut ReadKeyRequestBuilder::default())).await;
            
            
            match kafka_topic{
                Ok(kafka_topic) => {
                    kafka_topic.response.iter().filter_map(|response| response.value.clone())
                    .flat_map(TryInto::<String>::try_into)
                    .collect()
                },
                Err(_) => "".to_owned()
            }
        }
        _ =>{
            stream.write_all(response.as_bytes()).unwrap();
            return;
        }
    };
    let srv = consulrs::catalog::services(client, None).await;
    /*if let Ok(srv) = srv{
        println!("{:#?}",srv);
    }*/
    let logging_adresses: Vec<_> = {
        rs_consul_loc.get_service_addresses_and_ports("logging-service", None).await.unwrap()
        .iter().map(|(addr, port)| format!("{}:{}", addr, port)).collect()
    };

    let mut message_adresses: Vec<_> = {
        rs_consul_loc.get_service_addresses_and_ports("messages-service", None).await.unwrap()
        .iter().map(|(addr, port)| format!("{}:{}", addr, port)).collect()
    };
    message_adresses.shuffle(&mut rand::rng());

    let mut produce_targets = 
    match kv::read(client, "kafka_address", 
            Some(&mut ReadKeyRequestBuilder::default().recurse(true))).await{

                Ok(produce_targets) => {
                    produce_targets.response.iter().filter_map(|response| response.value.clone())
                    .flat_map(TryInto::<String>::try_into)
                    .collect()
                },
                Err(_) => vec![],
            };
    
    //let logging_adresses = serde_json::from_str(&logging_adresses.unwrap_or("".to_owned())).unwrap_or(Vec::<String>::new());
    produce_targets.shuffle(&mut rand::rng());
    if debug{
        println!("Logging adresses:\n{:#?}", &logging_adresses);
        println!("Message adresses:\n{:#?}", &message_adresses);
        println!("Kafka adresses:\n{:#?}", &produce_targets);
    }
    match *request.method(){
        http::Method::POST =>{
            
            response = "HTTP/1.1 500 Internal Server Error\r\n\r\n".to_owned();
            let mut code = "400 Bad Request";
            if let Some(body) = request.body(){
                
                let id = Uuid::new_v4();
                let mut http_result;// = Ok(String::default());
                for logging_adress in logging_adresses{
                    http_result = http_client
                    .post(format!("http://{}/post", logging_adress))
                    .body(format!("{id}: {body}")).send();
                    if debug{println!("Result of sending put request to logging at {}\n{:?}", &logging_adress, &http_result);}
                    match http_result{
                        Ok(resp) => {
                            match resp.text(){
                                Ok(val) => {
                                    code = "200 OK";
                                    response = val.clone();
                                    break;
                                },
                                Err(err) => response = err.to_string(),
                            }
                        },
                        Err(err) => response = err.to_string(),
                    }
                    };
                    {
                        //use std::fmt::Write;
                        use std::time::Duration;
                        use kafka::producer::{Producer, Record, RequiredAcks};
                        for produce_to in produce_targets{
                            match Producer::from_hosts(vec!(produce_to))
                                .with_ack_timeout(Duration::from_secs(1))
                                .with_required_acks(RequiredAcks::One)
                                .create(){
                                    Ok(mut producer) => {
                                        let res = producer.send(&Record::from_key_value(&kafka_topic, id.to_string(), body.clone()));
                                        
                                        //.send(&Record::from_value(kafka_topic, format!("{id}: {body}").as_bytes()));//.unwrap();
                                        match res{
                                            Ok(_) => {
                                                break},
                                            Err(err) => {
                                                if debug { println!("Failed to create a send a value: {:?}", err);}
                                            },
                                        }
                                    },
                                    Err(err) => {
                                        if debug{
                                            println!("Failed to create a producer {:?}", err);
                                        }
                                    },
                                }
                                //.unwrap();
                        }
                    }                    
                }



                stream.write_all(
                    format!("HTTP/1.1 {}\n\nContent-Length: {}\n\n{}",&code, response.len(), &response).as_bytes()
                ).unwrap();
            }
        
        http::Method::GET => {
            let http_client = Client::new();
            let mut http_result_logging = None ;
            for logging_adress in logging_adresses{
                http_result_logging = Some(http_client
                .get(format!("http://{}/get", logging_adress))
                .send());
                if debug{println!("Result of sending get request to loggig at {}\n{:?}", &logging_adress, &http_result_logging);}
                if let Some(res) =  &http_result_logging{
                    if res.is_ok(){
                        break
                    }
                }
            }
            
            let mut http_result_message = None ;

            if debug{
                println!("{:#?}", &message_adresses);
            }
            for message_adress in message_adresses{
                
                http_result_message = Some(http_client
                .get(format!("http://{}/get", message_adress))
                .send());
            if debug{println!("Result of sending get request to message at {}\n{:?}", &message_adress, &http_result_logging);}
            if let Some(res) =  &http_result_message{
                if let Ok(res) = res{
                    if debug{
                        println!("Response of the message {}", &res.status());
                    }
                    if res.status().is_success(){
                        println!("Is success");
                        break
                    }
                }
            }
            }
            match  (&http_result_logging, &http_result_message){
                (Some(_), Some(_)) => {},
                _ =>{
                    stream.write_all("HTTP/1.1 500 Internal Server Error\r\n\r\n".to_owned().as_bytes());
                    return
                }
            }
            let (http_result_logging_res, http_result_message_res) = (http_result_logging.expect("logging addresses were not provided"), http_result_message.expect("message adresses were not provided"));

            let response = 
            
            match (http_result_logging_res, http_result_message_res){
                (Ok(l), Ok(m)) => {
                    if debug{
                        println!("Responses:\n{:?} {:?}\r\n", &l, &m)
                    }
                    if let (Ok(text_1), Ok(text_2)) = (l.text(), m.text()){
                        if debug{
                            println!("{} {}\r\n", &text_1, &text_2)
                        }
                        let both_text = format!("logging: {}\n message: {}\n",  &text_1, &text_2);
                        Some(format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", both_text.len(), both_text))
                    }else{
                        None
                    }
                },
                (Ok(text_1), Err(message_error)) => {
                    println!("Message error: {}", message_error);
                    let both_text = format!("logging: {:#?}\n message: {}\n",  &text_1, &message_error);
                    Some(format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", both_text.len(), both_text))                
                },
                (Err(logging_error), Ok(test_2)) => {
                    println!("Logging error: {}", logging_error);
                    let both_text = format!("logging: {}\n message: {:#?}\n",  &logging_error, &test_2);
                    Some(format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", both_text.len(), both_text))
                },
                (Err(message_error), Err(logging_error)) => {
                    println!("Message error: {}", message_error);
                    println!("Logging error: {}", logging_error);
                    None
                },
            };
            stream.write_all(response.unwrap_or("HTTP/1.1 500 Internal Server Error\r\n\r\n".to_owned()).as_bytes()).unwrap();
            
        }
        _ =>{
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

    
}