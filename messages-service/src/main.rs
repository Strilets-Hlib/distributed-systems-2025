use std::{io::{prelude::*, BufReader}, net::{TcpListener, TcpStream}};
use http_reader::HttpReader;
use clap::Parser;


use rand::seq::SliceRandom;


use consulrs::api::{check::common::AgentServiceCheckBuilder, kv::requests::ReadKeyRequestBuilder};
use consulrs::api::service::requests::RegisterServiceRequest;
use consulrs::service;
use std::convert::TryInto;
use consulrs::kv;
use consulrs::client::{ConsulClient, ConsulClientSettingsBuilder};

#[derive(Parser,Default,Debug)]
struct Arguments {
    #[arg(short = 'p', long = "port", value_name = "PORT of messages service")]
    pub port : u16,
    #[arg(value_name = "IP of messages service")]
    pub ip: Option<String>,
    #[arg(long, short = 'd', action)]
    pub debug: bool,

    #[arg(long, value_name = "consul adress OPTIONAL")]
    pub consul_address: Option<String>,

    #[clap(long = "non_consume", action=clap::ArgAction::SetFalse, value_name = "Non consume flag, used to tell the Consumer to leave messages be")]
    pub consume: bool,
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();
    if args.debug{
        println!("{:?}",&args)
    }
    
    let consul_adress = (args.consul_address).map_or("http://127.0.0.1:8500".to_owned(), |v| v);
        let client = ConsulClient::new(
            ConsulClientSettingsBuilder::default()
                .address(&consul_adress)
                .build()
                .unwrap()
        ).unwrap();
    
    let service_name = "messages-service"; //service name


    
    let message_service_port = args.port;
    let message_service_ip = args.ip.as_ref().map_or("127.0.0.1", |v| v);
    let message_adress: String = format!("{}:{}", message_service_ip, message_service_port);
    let listener = TcpListener::bind(message_adress).unwrap();


    service::register(
        &client,
        service_name,
        Some(
            RegisterServiceRequest::builder()
                .id(format!("{}",message_service_port))
                .address(message_service_ip)
                .port(message_service_port)
                .check(
                    AgentServiceCheckBuilder::default()
                        .name("health_check")
                        .interval("10s")
                        .http(format!("http://{message_service_ip}:{message_service_port}/get/health"))
                        .status("passing")
                        .build()
                        .unwrap(),
                )
                
                
                ,
        ),
    )
    .await.expect("messages service relies on consul agent registration");
    
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream, args.debug, &client, args.consume).await;
    }
}

async fn handle_connection(mut stream: TcpStream, show_debug: bool, client: &ConsulClient, consume: bool){
    let mut buf_reader = BufReader::new(&stream);
    let mut line_consumer = HttpReader::new(&mut buf_reader);
    let request = line_consumer.make_request();
    if request.method() == &http::Method::GET && request.uri().path().split("/").collect::<Vec<_>>().get(2) == Some(&"health"){
        stream.write_all("HTTP/1.1 200 OK\r\n\r\n".to_owned().as_bytes()).unwrap();
        return;
    }
    if show_debug {
        println!("{:?}", request);
        //println!("{:#?}", consul.get_all_registered_service_names(None));
    }


    match *request.method(){
        http::Method::GET => {
            let mut response = "HTTP/1.1 501 Not Implemented\r\n\r\n".to_string();
            let mut messages_found = vec![];


            let mut consume_from = 
                match kv::read(client, "kafka_address", 
                        Some(&mut ReadKeyRequestBuilder::default().recurse(true))).await{

                            Ok(consume_from) => {
                                consume_from.response.iter().filter_map(|response| response.value.clone())
                                .flat_map(TryInto::<String>::try_into)
                                .collect()
                            },
                            Err(_) => vec![],
                        };
                
                //let logging_adresses = serde_json::from_str(&logging_adresses.unwrap_or("".to_owned())).unwrap_or(Vec::<String>::new());
                consume_from.shuffle(&mut rand::rng());

        if show_debug {
            println!("consume from adress(es) found: {:?}", consume_from);
        }

        
        let kafka_topic =
        match kv::read(client, "kafka_topic", 
        Some(&mut ReadKeyRequestBuilder::default())).await{
            Ok(kafka_topic) => {
                kafka_topic.response.iter().filter_map(|response| response.value.clone())
                .flat_map(TryInto::<String>::try_into)
                .collect()
            },
            Err(_) => "".to_owned()
        };
            match consume_from.first(){
                Some(address) => {

                //let mut consume_from = consume_from.clone();
                use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
                match Consumer::from_hosts(vec!(address.to_string()))
                    .with_topic(kafka_topic.to_owned())
                    .with_fallback_offset(FetchOffset::Earliest)
                    .with_group("messages-service".to_owned())
                    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
                    .create(){
                        Ok(mut consumer) => {

                            if let Some(mss) = consumer.poll().iter().next() {
                                if let Some(ms) = mss.iter().next() {
                                    let partition = ms.partition();
                                    //for message in 
                                    if let Some(message) = ms.messages().get(1){
                                        let key = match String::from_utf8(message.key.to_vec()){
                                            Ok(key) => key,
                                            Err(err) => format!("Instead of key found: {}", err),
                                        };
    
                                        let value = match String::from_utf8(message.value.to_vec()){
                                            Ok(value) => value,
                                            Err(err) => format!("Instead of value found: {}", err),
                                        };
                                        let res_message = format!("{}: {}", key, value);
                                        if show_debug{
                                            println!("{res_message}");
                                        }
                                        messages_found.push(res_message);
                                        if consume{
                                            consumer.consume_message(&kafka_topic, partition, message.offset).unwrap();
                                            consumer.commit_consumed().unwrap();
                                        }
                                    }
                                }
                            }
                        if show_debug{
                            println!("Found messages: {:#?}", &messages_found);
                        }
                        if !messages_found.is_empty(){
                            let messages_sent = serde_json::to_string(&messages_found);
                            let string_used = match messages_sent{
                                Ok(good) => good,
                                Err(er) => er.to_string(),
                            };
                            response = format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n{}", string_used.len(), string_used);
                        }
                        },
                        Err(err) => {
                            let err = err.to_string();
                            if show_debug{
                                println!("Error when trying to create consumer{:#?}", &err);
                            }
                            response = format!("HTTP/1.1 404 NOT FOUND\nContent-Length: {}\n\n{}", err.len(), err);
                        },
                    }

                    //.unwrap();
                },
                None => {
                    let not_found_kafka = "Not found kafka adresses";
                    response = format!("HTTP/1.1 404 NOT FOUND\nContent-Length: {}\n\n{}", not_found_kafka.len(), not_found_kafka);
                },
            }
            consume_from.is_empty();

            
            stream.write_all(response.to_string().as_bytes());//.unwrap();
        }
        _ =>{
            
            let response = "HTTP/1.1 501 Not Implemented\r\n\r\n";
            stream.write_all(response.as_bytes()).unwrap();
        }
    }

}
