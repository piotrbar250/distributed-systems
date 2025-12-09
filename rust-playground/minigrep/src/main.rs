use std::{env, fs};

fn main() {
    let args: Vec<String> = env::args().collect();
    
    let query = &args[1];
    let file_path = &args[2];
    
    println!("Searching for {query}");
    println!("In the file {file_path}");

    let contents = fs::read_to_string(file_path).unwrap();
    println!("{contents}");
}
