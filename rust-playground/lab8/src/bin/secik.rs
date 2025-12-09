use std::collections::HashSet;

fn main() {
    let mut secik = HashSet::new();
    secik.insert(String::from("msg"));
    secik.insert(String::from("msg2"));
    secik.insert(String::from("msg3"));

    for key in secik.iter() {
        println!("{key}");
    }

    println!("");
    let s = String::from("msg3");
    secik.remove(&s);

    for key in secik.iter() {
        println!("{key}");
    }


}