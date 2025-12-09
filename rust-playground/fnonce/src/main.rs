trait Animal {
    fn make_sound(&self);
}

trait Handler: Animal {
    fn handle(&self);
}

struct Dog {
    name: String,
    tail_length: i32,
}

impl Animal for Dog {
    fn make_sound(&self) {
        println!("Woof");
    }
}

impl Handler for Dog {
    fn handle(&self) {
        println!("Handling dog: {}", self.name);
    }
}

struct Cat {
    name: String,
    toy: String,
}

impl Handler for Cat {
    fn handle(&self) {
        println!("Handling cat: {}", self.name);
    }
}

impl Animal for Cat {
    fn make_sound(&self) {
        println!("Mew");
    }
}

fn register(constructor: impl FnOnce() -> Box<dyn Animal>) -> Box <dyn Animal> {
    let a= constructor();
    a.handle();
    a
}

fn main() {
    let name = "Fafik".to_string();
    let tail_length= 10;

    let d1 = register(|| {
        Box::new(Dog {name, tail_length})
    });
    d1.make_sound();

    let c1 = register(|| {
        Box::new(Cat {name: "Filemon".to_string(), toy: "Ball".to_string()})
    });

    c1.make_sound();
}
