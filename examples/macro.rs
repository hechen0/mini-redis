macro_rules! say_hello {
    () => {
        println!("nothing");
    };
    ($name:expr) => {
        println!("Hello {} with macro_rules!", $name);
    };
}

pub fn main() {
    say_hello!("world");
    let a: Vec<u8> = vec![0, 1, 3];
    println!("{:?}", a);
}
