#[path = "../import/mod.rs"]
mod import;

fn main() {
    if let Err(error) = import::run(std::env::args().skip(1).collect()) {
        eprintln!("Import failed: {error}");
        std::process::exit(1);
    }
}
