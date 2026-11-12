#[path = "../import/mod.rs"]
mod import;

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let (operation, result) = match args.as_slice() {
        [database, create, ..] if database == "database" && create == "create" => (
            "Database creation",
            import::create_database(args.into_iter().skip(2).collect()),
        ),
        _ => ("Import", import::run(args)),
    };
    if let Err(error) = result {
        eprintln!("{operation} failed: {error}");
        std::process::exit(1);
    }
}
