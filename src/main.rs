use pickem::database;

#[tokio::main]
async fn main() {
    let mut path = dirs::config_dir().expect("unable to find config directory");
    path.push("pickem/pickem.db");
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).unwrap();
        }
    }

    let pool = database::connect(&path).await.expect(&format!(
        "failed to connect to database at: {}",
        &path.display()
    ));

    match database::seed_data(&pool).await {
        Ok(_) => (),
        Err(e) => println!("Error seeding database: {}", e),
    }
}
