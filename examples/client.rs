#[macro_use]
extern crate clap;

#[tokio::main]
async fn main() {
    let matches = clap_app!(get_config =>
        (version: "0.1")
        (@arg ENDPOINT: +takes_value "xbus endpoint")
        (@arg CONFIG_KEY: +takes_value "config key")
        (@arg APP: --app "dev app")
    )
    .get_matches();

    let mut config = xbus::Config::new(matches.value_of("ENDPOINT").unwrap());
    config.insecure = true;
    config.dev_app = Some(matches.value_of("APP").unwrap_or("clitest").to_string());
    let client = xbus::Client::new(config).expect("create client fail");

    let item = client
        .get(matches.value_of("CONFIG_KEY").unwrap())
        .await
        .expect("get config fail");
    println!("{}", item.value);
}
