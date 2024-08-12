use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, StatusCode, Server};
pub use mysql_async::prelude::*;
pub use mysql_async::*;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::result::Result;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug)]
struct Architectures {
    arch_id: i32,
    name: String,
    status: bool,
    version: String,
    putch: String,
}

impl Architectures {
    fn new(
        arch_id: i32,
        name: String,
        status: bool,
        version: String,
        putch: String,
    ) -> Self {
        Self {
            arch_id,
            name,
            status,
            version,
            putch,
        }
    }
}

/// Получаю подключение к БД через переменные окружения
fn get_database_url() -> String {

    if let Ok(url) = std::env::var("DATABASE_URL") {
        let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
        if opts
            .db_name()
            .expect("database name is required")
            .is_empty()
        {
            panic!("database name is empty");
        }
        url
    } else {
        "mysql://localhost:3306/arch_db".into()
    }
}

/// Ассинхронная обработка запросов
async fn handle_request(req: Request<Body>, pool: Pool) -> Result<Response<Body>, anyhow::Error> {

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok(Response::new(Body::from(
            "Для работы с API сервиса используйте следующие endpoints: /init /create /update /delete /architectures"))),


        (&Method::OPTIONS, "/init") => Ok(response_build(&String::from(""))),
        (&Method::OPTIONS, "/create") => Ok(response_build(&String::from(""))),
        (&Method::OPTIONS, "/update") => Ok(response_build(&String::from(""))),
        (&Method::OPTIONS, "/delete") => Ok(response_build(&String::from(""))),
        (&Method::OPTIONS, "/architectures") => Ok(response_build(&String::from(""))),

        (&Method::GET, "/init") => {
            let mut conn = pool.get_conn().await.unwrap();
            "DROP TABLE IF EXISTS architectures;".ignore(&mut conn).await?;
            "CREATE TABLE architectures (arch_id INT, name VARCHAR(80), status INT, version VARCHAR(10), putch VARCHAR(10));".ignore(&mut conn).await?;
            drop(conn);
            Ok(response_build("{\"status\":true}"))
        }

        (&Method::POST, "/create") => {
            let mut conn = pool.get_conn().await.unwrap();

            let byte_stream = hyper::body::to_bytes(req).await?;
            let arch: Architectures = serde_json::from_slice(&byte_stream).unwrap();

            "INSERT INTO architectures (arch_id, name, status, version, putch) VALUES (:arch_id, :name, :status, :version, :putch)"
                .with(params! {
                    "arch_id" => arch.arch_id,
                    "name" => arch.name,
                    "status" => arch.status,
                    "version" => arch.version,
                    "putch" => arch.putch,
                })
                .ignore(&mut conn)
                .await?;

            drop(conn);
            Ok(response_build("{\"status\":true}"))
        }

        (&Method::POST, "/update") => {
            let mut conn = pool.get_conn().await.unwrap();

            let byte_stream = hyper::body::to_bytes(req).await?;
            let arch: Architectures = serde_json::from_slice(&byte_stream).unwrap();

            "UPDATE architectures SET arch_id=:arch_id, name=:name, status=:status, version=:version, putch=:putch WHERE arch_id=:arch_id"
                .with(params! {
                    "arch_id" => arch.arch_id,
                    "name" => arch.name,
                    "status" => arch.status,
                    "version" => arch.version,
                    "putch" => arch.putch,
                })
                .ignore(&mut conn)
                .await?;

            drop(conn);
            Ok(response_build("{\"status\":true}"))
        }

        (&Method::GET, "/architectures") => {
            let mut conn = pool.get_conn().await.unwrap();

            let architectures = "SELECT * FROM architectures"
                .with(())
                .map(&mut conn, |(arch_id, name, status, version, putch)| {
                    Architectures::new(
                        arch_id,
                        name,
                        status,
                        version,
                        putch,
                    )},
                ).await?;

            drop(conn);
            Ok(response_build(serde_json::to_string(&architectures)?.as_str()))
        }

        (&Method::GET, "/delete") => {
            let mut conn = pool.get_conn().await.unwrap();

            let params: HashMap<String, String> = req.uri().query().map(|v| {
                url::form_urlencoded::parse(v.as_bytes()).into_owned().collect()
            }).unwrap_or_else(HashMap::new);
            let arch_id = params.get("id");

            "DELETE FROM architectures WHERE arch_id=:arch_id"
                .with(params! { "arch_id" => arch_id, })
                .ignore(&mut conn)
                .await?;

            drop(conn);
            Ok(response_build("{\"status\":true}"))
        }
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

/// Заголовки для работы с CORS
fn response_build(body: &str) -> Response<Body> {

    Response::builder()
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header("Access-Control-Allow-Headers", "api,Keep-Alive,User-Agent,Content-Type")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let opts = Opts::from_url(&*get_database_url()).unwrap();
    let builder = OptsBuilder::from_opts(opts);
    /// Создаю пул подключений с минимальным набором подключений 10, а максимальное количество 15.
    let constraints = PoolConstraints::new(10, 15).unwrap();
    let pool_opts = PoolOpts::default().with_constraints(constraints);
    let pool = Pool::new(builder.pool_opts(pool_opts));

    let addr = SocketAddr::from(([0, 0, 0, 0], 9091));
    let make_svc = make_service_fn(|_| {
        let pool = pool.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let pool = pool.clone();
                handle_request(req, pool)
            }))
        }
    });
    let server = Server::bind(&addr).serve(make_svc);
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
    Ok(())
}