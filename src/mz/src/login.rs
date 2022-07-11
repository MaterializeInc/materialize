use std::{io::{Write}, collections::HashMap};

use open;
use actix_web::{get, App, HttpServer, Responder, HttpRequest, web };
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT, AUTHORIZATION};
use reqwest::{Client};
use crate::{API_TOKEN_AUTH_URL, BrowserAPIToken, FronteggAPIToken, FronteggAuthUser, Profile, USER_AUTH_URL};
use crate::profiles::{save_profile};
use crate::utils:: {trim_newline};

/// ----------------------------
///  Login code using browser
/// ----------------------------

#[get("/")]
async fn request(req: HttpRequest) -> impl Responder {
    println!("Getting token.");
    println!("Query string: {:?}", req.query_string());
    println!("Headers: {:?}", req.headers());

    let api_token = web::Query::<BrowserAPIToken>::from_query(req.query_string()).unwrap();

    let profile = Profile {
        secret: api_token.secret.to_string(),
        client_id: api_token.client_id.to_string(),
        default_region: None
    };
    save_profile(profile).unwrap();

    "You can now close the tab."
}

pub(crate) async fn login_with_browser() -> Result<(), std::io::Error> {
    /*
     * Open the browser to login user
     */
    let path = "http://localhost:8000/account/login?redirectUrl=/access/cli";
    match open::that(path) {
        Err(err) => panic!("An error occurred when opening '{}': {}", path, err),
        _ => {
            println!("Browser open.");
        }
    }

    /*
     * Start the server to handle the request response
     */
    HttpServer::new(|| {
        App::new()
            // .app_data(app_data)
            .service(request)
    })
        .bind(("127.0.0.1", 8808))?
        .run()
        .await.unwrap();

    Ok(())
}

/// ----------------------------
///  Login code using console
/// ----------------------------

async fn generate_api_token(client: &Client, access_token_response: FronteggAuthUser) -> Result<FronteggAPIToken, reqwest::Error> {
    let authorization: String = format!("Bearer {}", access_token_response.access_token);

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(AUTHORIZATION, HeaderValue::from_str(authorization.as_str()).unwrap());
    let mut body = HashMap::new();
    body.insert("description", &"Token for the CLI");

    client.post(API_TOKEN_AUTH_URL)
        .headers(headers)
        .json(&body)
        .send()
        .await?
        .json::<FronteggAPIToken>()
        .await
}

async fn authenticate_user(client: &Client, email: String, password: String) -> Result<FronteggAuthUser, reqwest::Error> {
    let mut access_token_request_body = HashMap::new();
    access_token_request_body.insert("email", email);
    access_token_request_body.insert("password", password);

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    client.post(USER_AUTH_URL)
        .headers(headers)
        .json(&access_token_request_body)
        .send()
        .await?
        .json::<FronteggAuthUser>()
        .await
}

pub(crate) async fn login_with_console() -> Result<(), reqwest::Error> {
    // Handle user input
    let mut email = String::new();
    print!("Email: ");
    let _ = std::io::stdout().flush();
    std::io::stdin().read_line(&mut email).unwrap();
    trim_newline(&mut email);

    print!("Password: ");
    let _ = std::io::stdout().flush();
    let password = rpassword::read_password().unwrap();

    println!("Email: {:?} - Password: {:?} ", email, password);
    let client = Client::new();

    // Check if there is a secret somewhere.
    // If there is none save the api token someone on the root folder.
    let auth_user = authenticate_user(&client, email, password).await?;
    let api_token = generate_api_token(&client, auth_user).await?;
    println!("ID: {:?} - Secret: {:?}", api_token.client_id, api_token.secret);
    let profile = Profile {
        secret: api_token.secret,
        client_id: api_token.client_id,
        default_region: None
    };
    save_profile(profile).unwrap();
    Ok(())
}
