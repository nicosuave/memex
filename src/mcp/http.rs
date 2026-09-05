use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::never::NeverSessionManager,
};
use tower_http::cors::{AllowHeaders, Any, CorsLayer};

use super::MemexServer;
use crate::{
    config::Paths,
    web_auth::{self, WebAuth},
};

pub struct HttpOptions {
    pub listen: SocketAddr,
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
}

struct Access {
    auth: WebAuth,
    origins: Vec<HeaderValue>,
}

async fn authorize(State(access): State<Arc<Access>>, request: Request, next: Next) -> Response {
    // Check every Origin, including preflight, before CORS can short-circuit.
    // An empty allowlist denies browser origins; clients without Origin still
    // need the bearer token. Cookie and query-string credentials are not used.
    for origin in request.headers().get_all(header::ORIGIN) {
        if !access.origins.contains(origin) {
            return (StatusCode::FORBIDDEN, "origin is not allowed").into_response();
        }
    }
    if request.method() != Method::OPTIONS {
        let mut values = request.headers().get_all(header::AUTHORIZATION).iter();
        let authorized = values
            .next()
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split_once(' '))
            .is_some_and(|(scheme, token)| {
                scheme.eq_ignore_ascii_case("bearer") && access.auth.authorize_bearer(token)
            });
        if !authorized || values.next().is_some() {
            return (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                "bearer token required",
            )
                .into_response();
        }
    }
    next.run(request).await
}

pub(super) async fn run(root: Option<PathBuf>, options: HttpOptions) -> Result<()> {
    let paths = Paths::new(root.clone())?;
    let origins = options
        .allowed_origins
        .iter()
        .map(|origin| {
            ensure!(origin != "*", "allowed origins must be explicit, not '*'");
            origin
                .parse::<HeaderValue>()
                .context("invalid allowed origin")
        })
        .collect::<Result<Vec<_>>>()?;
    let access = Arc::new(Access {
        auth: WebAuth::load_or_create(&paths)?,
        origins: origins.clone(),
    });
    let mut config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_allowed_origins(options.allowed_origins);
    config.allowed_hosts.extend(options.allowed_hosts);
    let cancellation = config.cancellation_token.clone();
    // Every HTTP request gets an SDK handler, but all handlers share the same
    // retrieval semaphore so opening more connections cannot bypass the limit.
    let server = MemexServer::new(root);
    let service = StreamableHttpService::<MemexServer, NeverSessionManager>::new(
        move || Ok(server.clone()),
        Default::default(),
        config,
    );
    let cors = CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([Method::POST])
        // MCP adds protocol headers as it evolves (including Mcp-Param-*).
        // Origins are explicitly allowlisted and credentials remain bearer-only.
        .allow_headers(AllowHeaders::mirror_request())
        .expose_headers(Any);
    let app = Router::new()
        .nest_service("/mcp", service)
        .layer(cors)
        .layer(middleware::from_fn_with_state(access, authorize));
    let listener = tokio::net::TcpListener::bind(options.listen).await?;
    eprintln!("MCP listening on http://{}/mcp", listener.local_addr()?);
    eprintln!(
        "MCP bearer token file: {}",
        web_auth::token_path(&paths).display()
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancellation.cancel();
        })
        .await?;
    Ok(())
}
