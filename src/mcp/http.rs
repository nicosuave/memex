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
use tokio::sync::Semaphore;
use tower_http::cors::{AllowHeaders, Any, CorsLayer};

use super::{MemexServer, oauth::OAuthServer};
use crate::{
    config::Paths,
    web_auth::{self, WebAuth},
};

pub struct HttpOptions {
    pub listen: SocketAddr,
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub public_url: Option<String>,
}

struct Access {
    auth: Arc<WebAuth>,
    oauth: Option<Arc<OAuthServer>>,
    oauth_checks: Arc<Semaphore>,
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
        let token = values
            .next()
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
            .map(|(_, token)| token.to_owned());
        let authorized = if values.next().is_some() {
            false
        } else if let Some(token) = token {
            if access.auth.authorize_bearer(&token) {
                true
            } else if let Some(oauth) = &access.oauth {
                let Ok(permit) = access.oauth_checks.clone().try_acquire_owned() else {
                    return (StatusCode::TOO_MANY_REQUESTS, "authentication is busy")
                        .into_response();
                };
                let oauth = oauth.clone();
                // SQLite work cannot block transport I/O or fill an unbounded
                // queue when unauthenticated requests arrive concurrently.
                match tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    oauth.authorize_access(&token)
                })
                .await
                {
                    Ok(Ok(valid)) => valid,
                    _ => {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            "authentication unavailable",
                        )
                            .into_response();
                    }
                }
            } else {
                false
            }
        } else {
            false
        };
        if !authorized {
            let challenge = access
                .oauth
                .as_ref()
                .map_or_else(|| "Bearer".to_owned(), |oauth| oauth.challenge());
            return (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, challenge)],
                "bearer token required",
            )
                .into_response();
        }
    }
    next.run(request).await
}

pub(super) async fn run(root: Option<PathBuf>, options: HttpOptions) -> Result<()> {
    let paths = Paths::new(root.clone())?;
    let auth = Arc::new(WebAuth::load_or_create(&paths)?);
    let oauth = options
        .public_url
        .as_deref()
        .map(|url| OAuthServer::new(&paths, url, auth.clone()))
        .transpose()?
        .map(Arc::new);
    let mut allowed_origins = options.allowed_origins;
    if let Some(oauth) = &oauth {
        allowed_origins.push(oauth.origin().to_owned());
    }
    let origins = allowed_origins
        .iter()
        .map(|origin| {
            ensure!(origin != "*", "allowed origins must be explicit, not '*'");
            origin
                .parse::<HeaderValue>()
                .context("invalid allowed origin")
        })
        .collect::<Result<Vec<_>>>()?;
    let access = Arc::new(Access {
        auth,
        oauth: oauth.clone(),
        oauth_checks: Arc::new(Semaphore::new(16)),
        origins: origins.clone(),
    });
    let mut config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_allowed_origins(allowed_origins);
    config.allowed_hosts.extend(options.allowed_hosts);
    if let Some(oauth) = &oauth {
        let public_url = url::Url::parse(oauth.origin())?;
        config
            .allowed_hosts
            .push(public_url[url::Position::BeforeHost..url::Position::AfterPort].to_owned());
    }
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
    let mut app = Router::new()
        .nest_service("/mcp", service)
        .layer(cors)
        .layer(middleware::from_fn_with_state(access, authorize));
    // OAuth discovery and consent must remain reachable before authorization;
    // the MCP resource middleware above still protects every tool request.
    if let Some(oauth) = &oauth {
        app = app.merge(oauth.clone().router());
    }
    let listener = tokio::net::TcpListener::bind(options.listen).await?;
    eprintln!("MCP listening on http://{}/mcp", listener.local_addr()?);
    if let Some(oauth) = &oauth {
        eprintln!("MCP OAuth endpoint: {}", oauth.resource());
    }
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
