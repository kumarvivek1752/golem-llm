use std::{str::FromStr, sync::Arc};

use aws_sdk_bedrockruntime::{config, error::ConnectorError};
use aws_smithy_runtime_api::{
    client::http::{
        HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    },
    http::{Headers, Response, StatusCode},
};
use aws_smithy_types::body::SdkBody;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Method,
};

use crate::async_utils::UnsafeFuture;

#[derive(Debug)]
pub struct WasiClient {
    reactor: wasi_async_runtime::Reactor,
}

impl WasiClient {
    pub fn new(reactor: wasi_async_runtime::Reactor) -> Self {
        Self { reactor }
    }
}

impl config::HttpClient for WasiClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _components: &config::RuntimeComponents,
    ) -> SharedHttpConnector {
        let client = reqwest::Client::builder(self.reactor.clone())
            .connect_timeout(settings.connect_timeout())
            .timeout(settings.read_timeout())
            .build()
            .expect("Valid http client configuration");
        let connector = SharedWasiConnector::new(client);
        SharedHttpConnector::new(connector)
    }
}

unsafe impl Send for WasiClient {}
unsafe impl Sync for WasiClient {}

#[derive(Debug)]
struct SharedWasiConnector {
    inner: Arc<WasiConnector>,
}

impl SharedWasiConnector {
    fn new(client: reqwest::Client) -> Self {
        Self {
            inner: Arc::new(WasiConnector(client)),
        }
    }
}

#[derive(Debug)]
struct WasiConnector(reqwest::Client);

unsafe impl Send for WasiConnector {}
unsafe impl Sync for WasiConnector {}

impl WasiConnector {
    async fn handle(
        &self,
        request: config::http::HttpRequest,
    ) -> Result<reqwest::Response, ConnectorError> {
        let method = Method::from_bytes(request.method().as_bytes()).expect("Valid http method");
        let url = request.uri().to_owned();
        let parts = request.into_parts();

        let mut header_map = HeaderMap::new();

        for header in parts.headers.iter() {
            header_map.append(
                HeaderName::from_str(header.0).expect("Valid http header name"),
                HeaderValue::from_str(header.1).expect("Valid http header value"),
            );
        }

        let mut request = self.0.request(method, url).headers(header_map);

        if let Some(bytes) = parts.body.bytes() {
            request = request.body(bytes.to_owned());
        }

        request
            .send()
            .await
            .map_err(|e| ConnectorError::other(e.into(), None))
    }
}

impl HttpConnector for SharedWasiConnector {
    fn call(&self, request: config::http::HttpRequest) -> HttpConnectorFuture {
        let inner_clone = Arc::clone(&self.inner);

        let future = async move {
            let response = inner_clone.handle(request).await?;
            log::trace!("WasiConnector: response received {response:?}");

            let status_code: StatusCode = response.status().into();
            let headers_map = response.headers().clone();
            let extensions = response.extensions().clone();

            let body = response
                .bytes()
                .await
                .map(|body| {
                    if body.is_empty() {
                        SdkBody::empty()
                    } else {
                        SdkBody::from(body)
                    }
                })
                .map_err(|e| ConnectorError::other(e.into(), None))?;

            let mut headers = Headers::new();
            for header in headers_map {
                if let Some(key) = header.0 {
                    if let Some(value) = header.1.to_str().ok() {
                        headers.insert(key.to_string(), value.to_string());
                    }
                }
            }

            let mut sdk_response = Response::new(status_code, body);
            *sdk_response.headers_mut() = headers;
            sdk_response.add_extension(extensions);

            Ok(sdk_response)
        };

        HttpConnectorFuture::new(UnsafeFuture::new(future))
    }
}
