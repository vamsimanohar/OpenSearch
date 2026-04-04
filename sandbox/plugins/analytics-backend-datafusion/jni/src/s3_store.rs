// SPDX-License-Identifier: Apache-2.0
//
// The OpenSearch Contributors require contributions made to
// this file be licensed under the Apache-2.0 license or a
// compatible open source license.

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::runtime_env::RuntimeEnv;
use object_store::aws::AmazonS3Builder;
use url::Url;

pub struct S3Config {
    pub region: String,
    pub bucket: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub endpoint: Option<String>,
}

/// Registers an S3-backed object store with the DataFusion RuntimeEnv.
pub fn register_s3_store(runtime_env: &RuntimeEnv, config: &S3Config) -> Result<()> {
    let mut builder = AmazonS3Builder::new()
        .with_region(&config.region)
        .with_bucket_name(&config.bucket);

    if let (Some(key), Some(secret)) = (&config.access_key_id, &config.secret_access_key) {
        builder = builder.with_access_key_id(key).with_secret_access_key(secret);
        if let Some(token) = &config.session_token {
            builder = builder.with_token(token);
        }
    }

    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint).with_virtual_hosted_style_request(false);
    }

    let store = Arc::new(builder.build().map_err(|e| {
        datafusion::error::DataFusionError::External(Box::new(e))
    })?);
    let url = Url::parse(&format!("s3://{}", config.bucket)).map_err(|e| {
        datafusion::error::DataFusionError::External(Box::new(e))
    })?;
    runtime_env.register_object_store(&url, store);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[test]
    fn test_register_s3_store_with_explicit_credentials() {
        let ctx = SessionContext::new();
        let config = S3Config {
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            session_token: None,
            endpoint: None,
        };
        let result = register_s3_store(ctx.runtime_env().as_ref(), &config);
        assert!(result.is_ok());
    }
}
