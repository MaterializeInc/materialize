// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use once_cell::sync::Lazy;
use tower_lsp::lsp_types::{
    CompletionItem, CompletionItemKind, CompletionItemLabelDetails, Documentation,
};

/// Contains all the functions
pub static SNIPPETS: Lazy<Vec<CompletionItem>> = Lazy::new(|| {
    vec![
        CompletionItem {
            label: "CREATE SECRET".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Generic function".to_string()),
            }),
            insert_text: Some("CREATE SECRET <SECRET_NAME> AS '<SECRET_VALUE>';".to_string()),
            kind: Some(CompletionItemKind::SNIPPET),
            detail: Some("Code snippet to create a Kafka connection.".to_string()),
            deprecated: Some(false),
            documentation: Some(Documentation::String(
                "https://materialize.com/docs/sql/create-secret/".to_string(),
            )),
            ..Default::default()
        },
        CompletionItem {
            label: "CREATE CONNECTION ... TO POSTGRES".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Generic function".to_string()),
            }),
            insert_text: Some(
                "CREATE CONNECTION <CONNECTION_NAME> TO POSTGRES (
    HOST '<HOST>',
    PORT 5432,
    USER '<USER>',
    PASSWORD SECRET <SECRET_NAME>,
    SSL MODE 'require',
    DATABASE 'postgres'
);"
                .to_string(),
            ),
            kind: Some(CompletionItemKind::SNIPPET),
            detail: Some("Code snippet to create a Postgres connection.".to_string()),
            deprecated: Some(false),
            documentation: Some(Documentation::String(
                "https://materialize.com/docs/sql/create-connection/".to_string(),
            )),
            ..Default::default()
        },
        CompletionItem {
            label: "CREATE CONNECTION ... TO KAFKA".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Generic function".to_string()),
            }),
            insert_text: Some(
                "CREATE CONNECTION <CONNECTION_NAME> TO KAFKA (
BROKER '<BROKER_URL>',
SSL KEY = SECRET <KEY_SECRET_NAME>,
SSL CERTIFICATE = SECRET <SSL_SECRET_NAME>
);"
                .to_string(),
            ),
            kind: Some(CompletionItemKind::SNIPPET),
            detail: Some("Code snippet to create a Kafka connection.".to_string()),
            deprecated: Some(false),
            documentation: Some(Documentation::String(
                "https://materialize.com/docs/sql/create-connection/".to_string(),
            )),
            ..Default::default()
        },
        CompletionItem {
            label: "CREATE SOURCE ... FROM KAFKA".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Generic function".to_string()),
            }),
            insert_text: Some(
                "CREATE SOURCE <SOURCE_NAME>
FROM KAFKA CONNECTION <CONNECTION_NAME> (TOPIC '<TOPIC_NAME>')
FORMAT JSON
WITH (SIZE = '3xsmall');"
                    .to_string(),
            ),
            kind: Some(CompletionItemKind::SNIPPET),
            detail: Some("Code snippet to create a Kafka connection.".to_string()),
            deprecated: Some(false),
            documentation: Some(Documentation::String(
                "https://materialize.com/docs/sql/create-source/kafka/".to_string(),
            )),
            ..Default::default()
        },
        CompletionItem {
            label: "CREATE CLUSTER".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("Create cluster".to_string()),
            }),
            insert_text: Some(
                "CREATE CLUSTER <CLUSTER_NAME> SIZE = '<SIZE>', REPLICATION FACTOR = 2;"
                    .to_string(),
            ),
            kind: Some(CompletionItemKind::SNIPPET),
            detail: Some("Code snippet to create a cluster.".to_string()),
            deprecated: Some(false),
            documentation: Some(Documentation::String(
                "https://materialize.com/docs/sql/create-cluster".to_string(),
            )),
            ..Default::default()
        },
    ]
});
