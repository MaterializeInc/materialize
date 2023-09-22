const fs = require('fs');
const YAML = require('yaml');

function generateRustCode(functionsContent) {
    let rustItems = functionsContent.map(({ type, functions }) => {
        return functions.map(({ signature, description }) => `    CompletionItem {
            label: "${signature}".to_string(),
            label_details: Some(CompletionItemLabelDetails {
                detail: None,
                description: Some("${type} function".to_string()),
            }),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("${description.replace(/"/g, '\\"')}".to_string()),
            documentation: Some(DOCS_LINK.clone()),
            deprecated: Some(false),
            ..Default::default()
        }
        `);
    }).join(",\n");

    return rustItems;
}

const file = fs.readFileSync('./functions.yml', 'utf8')
const functionsContent = YAML.parse(file);
const items = generateRustCode(functionsContent);
const rustCode = `
use once_cell::sync::Lazy;
use tower_lsp::lsp_types::{
    CompletionItem, CompletionItemKind, CompletionItemLabelDetails, Documentation,
};

// TODO: Turn into Markdown content.
pub static DOCS_LINK: Lazy<Documentation> = Lazy::new(|| {
    Documentation::String("https://materialize.com/docs/sql/functions/".to_string())
});

pub static FUNCTIONS: Lazy<Vec<CompletionItem>> = Lazy::new(|| vec![
    ${items}
]);
`
fs.writeFileSync('../src/functions.rs', rustCode);