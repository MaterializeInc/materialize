// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/* global instantsearch algoliasearch */

const ENV = document.head.querySelector("[name=environment][content]").content,
search = instantsearch({
  indexName: ENV + '_materialize',
  searchClient: algoliasearch('RD8N8OKT1S', '3964a300b5b3516d542717f6cf704bd4'),
  //Prevent empty search on load
  searchFunction: function(helper) {
    if (helper.state.query === '') {
      document.getElementById('search-hits').innerHTML = '';
      return;
    }
    helper.search();
  },
});

search.addWidgets([
  instantsearch.widgets.searchBox({
    container: '#search-input',
    placeholder: 'Search the docs'
  }),
  instantsearch.widgets.hits({
    container: '#search-hits',
    templates: {
      item: `
        <a href="{{url}}">
          <div class="hit-title">
            <span class="parentTitle">{{parentTitle}}</span>{{#helpers.highlight}}{ "attribute": "title" }{{/helpers.highlight}}
          </div>
          <div class="hit-description">
            {{#helpers.snippet}}{ "attribute": "description" }{{/helpers.snippet}}
          </div>
          <span class="eyebrow">{{breadcrumbs}}</span>
        </a>
      `,
    },
  }),
]);

search.start();
