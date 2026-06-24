---
title: "Live data products"
description: "Materialize is for building the live data products of your business — the typed, queryable ontology of your business, kept up to date in SQL."
menu:
  main:
    parent: concepts
    weight: 2
    identifier: 'concepts-live-data-products'
---

## Materialize is for building the live data products of your business

A live data product is something real in your business: a customer, an order, an account, a store, a courier. You define it once, in SQL. Materialize keeps it up to date as the underlying systems change, fresh within about a second. Your applications, services, ML features, dashboards, and AI agents read the same live result.

The live data products you build, together with the relationships between them, form your business's ontology: the typed, queryable model of the business.

## Your business is a set of nouns scattered across silos

The objects you reason about (Customer, Order, Subscription, Store, Courier) are the nouns of your business. Each has a meaning, fields, identity, and relationships to other nouns. Almost none of them live in a single system.

The Customer noun isn't in one place. Identity lives in the CRM, orders in the OMS, tickets in the support tool, payments in the billing system, fulfillment in the warehouse and dispatch systems. A consumer that wants the full Customer has to stitch the silos together itself, on every read.

In Materialize, a live data product does the stitching once, in SQL, and keeps the result current:

```mzsql
CREATE VIEW customers AS
  WITH order_summary AS (
    SELECT account_id, count(*) AS lifetime_orders
    FROM   erp.orders
    GROUP BY account_id
  ),
  ticket_summary AS (
    SELECT account_id, max(opened_at) AS last_ticket_at
    FROM   zendesk.tickets
    GROUP BY account_id
  )
  SELECT a.account_id,
         a.name,
         a.plan_tier,
         coalesce(o.lifetime_orders, 0) AS lifetime_orders,
         t.last_ticket_at
  FROM        crm.accounts   a
  LEFT JOIN   order_summary  o USING (account_id)
  LEFT JOIN   ticket_summary t USING (account_id);
```

The view is your business's authoritative statement of what a customer is. Each row is the live representation of one customer, joined across whatever silos own that state. Open a ticket, place an order, change a plan, and the row reflects it within about a second. Materialize doesn't add a batch window on top of your source systems; whatever those systems publish, the row reflects within an incremental maintenance step. You don't write incremental update logic, schedule batch refreshes, or reconcile staleness windows.

## The ontology compounds

Each live data product you define joins a few silos and yields one noun. As the ontology grows, you don't just gain a noun; you gain every combination of that noun with the ones already there. With ten silo-spanning nouns, you can express hundreds of derived views in plain SQL.

Define Orders the same way you defined Customer, joining ERP orders with billing payments and warehouse fulfillment. Define Stores by joining the locations registry with inventory and active shifts:

```mzsql
CREATE VIEW stores AS
  WITH inventory_summary AS (
    SELECT store_id, sum(on_hand) AS units_on_hand
    FROM   wms.inventory
    GROUP BY store_id
  ),
  staffing_summary AS (
    SELECT store_id, count(*) AS staff_on_shift
    FROM   workforce.active_shifts
    WHERE  shift_end > now()
    GROUP BY store_id
  )
  SELECT s.store_id,
         s.name,
         s.geo,
         coalesce(i.units_on_hand, 0)   AS units_on_hand,
         coalesce(st.staff_on_shift, 0) AS staff_on_shift
  FROM        locations.stores  s
  LEFT JOIN   inventory_summary i  USING (store_id)
  LEFT JOIN   staffing_summary  st USING (store_id);
```

Now the operational question, which orders need intervention right now, is one view over two existing nouns:

```mzsql
CREATE VIEW at_risk_orders AS
  SELECT o.order_id,
         o.account_id,
         o.placed_at,
         o.current_status,
         s.store_id,
         s.name               AS store_name,
         s.units_on_hand      AS store_units_on_hand,
         s.staff_on_shift     AS store_staff_on_shift
  FROM   orders o
  JOIN   stores s ON s.store_id = o.fulfillment_store_id
  WHERE  o.current_status NOT IN ('delivered', 'cancelled')
    AND  (mz_now() - o.placed_at > interval '30 minutes'
          OR s.units_on_hand  = 0
          OR s.staff_on_shift = 0);
```

`at_risk_orders` doesn't exist in any operational system. It's a new noun defined over two existing nouns, built with no new silo integration: pure SQL over what's already in the ontology. An agent asking "what should I escalate right now?" reads this view directly. The same applies to any combination: Customer × Order × Store, Store × Courier × Inventory, Courier × Order × Customer.
Materialize's ontology models the nouns. The verbs, actions that change state, happen in your systems of action: order placement, account updates, ticket resolution. You take action in those systems; you observe the effects through the live ontology.

## Always live, always correct

Once you've defined a noun, Materialize keeps it current. Changes flow in; every dependent noun updates incrementally, fresh within about a second, not within a batch window.

Any consumer (an application, a service, a dashboard, an AI agent) reads the ontology with the strict serializability of a SQL query: every read sees a consistent snapshot that respects real-time write ordering. A change propagates through your entire business within about a second.

This is what makes the ontology trustworthy. Other systems can model your business; Materialize keeps the model true to it in the moment. See *Reaction time, freshness, and query latency* for the bound.

## Fast feedback loops

Observe → act → observe the consequences.

Because the ontology updates live, any consumer can take an action in its own system and watch the effect propagate through dependent nouns:

```
operational silos                          live data products              consumer

  erp.orders       ──┐
  billing.payments ──┤  CDC  ──►  orders  ──┐
  wms.fulfillment  ──┘                      │
                                            ├──►  at_risk_orders  ──► reads now
  locations.stores ──┐                      │
  wms.inventory    ──┤  CDC  ──►  stores  ──┘
  workforce.shifts ──┘
```

An AI agent calls a tool and verifies the change reached the customer record; a service makes a transactional decision and reads the downstream signal on the next request; a UI reacts to a user action without polling; a pipeline alerts the moment a condition flips. All close the loop against the same ontology.

The interval between a real-world event and the moment it becomes trusted context is *time to trusted action*. When it drops to seconds, the experiences you can build change fundamentally.

Agents need to observe the consequences of their actions. That's what unlocks the agentic feedback loop: an agent observes the state of the world through the ontology, thinks using a large language model, acts, and then takes a follow-on action based on the consequences. Without observing the consequence, there is no next step. A warehouse hours behind cannot close that loop; a live ontology can.

For agent builders, Materialize provides two primitives:

- **Read:** typed queries over live rows in the ontology.
- **Compose:** SQL functions and views that shape rows for an agent's task.

The MCP integration exposes both primitives to agents through tool definitions over the SQL surface.

Write-back happens through your existing systems. Materialize observes the changes from those systems and updates the ontology within about a second. The closed loop is only as fast as the source systems publish changes.

## The right primitive for many use cases

Once you've modeled your business as an ontology of live data products, the same ontology serves every application, service, dashboard, ML feature, alert, and agent you build on top of it. You stop building bespoke pipelines per consumer; you build the ontology once, and every downstream system reads the same truth.

You don't write a pipeline per consumer, a cache per query, or a refresh job per dashboard. You define the ontology once, in SQL, and Materialize keeps it true. The live data product is the unit of work.

## Learn more

- [Quickstart](/get-started/quickstart/) — build your first live data product.
- [Reaction time, freshness, and query latency](/concepts/reaction-time/) — the freshness contract.
- [Serve results](/serve-results/) — read the ontology from your applications and services.
- [MCP integration](/integrations/mcp-server/) — expose the ontology to AI agents.
