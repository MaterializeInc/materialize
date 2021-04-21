// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Insert random data into MySQL, assuming the Debezium demo database

use anyhow::Error;
use log::{error, info};
use mysql_async::prelude::Queryable;
use mysql_async::{Pool, TxOpts};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Error> {
    ore::panic::set_abort_on_panic();
    ore::test::init_logging();

    info!("startup!");

    let mysql_url = "mysql://mysqluser:mysqlpw@mysql:3306/inventory";
    let pool = Pool::new(mysql_url);

    alter_constraints(&pool).await?;
    issue_random_queries(&pool).await?;
    pool.disconnect().await?;
    Ok(())
}

// Modify foreign key relationships to enable cascade deletion
async fn alter_constraints(pool: &Pool) -> Result<(), Error> {
    let alter_key_queries = vec![
        "ALTER TABLE addresses DROP FOREIGN KEY addresses_ibfk_1",
        "ALTER TABLE addresses ADD CONSTRAINT addresses_ibfk_1 FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE ON UPDATE NO ACTION",
        "ALTER TABLE products_on_hand DROP FOREIGN KEY products_on_hand_ibfk_1",
        "ALTER TABLE products_on_hand ADD CONSTRAINT products_on_hand_ibfk_1 FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE ON UPDATE NO ACTION",
        "ALTER TABLE orders DROP FOREIGN KEY orders_ibfk_1",
        "ALTER TABLE orders ADD CONSTRAINT orders_ibfk_1 FOREIGN KEY (purchaser) REFERENCES customers (id) ON DELETE CASCADE ON UPDATE NO ACTION",
        "ALTER TABLE orders DROP FOREIGN KEY orders_ibfk_2",
        "ALTER TABLE orders ADD CONSTRAINT orders_ibfk_2 FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE ON UPDATE NO ACTION",
    ];

    let mut conn = pool.get_conn().await?;
    let mut tx = conn.start_transaction(TxOpts::default()).await?;

    for query in alter_key_queries.into_iter() {
        tx.query_drop(query).await?;
    }

    Ok(())
}

async fn issue_random_queries(pool: &Pool) -> Result<(), Error> {
    let random_transactions = vec![
        vec![r#"INSERT INTO orders (order_date, purchaser, quantity, product_id)
                SELECT date(now()), customers.id, 1, products_on_hand.product_id
                FROM customers, products_on_hand
                WHERE products_on_hand.quantity > 10
                ORDER BY rand() LIMIT 1"#,],
        vec![r#"INSERT INTO orders (order_date, purchaser, quantity, product_id)
                SELECT date(now()), customers.id, 2, products_on_hand.product_id
                FROM customers, products_on_hand
                WHERE products_on_hand.quantity > 20
                ORDER BY rand() LIMIT 1"#,],
        vec![r#"INSERT INTO orders (order_date, purchaser, quantity, product_id)
                SELECT date(now()), customers.id, 3, products_on_hand.product_id
                FROM customers, products_on_hand
                WHERE products_on_hand.quantity > 30
                ORDER BY rand() LIMIT 1"#,],
        vec![r#"INSERT INTO orders (order_date, purchaser, quantity, product_id)
                SELECT date(now()), customers.id, 4, products_on_hand.product_id
                FROM customers, products_on_hand
                WHERE products_on_hand.quantity > 40
                ORDER BY rand() LIMIT 1"#,],
        vec![r#"INSERT INTO orders (order_date, purchaser, quantity, product_id)
                SELECT date(now()), customers.id, 5, products_on_hand.product_id
                FROM customers, products_on_hand
                WHERE products_on_hand.quantity > 50
                ORDER BY rand() LIMIT 1"#,],
        vec![r#"INSERT INTO addresses (customer_id, street, city, state, zip, type)
                VALUES (
                   (SELECT id FROM customers ORDER BY rand() LIMIT 1),
                   '1 Infinite View',
                   'New York',
                   'New York',
                   '10001',
                   'BILLING'
                  )"#,],
        vec![r#"INSERT INTO addresses (customer_id, street, city, state, zip, type)
                VALUES (
                   (SELECT id FROM customers ORDER BY rand() LIMIT 1),
                   '1 Fantastic Sink',
                   'New York',
                   'New York',
                   '10001',
                   'SHIPPING'
                  )"#,],
        vec!["INSERT IGNORE INTO customers (first_name, last_name, email) VALUES ('alex', 'materialize', 'am@materialize.com')",],
        vec!["INSERT IGNORE INTO customers (first_name, last_name, email) VALUES ('cameron', 'materialize', 'cm@materialize.com')",],
        vec!["INSERT IGNORE INTO customers (first_name, last_name, email) VALUES ('frankie', 'materialize', 'fm@materialize.com')",],
        vec!["INSERT IGNORE INTO customers (first_name, last_name, email) VALUES ('jaime', 'materialize', 'jm@materialize.com')",],
        vec!["INSERT IGNORE INTO customers (first_name, last_name, email) VALUES ('kyle', 'materialize', 'km@materialize.com')",],
        vec!["INSERT IGNORE INTO customers (first_name, last_name, email) VALUES ('mattie', 'materialize', 'mm@materialize.com')",],
        vec!["INSERT IGNORE INTO customers (first_name, last_name, email) VALUES ('nicky', 'materialize', 'nm@materialize.com')",],
        vec!["INSERT IGNORE INTO customers (first_name, last_name, email) VALUES ('pat', 'materialize', 'pm@materialize.com')",],
        vec!["INSERT IGNORE INTO customers (first_name, last_name, email) VALUES ('quinn', 'materialize', 'qm@materialize.com')",],
        vec!["INSERT IGNORE INTO customers (first_name, last_name, email) VALUES ('riley', 'materialize', 'rm@materialize.com')",],
        vec!["INSERT IGNORE INTO customers (first_name, last_name, email) VALUES ('taylor', 'materialize', 'tm@materialize.com')",],
        vec![r#"INSERT INTO products (name, description, weight)
                VALUES ('kitchen sink', 'something with everything', '2000.0')"#,
         "INSERT INTO products_on_hand (product_id, quantity) VALUES (LAST_INSERT_ID(), rand()*25 + rand()*25)"],
        vec![r#"INSERT INTO products (name, description, weight)
                VALUES ('socket wrench', '12mm ratcheting', '.25')"#,
         "INSERT INTO products_on_hand (product_id, quantity) VALUES (LAST_INSERT_ID(), rand()*50 + rand()*50)"],
        vec![r#"INSERT INTO products (name, description, weight)
                VALUES ('socket wrench', '13mm ratcheting', '.25')"#,
         "INSERT INTO products_on_hand (product_id, quantity) VALUES (LAST_INSERT_ID(), rand()*50 + rand()*50)"],
        vec![r#"INSERT INTO products (name, description, weight)
                VALUES ('socket wrench', '14mm ratcheting', '.25')"#,
         "INSERT INTO products_on_hand (product_id, quantity) VALUES (LAST_INSERT_ID(), rand()*50 + rand()*50)"],
        vec!["DELETE FROM addresses ORDER BY rand() LIMIT 1",],
        vec!["DELETE FROM customers ORDER BY rand() LIMIT 1",],
        vec!["DELETE FROM orders ORDER BY rand() LIMIT 1",],
        vec!["DELETE FROM products ORDER BY rand() LIMIT 1",],
        vec!["DELETE FROM products_on_hand ORDER BY rand() LIMIT 1",],
    ];

    let tasks = (0..8)
        .map(|i| {
            let pool = pool.clone();
            let queries = random_transactions.clone();

            tokio::spawn(async move {
                let mut conn = pool.get_conn().await?;

                let mut successes = 0;
                let mut failures = 0;

                for _ in 0..10 {
                    // Grab random set of queries and execute them. Ignore errors
                    let mut tx = conn.start_transaction(TxOpts::default()).await?;

                    let mut commit = true;

                    let num_statements = thread_rng().gen_range(4..=8);
                    // Randomly join multiple statements into the same transaction
                    'transaction: for _ in 0..num_statements {
                        let tx_queries = queries
                            .choose(&mut thread_rng())
                            .expect("can select random query");

                        for query in tx_queries {
                            // info!("[{}]: {}", i, query);
                            match tx.query_drop(query).await {
                                Ok(_) => (),
                                Err(mysql_async::Error::Server(mysql_async::ServerError {
                                    code: 1213,
                                    ..
                                })) => {
                                    commit = false;
                                    break 'transaction;
                                }
                                Err(e) => {
                                    error!("Error! {:#?}", e);
                                    commit = false;
                                    break 'transaction;
                                }
                            }

                            // Random jigger to encourage transaction conflicts
                            let sleep_duration =
                                Duration::from_millis(thread_rng().gen_range(4..=7));
                            tokio::time::sleep(sleep_duration).await;
                        }
                    }

                    if commit {
                        successes += 1;
                        tx.commit().await?;
                    } else {
                        failures += 1;
                        tx.rollback().await?;
                    }
                }

                Ok::<(i32, i32, i32), Error>((i, successes, failures))
            })
        })
        .collect::<Vec<_>>();

    for task in tasks {
        let (thread, successes, failures) = task.await??;
        info!(
            "tid: {}, successes: {}, failures: {}",
            thread, successes, failures
        );
    }

    Ok(())
}
