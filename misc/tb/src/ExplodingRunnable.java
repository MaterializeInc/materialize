// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

package io.materialize.tb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a Runnable and exits the process if the wrapped runnable throws
 * an exception.
 */
public class ExplodingRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TB.class);

    private final Runnable task;

    public ExplodingRunnable(Runnable task) {
        this.task = task;
    }

    /**
    * Runs the wrapped Runnable, exiting the process if the wrapped runnable
    * throws an exception.
    */
    public void run() {
        try {
            task.run();
        } catch (Throwable t) {
            logger.error("task {} exception", task, t);
            System.exit(1);
        }
    }
}
