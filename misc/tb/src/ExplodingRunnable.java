// Copyright Materialize, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
