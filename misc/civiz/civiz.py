# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sqlalchemy
from collections import namedtuple
from flask import Flask, render_template

app = Flask(__name__)
app.config["APPLICATION_ROOT"] = "/civiz/"

db = sqlalchemy.create_engine("postgres:///buildkite")

@app.route("/")
def slt():
    rows = db.execute("SELECT * FROM slt")
    cols = rows.keys()
    Row = namedtuple("Row", cols)
    results = [Row(*row) for row in rows]
    return render_template("slt.html", results=results)
