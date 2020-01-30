# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

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
