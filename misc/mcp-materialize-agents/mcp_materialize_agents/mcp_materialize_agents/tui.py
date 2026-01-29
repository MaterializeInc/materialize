# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Container, ScrollableContainer
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Markdown, Static


class DataProductDetailScreen(Screen):
    """A screen to show details of a selected data product."""

    BINDINGS = [
        ("escape", "go_back", "Back"),
    ]

    def __init__(self, product_data: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.product_data = product_data

    def compose(self) -> ComposeResult:
        yield Header()
        yield ScrollableContainer(
            Static(f"[bold cyan]Data Product Details[/bold cyan]", classes="title"),
            Static(""),
            Static(f"[bold]Name:[/bold] {self.product_data['object_name']}"),
            Static(f"[bold]Cluster:[/bold] {self.product_data['cluster']}"),
            Static(f"[bold]Description:[/bold] {self.product_data['description']}"),
            Static(""),
            Static("[bold cyan]Schema:[/bold cyan]"),
            Markdown(self._format_schema()),
            Static(""),
            Static("[dim]Press ESC to go back[/dim]", classes="title"),
            id="details-container",
        )
        yield Footer()

    def _format_schema(self) -> str:
        """Format the schema as markdown."""
        schema = self.product_data.get("schema", {})
        if not schema:
            return "*No schema information available*"

        md_lines = []
        properties = schema.get("properties", {})

        if properties:
            md_lines.append("### Fields\n")
            for field_name, field_info in properties.items():
                field_type = field_info.get("type", "unknown")
                field_desc = field_info.get("description", "")

                md_lines.append(f"**`{field_name}`** ({field_type})")
                if field_desc:
                    md_lines.append(f"  - {field_desc}")
                md_lines.append("")

        return "\n".join(md_lines)

    def action_go_back(self) -> None:
        """Return to the main screen."""
        self.app.pop_screen()


class DataProductsTUI(App):
    """A Textual app for browsing data products."""

    CSS = """
    #list-container {
        padding: 1;
    }

    #details-container {
        padding: 2;
    }

    .title {
        text-align: center;
        margin: 1 0;
    }

    .context {
        text-align: center;
        margin: 0 0 1 0;
    }

    DataTable {
        height: 100%;
        margin: 1 0;
    }

    Button {
        margin: 1 0;
    }
    """

    BINDINGS = [
        ("escape", "quit", "Quit"),
    ]

    def __init__(self, data_products: list, env_info: dict = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_products = data_products
        self.env_info = env_info or {}
        self.selected_row = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static(
                "[bold cyan]Materialize AI Data Products Explorer[/bold cyan]",
                classes="title",
            ),
            Static(
                f"[dim]Environment: {self.env_info.get('env', 'Unknown')} | User: {self.env_info.get('role', 'Unknown')}[/dim]",
                classes="context",
            )
            if self.env_info
            else Static(""),
            Static("Use ↑↓ to navigate, Enter to view details, ESC to quit"),
            DataTable(cursor_type="row"),
            id="list-container",
        )
        yield Footer()

    def on_mount(self) -> None:
        """Set up the data table when the app starts."""
        table = self.query_one(DataTable)
        table.add_columns("Name", "Cluster", "Description")

        for idx, product in enumerate(self.data_products):
            table.add_row(
                product["object_name"],
                product["cluster"],
                product["description"][:80] + "..."
                if len(product["description"]) > 80
                else product["description"],
                key=str(idx),
            )

    @on(DataTable.RowSelected)
    def on_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle row selection."""
        if event.row_key:
            idx = int(event.row_key.value)
            selected_product = self.data_products[idx]
            self.push_screen(DataProductDetailScreen(selected_product))
