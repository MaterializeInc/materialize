from materialize.mzcompose.service import Service


class Persistcli(Service):
    def __init__(
        self,
    ) -> None:
        super().__init__(
            name="persistcli",
            config={
                # TODO: depends!
                "mzbuild": "jobs"
            },
        )
