### Symbolizing profiles

The dumped profiles contain raw addresses rather than function names. The `profiles/` directory includes a `symbolize.sh` helper that fetches the matching debug information and symbolizes every profile in one step. It requires only Docker and network access:

```shell
cd <dump-directory>/profiles
bash ./symbolize.sh
```

The resulting `{service}.*.symbolized.pprof.gz` files can be shared and viewed directly, for example by uploading them to [pprof.me](https://pprof.me). Run `bash ./symbolize.sh --help` for more options, including an interactive web UI.
