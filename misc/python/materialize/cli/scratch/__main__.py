import argparse


def main() -> None:
    from materialize.cli.scratch import create, mine
    modules = [create, mine]

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True, dest='subcommand')
    for m in modules:
        s = subparsers.add_parser(m.__name__.split('.')[-1])
        m.configure_parser(s)
        s.set_defaults(mod=m)

    args = parser.parse_args()
    args.mod.run(args)
        

if __name__ == "__main__":
    import sys, pprint
    pprint.pprint(sys.argv)
    main()
