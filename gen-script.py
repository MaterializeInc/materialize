import subprocess


def main():
    for i in range(100, 1000):
        subprocess.run(
            ["psql", "-q", "-U", "materialize", "-h", "localhost", "-p", "6875", "materialize",
             "-c", f"BEGIN; CREATE VIEW v{i} AS SELECT 1; COMMIT; BEGIN; CREATE VIEW vv{i} AS SELECT 1; COMMIT; BEGIN; CREATE VIEW vvv{i} AS SELECT 3; COMMIT;"])


if __name__ == "__main__":
    main()
