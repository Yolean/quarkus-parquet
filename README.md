# quarkus-parquet

Bring Apache Parquet core features to Quarkus native images with a dedicated extension.

## Development

- `mvn -pl deployment -am verify` to iterate.
- `mvn verify -Dnative` to run integration tests.
- `mvn -pl deployment -am clean deploy` to produce jars to `./shapshots`.
