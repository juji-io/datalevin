# Datalevin Database Upgrade

Before introducing steps to upgrade Datalevin databases, let us discuss the
versioning of Datalevin, so we have the right expectations as to when a database
upgrade is needed.

## Versioning

Datalevin version numbers roughly follow this numbering schema:
`major.minor.non-breaking`.

`major` version bumps means Datalevin having reached a major milestone in
functionality. For example, we will bump the version to 1.0.0 when we have
rewritten the query engine and reached feature parity (minus temporal features)
with Datomic in term of Datalog processing. Such version changes may or may not
requires data migration, as these may have little to do with concrete data
encoding and storage changes.

`minor`version number change indicate big code changes that ship new features. These
often involve breaking changes that requires data migration, even when the
impact of the changes on data encoding is not obvious. For example, the version
number goes from 0.4.x to 0.5.x when we introduced client/server mode, and some
databases needed migration when this happened.

`non-breaking` version number indicates small code changes that do not break
existing API or affect existing databases. These are bug fixes or minor feature
introductions. No data migration should be expected for such version bumps.

In summary, we should expect that minor version number changes require migrating
existing databases when upgrading. Major version bumps may not require migration
 if you are diligent in following the minor version upgrades, but if you are
 not, data migration is needed. No-breaking version bumps do not require data
 migration.

## Database Upgrade

Now we know when a database upgrade is needed, here is how to do it.

Upgrading a database from an old version to a new version requires the use of
native command line tool, `dtlv`. In fact, both the old and the new version of
the `dtlv` tools are needed.

For example, we want to upgrade a Datalog database that has been running in
Datalevin 0.4.x to run in Datalevin 0.5.x.

1. Download the latest versions of both versions of `dtlv` tool. Rename the
   older version, e.g. 0.4.44 binary from `dtlv` to `dtlv-0.4`.

```
wget https://github.com/juji-io/datalevin/releases/download/0.4.44/dtlv-0.4.44-macos-latest-amd64.zip

unzip dtlv-0.4.44-macos-latest-amd64.zip

rm dtlv dtlv-0.4

wget https://github.com/juji-io/datalevin/releases/download/0.5.21/dtlv-0.5.21-macos-latest-amd64.zip

unzip dtlv-0.5.21-macos-latest-amd64.zip
```

2. Backup the current database first, e.g.

```
./dtlv-0.4 -d /src/dir -c copy /backup/dir
```
This also compacts the data file, so it's not huge. Ideally, one would run a cron job to backup daily for production databases.

3. Dump the current database as a text file, e.g.

```
./dtlv-0.4 -d /src/dir -g -f dump-file dump
```

This dumps the content of the Datalog database to a file called `dump-file`. The
format of the database dump is version independent.

4. Import the data into the new version of database, e.g.
```
./dtlv -d /dest/dir -f dump-file -g load
```

Now your new Datalog database is in `/dest/dir`. That's it.

If the database is a key-value store instead of a Datalog one, the dump and
load commands have different options, please consult the `dtlv help dump` and
`dtlv help load` for details.
