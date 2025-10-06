# Gatling Benchmark Scripts

This directory contains automation scripts for running Gatling benchmarks (located in the `gatling-simulations` module) on a periodic basis.

Each script "run" is intended to (1) identify the commits that are new since the previous run, (2) build a Solr distribution representing that commit, (3) start Solr, and (4) run a set of Gatling benchmarks using the Solr node. 

## "State" and Output

The scripts in this directory can be run manually, but are primarily intended to be invoked via a cronjob or in some other automated fashion.
To this end the scripts are "stateful", so that they can remember what commits are "new" since the last run.
By default, all state is stored in the `$HOME/.solr-benchmarks` directory.
(This can be overridden by exporting a `BENCH_STATE_ROOT` env var pointing to an alternate location.)
This directory is used to store repository checkouts, a "state" text file which remembers which commits have been benchmarked, and benchmark results themselves (located in the `results` subdirectory).

Scripts store all Gatling output for each "run", which primarily consists of a HTML "report" web page and any JS and other assets that this requires.
(We don't yet have a good way to aggregate these reports over time and show trendlines or other longitudinal data, but this is in progress.)

## Customization

The scripts support several environment variables that can be used to customize different aspects of the automation:

- `START_SOLR_OPTS` - a string containing any arguments to pass as a part of the `bin/solr start` command used to start Solr.  Particularly useful for specifying Solr heap size and adjacent settings. 
- `GATLING_SIMULATION_LIST` - (TODO unimplemented) by default scripts will run all available Gatling simulations, but if a more targeted subset of simulations is desired, users can specify that as a comma-delimited list here
- `ALERT_COMMAND` - (TODO unimplemented) an optional CLI command to run to alert users for any warnings or errors (e.g. a Solr commit that fails to build).  The command will be passed a single argument contain the text of the notification.  Alerting mechanisms can be relatively simple (e.g. using `echo` to write to a known location on disk, or `mail` to send email) or more complex (e.g. CLI integrations with PagerDuty or other notification providers like "Pushover")

## Manual Invocation Examples

Runs all benchmarks on the three specified commits.
(Assumes commits are on Solr's `main` branch, since `-b` not provided)
```
./scripts/gatling/run-benchmark-on-commits.sh -c df0fd5,9d1ecc,7f454a
```

Runs all benchmarks on all `branch_9x` commits between `88e363` and `b56ffc` (inclusive on both sides).
```
./scripts/gatling/run-benchmark-on-commits.sh -b branch_9x -e b56ffc -s 88e363
```

## Automated (i.e. cronjob) Invocation Examples

The commands above are useful for running benchmarks on a static set (or range) of commits, but more useful for long term performance monitoring is to trigger the scripts to run periodically on any "new" commits since the last run.

This requires either a CI system like Jenkins, or a cronjob to run the script at a regular interval.
A full CI system will provide a more robust featureset for triggering, tracking, and debugging benchmark runs.
However, for simplicity this directory also includes a cronjob-ready script (`periodic-benchmark.sh`) that can be used to run benchmarks periodically without the effort of setting up a complex system like Jenkins.

The cronjob below will run all benchmarks nightly on each `main` commit that is "new" since the previous night's run.
Script logs will be stored in the `/tmp` directory for review in case of error.
```
0 2 * * * cd /path/to/source/checkout/solr-sandbox ; export OUTFILE_NAME="/tmp/`date '+%Y-%m-%d'`-benchmark-run.log" ; ./scripts/gatling/periodic-benchmark.sh -b main -t java21_on_main &> $OUTFILE_NAME
```
