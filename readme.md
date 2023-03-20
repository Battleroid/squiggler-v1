# Squiggler

To use the rollover API within Elasticsearch a call needs to be made to each alias with the given max age & max doc count. Before Squiggler we would measure this by hand, make the alias, make the rollover call, add the index, index pattern, etc.

Instead, Squiggler will handle all of these steps. It will by default look for non-suffix'd indexes (e.g. 'sample', 'fiddle', 'sample-thing') and automate the process of creating an alias and the approximate max doc count to reach the tipping point (default of 10 GiB).

It will also:

* Lock allocation while it reindexes data
* Starts reindex tasks for new unmigrated indexes, and waits for completion
* Deletes the old indexes once the reindex tasks have finished
* Creates aliases for the rollover indexes
* Unlocks allocation once finished
* Creates the kibana patterns on both the cluster and the 'mothership' (👽, cross ~~cultural~~ cluster search) for new rollover indexes

And it will perform the rollover max docs & max age approximation regardless if new indexes have been found or not. Effectively replacing the curator rollover capabilities, making the entire process pretty automatic.

## Configuration

See the [sample config](config.yaml).

Configuration consists of an array of clusters, global settings, and global notifiers (this could be tweaked to be per cluster notifier(s), but this is unnecessary and I've left it unimplemented because I simply don't see any need for it at the moment).

Global settings specified under `settings` **do not** override cluster specific settings, they should be treated as the default setting.

Mothership functionality is *not required*, but if you include it, it will do a health check before moving on, be aware.

Hipchat notifier (and Slack) will only dump new indexes affected. It will not dump any information related to rollover (as this will happen *every* time).

## Notes

With the exception of health checks and logging from the requests library all the log messages should be tagged with the cluster name.

Squiggler exits 0 regardless, because if it's anything else k8s will restart it indefinitely.
