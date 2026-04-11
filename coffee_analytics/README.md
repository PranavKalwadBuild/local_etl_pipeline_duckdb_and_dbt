Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Containerized dbt usage

This project can also be run from a container built from `coffee_analytics/Dockerfile`.

Build the container from the repo root:

```bash
cd /Users/pranavkalwad/Desktop/Pranav/Career/Modern-data-platform/coffee_analytics
docker compose build
```

For DuckDB targets, set the runtime path inside the container:

```bash
export DBT_DUCKDB_PATH=/opt/app/coffeeshop.duckdb
```

Then run:

```bash
docker compose run --rm coffee_analytics_dbt run \
  --profiles-dir . \
  --project-dir . \
  --target dev
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
