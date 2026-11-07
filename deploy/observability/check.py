# The deepbook dashboard's own check.
#
# deepmsg's `deploy/observability/e2e/check-config.py` validates deepmsg's
# tree, and deliberately does not count the dashboard provisioned from this
# repository (its provider lives in deepmsg's provisioning directory on the log
# host, not in deepmsg's tree). The two checks meet in the folder: Grafana
# loads deepmsg's three into `deepmsg` and this one into `deepbook`.
#
# What it can go red on:
#
# - a dashboard that does not parse, or has no uid/title/tags;
# - a panel that names a metric the deployment cannot produce (the whitelist is
#   the series D3 publishes plus the two deepmsg counters the variable and the
#   leader panel read);
# - a datasource other than the two provisioned ones;
# - a `$variable` used but not defined, or the `node` variable not reading
#   `node_id` from `deepmsg_driver_up` (the variable has to work on the member
#   hosts *and* the client host, which run different applications);
# - the logs panels missing, or reading the wrong components;
# - the provider YAML not naming the `deepbook` folder and its own directory.
#
# Run: python3 deploy/observability/check.py
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent
GRAFANA = ROOT / "grafana"
DASHBOARDS = GRAFANA / "dashboards"
PROVIDER = GRAFANA / "provisioning" / "dashboards" / "deepbook.yaml"

# Every series the page may name. A dashboard that asks for something else is
# a lie about what the deployment publishes, and it fails here rather than on
# a screen nobody is watching.
METRICS = {
    "deepbook_engine_up",
    "deepbook_engine_is_leader",
    "deepbook_engine_commit_position",
    "deepbook_engine_applied_total",
    "deepbook_engine_rejected_total",
    "deepbook_engine_filled_total",
    "deepbook_engine_book_levels",
    "deepbook_engine_book_qty",
    "deepbook_engine_snapshot_count",
    "deepbook_engine_md_seq",
    "deepbook_gateway_sessions",
    "deepbook_gateway_cluster_connected",
    "deepbook_gateway_orders_total",
    "deepbook_gateway_reports_total",
    "deepbook_gateway_rejects_total",
    # Read from the agents' own counters, not from the application.
    "deepmsg_cluster_is_leader",
    "deepmsg_driver_up",
}
DATASOURCES = {"victoriametrics", "victorialogs"}
METRIC_TOKEN = re.compile(r"\bdeep(?:book|msg)_[a-z0-9_]+\b")
VARIABLE = re.compile(r"\$([a-zA-Z_][a-zA-Z0-9_]*)")

failures: list[str] = []


def check(condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


def panels_of(dashboard: dict) -> list[dict]:
    return dashboard.get("panels", [])


def check_dashboard(path: pathlib.Path, seen_uids: set[str]) -> None:
    try:
        dashboard = json.loads(path.read_text())
    except json.JSONDecodeError as error:
        failures.append(f"{path.name}: not JSON ({error})")
        return

    uid = dashboard.get("uid", "")
    title = dashboard.get("title", "")
    tags = dashboard.get("tags", [])
    check(bool(uid), f"{path.name}: no uid")
    check(uid not in seen_uids, f"{path.name}: uid {uid!r} is used twice")
    seen_uids.add(uid)
    check(bool(title), f"{path.name}: no title")
    check("deepbook" in tags, f"{path.name}: tags do not carry 'deepbook': {tags}")
    check(
        isinstance(dashboard.get("schemaVersion"), int),
        f"{path.name}: no integer schemaVersion",
    )

    variables = {
        variable.get("name")
        for variable in dashboard.get("templating", {}).get("list", [])
    }
    node = next(
        (
            variable
            for variable in dashboard.get("templating", {}).get("list", [])
            if variable.get("name") == "node"
        ),
        None,
    )
    check(node is not None, f"{path.name}: no 'node' variable")
    if node is not None:
        query = node.get("query", {}).get("query", "")
        check(
            "label_values(deepmsg_driver_up, node_id)" in query,
            f"{path.name}: the node variable does not read node_id from deepmsg_driver_up",
        )
        check(node.get("multi") is False, f"{path.name}: the node variable is multi")
        check(
            "includeAll" in node and node["includeAll"] is False,
            f"{path.name}: the node variable includes all",
        )

    panels = panels_of(dashboard)
    check(len(panels) > 0, f"{path.name}: no panels")

    logs_components: set[str] = set()
    for panel in panels:
        name = panel.get("title", "<untitled>")
        datasource = panel.get("datasource")
        if datasource is not None:
            check(
                datasource.get("uid") in DATASOURCES,
                f"{path.name}/{name}: datasource {datasource.get('uid')!r} is not provisioned",
            )
        for target in panel.get("targets", []):
            target_datasource = target.get("datasource")
            if target_datasource is not None:
                check(
                    target_datasource.get("uid") in DATASOURCES,
                    f"{path.name}/{name}: target datasource {target_datasource.get('uid')!r}",
                )
            expr = target.get("expr", "")
            for metric in METRIC_TOKEN.findall(expr):
                check(
                    metric in METRICS,
                    f"{path.name}/{name}: {metric} is not a series the deployment publishes",
                )
            for variable in VARIABLE.findall(expr):
                check(
                    variable in variables,
                    f"{path.name}/{name}: ${variable} is not defined",
                )
            if datasource is not None and datasource.get("uid") == "victorialogs":
                for component in ("engine", "gateway"):
                    if f'component:"{component}"' in expr:
                        logs_components.add(component)

    check(
        {"engine", "gateway"} <= logs_components,
        f"{path.name}: the logs panels do not read both components: {sorted(logs_components)}",
    )

    # The two questions D6's rules ask, asked from the same page.
    every_expr = " ".join(
        target.get("expr", "")
        for panel in panels
        for target in panel.get("targets", [])
    )
    check(
        "count(deepbook_engine_up" in every_expr,
        f"{path.name}: no panel counts the engines that are up",
    )
    check(
        "sum(deepmsg_cluster_is_leader" in every_expr,
        f"{path.name}: no panel sums the cluster's leader counters",
    )


def check_provider() -> None:
    check(PROVIDER.is_file(), f"{PROVIDER.name}: the provider file is missing")
    if not PROVIDER.is_file():
        return
    text = PROVIDER.read_text()
    for needle in (
        "folder: deepbook",
        "path: /etc/deepmsg/grafana/dashboards-deepbook",
        "name: deepbook",
    ):
        check(needle in text, f"{PROVIDER.name}: does not say {needle!r}")


def main() -> int:
    check(DASHBOARDS.is_dir(), f"{DASHBOARDS}: the dashboard directory is missing")
    seen_uids: set[str] = set()
    paths = sorted(DASHBOARDS.glob("*.json"))
    check(bool(paths), f"{DASHBOARDS}: no dashboards")
    for path in paths:
        check_dashboard(path, seen_uids)
    check_provider()

    if failures:
        for failure in failures:
            print(f"check: {failure}", file=sys.stderr)
        return 1
    print(
        f"check: {len(paths)} dashboard(s), "
        f"{sum(len(panels_of(json.loads(path.read_text()))) for path in paths)} panels, ok"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
