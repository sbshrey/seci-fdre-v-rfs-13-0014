"""Hosted worker for queued AWS-backed study runs."""

from __future__ import annotations

import argparse
import logging
import time
from typing import Sequence

from seci_fdre_v_model.web.storage import CloudStorageBackend

_logger = logging.getLogger("seci_fdre_v_model.web.worker")


def run_once(*, source_config_path: str | None = None) -> bool:
    backend = CloudStorageBackend.from_env(source_config_path=source_config_path)
    item = backend.claim_next_queued_run()
    if item is None:
        return False
    _logger.info("Executing queued run %s for owner %s", item.get("run_id"), item.get("owner_key"))
    backend.execute_claimed_run(item)
    return True


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run queued SECI FDRE-V studies from the AWS backend.")
    parser.add_argument("--source-config", default=None)
    parser.add_argument("--once", action="store_true", help="Process at most one queued run and exit.")
    parser.add_argument("--poll-seconds", type=float, default=10.0)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    while True:
        processed = run_once(source_config_path=args.source_config)
        if args.once:
            return 0 if processed else 1
        if not processed:
            time.sleep(max(args.poll_seconds, 1.0))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
