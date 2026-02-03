import csv
import io
from typing import Iterable, Callable, List, Any

from flask import Response, stream_with_context


def stream_csv(
    rows: Iterable[Any],
    headers: List[str],
    row_fn: Callable[[Any], List[Any]],
    filename: str,
) -> Response:
    """
    Stream CSV rows without building the full file in memory.
    """
    def generate():
        out = io.StringIO()
        w = csv.writer(out)

        # header
        w.writerow(headers)
        yield out.getvalue()
        out.seek(0)
        out.truncate(0)

        # rows
        for r in rows:
            w.writerow(row_fn(r))
            yield out.getvalue()
            out.seek(0)
            out.truncate(0)

    return Response(
        stream_with_context(generate()),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
