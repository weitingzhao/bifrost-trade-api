"""GET /portfolio/short-legs — the short option legs and what each is priced against.

The shell's status bar needs one reading on every page: how close the short
legs are to assignment. Deriving that the way the Positions page does costs ten
queries, which is right for a page and wrong for a bar that is always on
screen.

This returns the legs and their spots and stops there. The cushion arithmetic
and the trader's own "tight" line are applied by the caller, where the single
implementation of both already lives -- a second copy here would be a second
copy of a rule, in a second language, with nothing able to catch the two
drifting apart.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(tags=["portfolio-short-legs"])


@router.get("/portfolio/short-legs")
def get_short_legs(
    request: Request,
    account_id: Optional[List[str]] = Query(
        None, description="Restrict to these IB account ids; omit for every account"
    ),
) -> Dict[str, Any]:
    """Short option legs across the book, each with the underlying's live price.

    `spot` is null where the underlying carries no live quote. That is the
    honest answer and the caller must treat it as unpriced, never as safe.
    """
    reader = request.app.state.reader
    legs = reader.get_short_option_legs(account_id)
    if legs is None:
        raise HTTPException(status_code=503, detail="Database unavailable")
    return {"legs": legs, "count": len(legs)}
