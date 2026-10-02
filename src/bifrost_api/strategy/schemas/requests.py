"""POST / PUT bodies for the strategy routers (TD-24, batch 3c-1).

Strict types, unknown fields ignored and logged this release, ``extra="forbid"``
next (:class:`~bifrost_api.common.request_bodies.LenientBody`). Every field is
optional at the model: the presence rules (``name is required`` …) and the domain
rules (catalog codes, leg roles, gate ranges) stay where they were -- the route's
400 or core's -- with the same messages. The routes hand core
``body.declared(exclude_unset=True)``: exactly the declared fields the client sent.

The opportunity, allocation, instance, plan and review bodies are core's models
(``bifrost_core.monitor.schemas``) and are not repeated here; the PATCH bodies are
in ``bifrost_api.strategy.patch_bodies``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import StrictBool, StrictFloat, StrictInt, StrictStr

from bifrost_api.common.request_bodies import LenientBody, LenientItem

# Core stores these values as sent (jsonb), so a row written before the types were
# checked may hold a number; the UI sends such a value back unchanged on save.
MetaText = Union[StrictStr, StrictInt, StrictFloat]
Number = Union[StrictInt, StrictFloat]


class TemplateBody(LenientBody):
    """POST /strategies/templates and the replaced PUT /strategies/templates/{id}
    (the PUT changes the fields sent). POST needs ``template_code`` (core, 400)."""

    template_code: Optional[StrictStr] = None
    display_name: Optional[StrictStr] = None
    dim_direction: Optional[StrictStr] = None
    dim_structure: Optional[StrictStr] = None
    dim_coverage: Optional[StrictStr] = None
    dim_risk: Optional[StrictStr] = None
    dim_volatility: Optional[StrictStr] = None
    dim_time: Optional[StrictStr] = None
    explanation: Optional[StrictStr] = None
    typical_use: Optional[StrictStr] = None
    example: Optional[StrictStr] = None
    nature: Optional[StrictStr] = None
    sort_order: Optional[StrictInt] = None
    is_active: Optional[StrictBool] = None


class TemplateLegItem(LenientItem):
    role: Optional[StrictStr] = None
    direction: Optional[StrictStr] = None
    # The UI sends "" for a leg without a right (the stock leg).
    option_right: Optional[StrictStr] = None
    quantity_default: Optional[StrictInt] = None
    # Read when quantity_default is absent.
    quantity: Optional[StrictInt] = None
    # Ignored: core numbers the legs by their position in the array.
    sort_order: Optional[StrictInt] = None


class TemplateLegsBody(LenientBody):
    """PUT /strategies/templates/{id}/legs: replaces the legs. 400 without ``legs``."""

    legs: Optional[List[TemplateLegItem]] = None


class TemplateParamItem(LenientItem):
    meta_key: Optional[StrictStr] = None
    display_label: Optional[StrictStr] = None
    default_value_text: Optional[MetaText] = None
    param_kind: Optional[StrictStr] = None
    sort_order: Optional[StrictInt] = None


class TemplateParamsBody(LenientBody):
    """PUT /strategies/templates/{id}/params: replaces the parameters. 400 without ``items``."""

    items: Optional[List[TemplateParamItem]] = None


class TemplateCharacteristicsBody(LenientBody):
    """PUT /strategies/templates/{id}/characteristics: replaces the lines; null or absent clears."""

    items: Optional[List[StrictStr]] = None


class StructureLegItem(LenientItem):
    """A leg as the UI holds it. Core takes a structure's legs from its template, so
    these are checked for type and not stored."""

    role: Optional[StrictStr] = None
    direction: Optional[StrictStr] = None
    option_right: Optional[StrictStr] = None
    quantity: Optional[Number] = None
    strike: Optional[Number] = None
    expiration: Optional[StrictStr] = None


class StructureMetaItem(LenientItem):
    meta_key: Optional[StrictStr] = None
    meta_value_text: Optional[MetaText] = None


class StructureBody(LenientBody):
    """POST /strategies/structures and PUT /strategies/structures/{id} (a full replace).

    ``name`` and ``legs`` are required (core, 400); the template comes from
    ``strategy_template_id``, else from ``structure_type`` (its code)."""

    name: Optional[StrictStr] = None
    strategy_template_id: Optional[StrictInt] = None
    structure_type: Optional[StrictStr] = None
    structure_subtype: Optional[StrictStr] = None
    legs: Optional[List[StructureLegItem]] = None
    version: Optional[StrictInt] = None
    is_active: Optional[StrictBool] = None
    notes: Optional[StrictStr] = None
    meta: Optional[List[StructureMetaItem]] = None


class GateSafetyBody(LenientBody):
    """POST /strategies/gate-safety and PUT /strategies/gate-safety/{id} (a full replace).

    ``name`` is required (the route, 400). ``gates`` is the nested gates object, checked
    against core's ``GateParams`` (400 with the reason); its earnings dates go in the
    top-level ``earnings_dates`` (YYYY-MM-DD), never inside ``gates``."""

    name: Optional[StrictStr] = None
    version: Optional[StrictInt] = None
    dim_direction: Optional[StrictStr] = None
    dim_structure: Optional[StrictStr] = None
    dim_coverage: Optional[StrictStr] = None
    dim_risk: Optional[StrictStr] = None
    dim_volatility: Optional[StrictStr] = None
    dim_time: Optional[StrictStr] = None
    is_active: Optional[StrictBool] = None
    gates: Optional[Dict[str, Any]] = None
    earnings_dates: Optional[List[StrictStr]] = None


class SavedSearchBody(LenientBody):
    """POST /strategies/saved-searches. Core's rules: ``route`` an app path, ``label``
    non-blank, ``state`` an object (400 with the reason)."""

    route: Optional[StrictStr] = None
    label: Optional[StrictStr] = None
    state: Optional[Dict[str, Any]] = None
