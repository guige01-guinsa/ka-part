from flask import Blueprint

ts_bp = Blueprint(
    "tool_search",
    __name__,
    url_prefix="/ts"
)

from . import routes

