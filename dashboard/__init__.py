from dash import Dash

from . import callbacks
from .layout import get_layout


def create_dash_app(server):
    app = Dash(
        __name__,
        server=server,
        url_base_pathname="/dashboard/",
        suppress_callback_exceptions=True,
    )
    app.layout = get_layout()
    callbacks.register(app)
    return app
