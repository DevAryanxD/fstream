from aiohttp import web
from aiohttp.web import Application
from .stream_routes import routes
from api.app import app as flask_app

def web_server():
    web_app = web.Application(client_max_size=30000000)
    web_app.add_routes(routes)
    # Mount Flask app at /api
    web_app.add_subapp('/api/', Application().add_routes([
        web.route('*', '/{path:.*}', lambda request: web.Response(
            body=flask_app.wsgi_app(request.environ, lambda *args: None),
            content_type='application/json'  # Adjusted for API responses
        ))
    ]))
    return web_app