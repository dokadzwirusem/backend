from auth0.v3 import Auth0Error
from auth0.v3.authentication import Users
from flask import current_app, Flask, jsonify, redirect, render_template, request
from functools import wraps

from .elastic import Elasticsearch


# Error handler
class AuthError(Exception):
    def __init__(self, error, status_code):
        self.error = error
        self.status_code = status_code


def get_token_auth_header():
    """Obtains the Access Token from the Authorization Header
    """
    auth = request.headers.get("Authorization", None)
    if not auth:
        raise AuthError({"code": "authorization_header_missing",
                        "description":
                            "Authorization header is expected"}, 401)

    parts = auth.split()

    if parts[0].lower() != "bearer":
        raise AuthError({"code": "invalid_header",
                        "description":
                            "Authorization header must start with"
                            " Bearer"}, 401)
    elif len(parts) == 1:
        raise AuthError({"code": "invalid_header",
                        "description": "Token not found"}, 401)
    elif len(parts) > 2:
        raise AuthError({"code": "invalid_header",
                        "description":
                            "Authorization header must be"
                            " Bearer token"}, 401)

    token = parts[1]
    return token


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            app_metadata_key = 'https://koronapoints.netlify.com/app_metadata'
            a0_users = Users(current_app.config['AUTH0_DOMAIN'])
            a0_user = a0_users.userinfo(get_token_auth_header())
            user = {'sub': a0_user.get('sub')}
            if a0_user.get(app_metadata_key):
                user['role'] = a0_user.get(app_metadata_key).get('role')
        except Auth0Error:
            return redirect('login')
        return f(*args, **kwargs, user=user)

    return decorated


def check_rights(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if kwargs['user'].get('role') == 'moderator':
            return f(*args, **kwargs)
        es = Elasticsearch(current_app.config['ES_CONNECTION_STRING'], index=current_app.config['INDEX_NAME'])
        req_json = request.json
        point = es.get_full_point(point_id=req_json['id'])
        if point['owned_by'] == kwargs['user']['sub']:
            return f(*args, **kwargs)
        raise Exception("Not allowed")

    return decorated


def moderator(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if kwargs['user']['role'] == 'moderator':
            return f(*args, **kwargs)
        raise Exception("Not allowed")

    return decorated
