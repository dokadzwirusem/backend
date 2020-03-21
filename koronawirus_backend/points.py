from flask import abort, Blueprint, current_app, request
from .auth import requires_auth, moderator, check_rights
from .elastic import Elasticsearch, NotDefined

points = Blueprint('points', __name__, )


@points.route('/get_points', methods=['POST'])
def get_points():
    boundaries = request.json
    es = Elasticsearch(current_app.config['ES_CONNECTION_STRING'], index=current_app.config['INDEX_NAME'])
    return es.get_points(boundaries['top_right'], boundaries['bottom_left'])


@points.route('/get_point', methods=['POST'])
def get_point():
    params = request.json
    es = Elasticsearch(current_app.config['ES_CONNECTION_STRING'], index=current_app.config['INDEX_NAME'])
    return es.get_point(point_id=params['id'])


@points.route('/get_point', methods=['PUT'])
@requires_auth
def get_full_point(user):
    params = request.json
    es = Elasticsearch(current_app.config['ES_CONNECTION_STRING'], index=current_app.config['INDEX_NAME'])
    return es.get_full_point(point_id=params['id'])


@points.route('/delete_point', methods=['POST'])
def delete_point():
    params = request.json
    es = Elasticsearch(current_app.config['ES_CONNECTION_STRING'], index=current_app.config['INDEX_NAME'])
    return es.delete_point(point_id=params['id'])


@points.route('/get_nearest', methods=['POST'])
def get_nearest():
    params = request.json
    location = params['location']
    es = Elasticsearch(current_app.config['ES_CONNECTION_STRING'], index=current_app.config['INDEX_NAME'])
    return es.get_nearest(location=location)


@points.route('/add_point', methods=['POST'])
@requires_auth
@moderator
def add_point(user):
    req_json = request.json
    sub = user['sub']
    es = Elasticsearch(current_app.config['ES_CONNECTION_STRING'], index=current_app.config['INDEX_NAME'])
    return es.add_point(name=req_json['name'], operator=req_json['operator'], address=req_json['address'],
                        lat=req_json['lat'], lon=req_json['lon'], point_type=req_json['type'],
                        opening_hours=req_json['opening_hours'], phone=req_json.get('phone'),
                        prepare_instruction=req_json['prepare_instruction'], waiting_time=req_json['waiting_time'],
                        user_sub=sub)


@points.route('/get_my_points', methods=['POST'])
@requires_auth
def get_my_points(user):
    sub = user['sub']
    es = Elasticsearch(current_app.config['ES_CONNECTION_STRING'], index=current_app.config['INDEX_NAME'])
    return es.get_my_points(sub)


@points.route('/modify_point', methods=['POST'])
@requires_auth
@check_rights
def modify_point(user):
    req_json = request.json
    sub = user['sub']
    es = Elasticsearch(current_app.config['ES_CONNECTION_STRING'], index=current_app.config['INDEX_NAME'])
    return es.modify_point(point_id=req_json['id'], name=req_json.get('name', NotDefined()),
                           operator=req_json.get('operator', NotDefined()),
                           address=req_json.get('address', NotDefined()),
                           lat=str(req_json['lat']) if type(req_json.get('lat', NotDefined())) is not NotDefined \
                               else NotDefined(),
                           lon=str(req_json['lon']) if type(req_json.get('lon', NotDefined())) is not NotDefined \
                               else NotDefined(),
                           point_type=req_json.get('type', NotDefined()),
                           opening_hours=req_json.get('opening_hours', NotDefined()),
                           phone=req_json.get('phone', NotDefined()),
                           prepare_instruction=req_json.get('prepare_instruction', NotDefined()),
                           owned_by=req_json.get('owned_by', NotDefined()),
                           waiting_time=req_json.get('waiting_time', NotDefined()),
                           user_sub=sub)


@points.route('/set_availability', methods=['POST'])
def set_availability():
    params = request.json
    availability = params['availability']
    if availability not in ['short', 'moderate', 'long']:
        abort(500)
    es = Elasticsearch(current_app.config['ES_CONNECTION_STRING'], index=current_app.config['INDEX_NAME'])
    return es.modify_point(point_id=params['id'], waiting_time=params['availability'], user_sub='anonymous',
                           name=NotDefined(),
                           operator=NotDefined(),
                           address=NotDefined(),
                           lat=NotDefined(),
                           lon=NotDefined(),
                           point_type=NotDefined(),
                           opening_hours=NotDefined(),
                           phone=NotDefined(),
                           prepare_instruction=NotDefined(),
                           owned_by=NotDefined())


@points.route('/search_points', methods=['POST'])
def search_points():
    params = request.json
    phrase = params['phrase']
    point_type = params.get('point_type', None)
    top_right = params.get('top_right', None)
    bottom_left = params.get('bottom_left', None)

    es = Elasticsearch(current_app.config['ES_CONNECTION_STRING'], index=current_app.config['INDEX_NAME'])
    return es.search_points(phrase, point_type, top_right, bottom_left)
