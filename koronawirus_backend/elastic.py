from datetime import datetime
from elasticsearch import Elasticsearch as ES


class NotDefined:
    pass


class Point:
    def __init__(self, name, operator, address, opening_hours, lat, lon, point_type, phone, prepare_instruction,
                 owned_by, waiting_time, doc_id=None, last_modified_timestamp=None):
        self.waiting_time = waiting_time
        self.owned_by = owned_by
        self.prepare_instruction = prepare_instruction
        self.phone = phone
        self.opening_hours = opening_hours
        self.address = address
        self.operator = operator
        self.name = name
        self.lat = str(lat)
        self.lon = str(lon)
        self.point_type = point_type
        self.doc_id = doc_id
        self.last_modified_timestamp = datetime.utcnow().strftime("%s") if last_modified_timestamp is None \
            else last_modified_timestamp

    @classmethod
    def new_point(cls, name, operator, address, opening_hours, lat, lon, point_type, phone, prepare_instruction,
                  waiting_time, user_sub):
        return cls(name=name, operator=operator, address=address, opening_hours=opening_hours, lat=lat, lon=lon,
                   point_type=point_type, phone=phone, prepare_instruction=prepare_instruction,
                   waiting_time=waiting_time, owned_by=user_sub)

    @classmethod
    def from_dict(cls, body):
        source = body['_source']
        return cls(name=source['name'], operator=source['operator'], address=source['address'],
                   lat=source['location']['lat'], lon=source['location']['lon'], point_type=source['type'],
                   opening_hours=source['opening_hours'], phone=source['phone'],
                   prepare_instruction=source['prepare_instruction'], owned_by=source['owned_by'],
                   last_modified_timestamp=source['last_modified_timestamp'], waiting_time=source['waiting_time'],
                   doc_id=body['_id'])

    def to_dict(self, with_id=False):
        body = {
            "name": self.name,
            "operator": self.operator,
            "address": self.address,
            "location": {
                "lat": self.lat,
                "lon": self.lon
            },
            "type": self.point_type,
            "opening_hours": self.opening_hours,
            "phone": self.phone,
            "prepare_instruction": self.prepare_instruction,
            "last_modified_timestamp": self.last_modified_timestamp,
            "waiting_time": self.waiting_time
        }
        if with_id is True:
            body["id"] = self.doc_id
        return body

    def to_index(self, with_id=False):
        body = self.to_dict(with_id=with_id)
        body['owned_by'] = self.owned_by
        return body

    def modify(self, name, operator, address, lat, lon, point_type, opening_hours, phone,
               prepare_instruction, owned_by, waiting_time):
        params = locals()
        params.pop('self')
        changed = dict()
        for param in params.keys():
            if type(params[param]) is not NotDefined:
                if getattr(self, param) != params[param]:
                    changed[param] = {'old_value': getattr(self, param),
                                      'new_value': params[param]}
                setattr(self, param, params[param])
        self.last_modified_timestamp = datetime.utcnow().strftime("%s")
        return changed


def add_to_or_create_list(location, name, query):
    try:
        location[name]
    except KeyError:
        location[name] = []
    location[name].append(query)


class Elasticsearch:
    def __init__(self, connection_string, index='hospitals'):
        self.es = ES([connection_string])
        self.index = index

    def search_points(self, phrase, point_type=None, top_right=None, bottom_left=None, water=None, fire=None):
        body = {
            "query": {
                "bool": {
                    "must": [{
                        "multi_match": {
                            "query": phrase,
                            "fields": [
                                "name^3",
                                "operator",
                                "address"
                            ]
                        }
                    }]
                }
            }
        }
        if point_type is not None:
            add_to_or_create_list(location=body['query']['bool'], name='filter',
                                  query={"term": {"type": {"value": point_type}}})
        if top_right is not None and bottom_left is not None:
            add_to_or_create_list(location=body['query']['bool'], name='filter', query={
                "geo_bounding_box": {
                    "location": {
                        "top_left": {
                            "lat": str(top_right['lat']),
                            "lon": str(bottom_left['lon'])
                        },
                        "bottom_right": {
                            "lat": str(bottom_left['lat']),
                            "lon": str(top_right['lon'])
                        }
                    }
                }
            }
                                  )
        response = self.es.search(index=self.index, body=body)
        read_points = list(map(Point.from_dict, response['hits']['hits']))
        out_points = [point.to_dict(with_id=True) for point in read_points]
        return {'points': out_points}

    def get_nearest(self, location):
        body_transport = {'query': {'bool': {'must': [{'term': {'type': 'transport'}},
                                            {'geo_distance': {'distance': '1000km',
                                                              'location': {'lat': float(location['lat']),
                                                                           'lon': float(location['lon'])}}}]}},
                'size': 1,
                'sort': [{'_geo_distance': {'location': {'lat': float(location['lat']),
                                                                           'lon': float(location['lon'])},
                                            'order': 'asc',
                                            'unit': 'km'}}]}

        response_transport = self.es.search(index=self.index, body=body_transport)
        nearest_transport = Point.from_dict(response_transport['hits']['hits'][0])

        body_hospital = {'query': {'bool': {'must': [{'term': {'type': 'hospital'}},
                                                      {'geo_distance': {'distance': '1000km',
                                                                        'location': {'lat': float(location['lat']),
                                                                                     'lon': float(
                                                                                         location['lon'])}}}]}},
                          'size': 1,
                          'sort': [{'_geo_distance': {'location': {'lat': float(location['lat']),
                                                                   'lon': float(location['lon'])},
                                                      'order': 'asc',
                                                      'unit': 'km'}}]}
        response_hospital = self.es.search(index=self.index, body=body_hospital)
        nearest_hospital = Point.from_dict(response_hospital['hits']['hits'][0])
        return {'hospital': nearest_hospital.to_dict(with_id=True),
                'transport': nearest_transport.to_dict(with_id=True)}

    def get_points(self, top_right, bottom_left):
        body = '''{
        "query": {
            "bool" : {
                "must" : {
                    "match_all" : {}
                },
                "filter" : {
                    "geo_bounding_box" : {
                        "validation_method": "COERCE",
                        "location" : {
                            "top_left" : {
                                "lat" : ''' + str(top_right['lat']) + ''',
                                "lon" : ''' + str(bottom_left['lon']) + '''
                            },
                            "bottom_right" : {
                                "lat" : ''' + str(bottom_left['lat']) + ''',
                                "lon" : ''' + str(top_right['lon']) + '''
                            }
                        }
                    }
                }
            }
        },
      	"size": 9000
    	}'''
        response = self.es.search(index=self.index, body=body)
        read_points = list(map(Point.from_dict, response['hits']['hits']))
        out_points = [point.to_dict(with_id=True) for point in read_points]
        return {'points': out_points}

    def get_point(self, point_id):
        response = self.es.get(index=self.index, id=point_id)
        point = Point.from_dict(body=response)
        return point.to_dict(with_id=True)

    def delete_point(self, point_id):
        res = self.es.delete(index=self.index, id=point_id)
        if res['result'] == 'deleted':
            return
        raise Exception("Can't delete point")

    def get_my_points(self, sub):
        body = {
            "query": {
                "bool": {
                    "filter": {
                        "term": {
                            "owned_by": sub
                        }
                    }
                }
            }
        }
        response = self.es.search(index=self.index, body=body)
        read_points = list(map(Point.from_dict, response['hits']['hits']))
        out_points = [point.to_dict(with_id=True) for point in read_points]
        return {'points': out_points}

    def get_full_point(self, point_id):
        response = self.es.get(index=self.index, id=point_id)
        point = Point.from_dict(body=response)
        return point.to_index(with_id=True)

    def get_logs(self, point_id=None, size=25, offset=0):
        body = {"sort": [{"timestamp": {"order": "desc"}}], "from": offset, "size": size}
        if point_id is not None:
            body['query'] = {'term': {'doc_id.keyword': {'value': point_id}}}
        response = self.es.search(index=self.index + '_*', body=body)
        return {"logs": response['hits']['hits'], "total": response['hits']['total']['value']}

    def modify_point(self, point_id, user_sub, name, operator, address, lat, lon,
                     point_type, opening_hours, phone, prepare_instruction, waiting_time, owned_by):
        body = self.es.get(index=self.index, id=point_id)
        point = Point.from_dict(body=body)
        changes = point.modify(name=name, operator=operator, address=address, lat=lat, lon=lon,
                               point_type=point_type, opening_hours=opening_hours, phone=phone,
                               prepare_instruction=prepare_instruction, waiting_time=waiting_time, owned_by=owned_by)
        res = self.es.index(index=self.index, id=point_id, body=point.to_index())
        if res['result'] == 'updated':
            self.save_log(user_sub=user_sub, doc_id=point_id, name=point.name, changed=changes)
            return self.get_full_point(point_id=point_id)
        return res

    def add_point(self, name, operator, address, opening_hours, lat, lon, point_type, phone, prepare_instruction,
                  waiting_time, user_sub):
        point = Point.new_point(name=name, operator=operator, address=address, opening_hours=opening_hours, lat=lat,
                                lon=lon, point_type=point_type, phone=phone,
                                prepare_instruction=prepare_instruction, waiting_time=waiting_time, user_sub=user_sub)
        res = self.es.index(index=self.index, body=point.to_index())
        if res['result'] == 'created':
            return self.get_point(point_id=res['_id'])
        return res

    def save_log(self, user_sub, doc_id, name, changed):
        document = {"modified_by": user_sub, "doc_id": doc_id, "changes": changed,
                    "timestamp": datetime.utcnow().strftime("%Y/%m/%d %H:%M:%S"), "name": name}
        self.es.index(index=''.join((self.index, datetime.today().strftime('_%m_%Y'))), body=document)
