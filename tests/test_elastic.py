import pytest
from wiating_backend.elastic import Point, NotDefined


def test_createPoint():
    point = Point(name='some name', description='some desc', directions='some directions',
                  lat="15", lon="20", point_type="SHED", water_exists=True, water_comment="some water comment",
                  fire_exists=True, fire_comment="some fire comment", created_by='some id', last_modified_by='other id')
    assert point.last_modified_by == "other id"
    assert point.created_by == "some id"


def test_newPoint():
    point = Point.new_point(name='some name', description='some desc', directions='some directions',
                            lat="15", lon="20", point_type="SHED", water_exists=True,
                            water_comment="some water comment",
                            fire_exists=True, fire_comment="some fire comment", user_sub="some sub")
    assert point.last_modified_by == "some sub"
    assert point.created_by == "some sub"


@pytest.fixture
def point_from_dict():
    body = {"_id": "7g5qqnABsqio5qhd0cbc", "_index": "wiaty_images1", "_primary_term": 1, "_seq_no": 29626, "_source":
        {"created_timestamp": "1583403492", "created_by": "some id", "description": "EDIT: XII 2018: wiata spalona",
         "directions": "", "fire_comment": None, "fire_exists": None, "images":
             [{"created_timestamp": "1583403492", "name": "f660785da287e72143a5eddf77d37440.jpg", "created_by": "someone"}],
         "location": {"lat": "50.763923", "lon": "16.180389"},
         "name": "G\u00f3ry Wa\u0142brzyskie, masyw Che\u0142mca", "type": "SHED",
         "water_comment": None, "water_exists": None, "last_modified_timestamp": "1583403439",
         "last_modified_by": "other id"},
            "_type": "_doc", "_version": 12, "found": True}
    return Point.from_dict(body=body)


def test_pointFromDict(point_from_dict):
    assert point_from_dict.created_by == "some id"
    assert point_from_dict.description == "EDIT: XII 2018: wiata spalona"


def test_changePointName(point_from_dict):
    changes = point_from_dict.modify(name="changed name", description=NotDefined(), directions=NotDefined(),
                                     lat=NotDefined(), lon=NotDefined(), point_type=NotDefined(),
                                     water_exists=NotDefined(), fire_exists=NotDefined(), water_comment=NotDefined(),
                                     fire_comment=NotDefined(), user_sub=NotDefined())
    assert changes == {'name': {'new_value': 'changed name', 'old_value': 'Góry Wałbrzyskie, masyw Chełmca'}}


def test_changePointLat(point_from_dict):
    changes = point_from_dict.modify(name=NotDefined(), description=NotDefined(), directions=NotDefined(),
                                     lat="49", lon=NotDefined(), point_type=NotDefined(),
                                     water_exists=NotDefined(), fire_exists=NotDefined(), water_comment=NotDefined(),
                                     fire_comment=NotDefined(), user_sub=NotDefined())
    assert changes == {'lat': {'new_value': '49', 'old_value': '50.763923'}}


def test_pointToDictWithId(point_from_dict):
    result = point_from_dict.to_dict(with_id=True)
    assert result == {"created_timestamp": "1583403492", "description": "EDIT: XII 2018: wiata spalona",
         "directions": "", "fire_comment": None, "fire_exists": None, "images":
             [{"created_timestamp": "1583403492", "name": "f660785da287e72143a5eddf77d37440.jpg"}],
         "location": {"lat": "50.763923", "lon": "16.180389"},
         "name": "G\u00f3ry Wa\u0142brzyskie, masyw Che\u0142mca", "type": "SHED",
         "water_comment": None, "water_exists": None, "last_modified_timestamp": "1583403439",
         "id": "7g5qqnABsqio5qhd0cbc"}


def test_pointToDictWithoutId(point_from_dict):
    result = point_from_dict.to_dict()
    assert result == {"created_timestamp": "1583403492", "description": "EDIT: XII 2018: wiata spalona",
         "directions": "", "fire_comment": None, "fire_exists": None, "images":
             [{"created_timestamp": "1583403492", "name": "f660785da287e72143a5eddf77d37440.jpg"}],
         "location": {"lat": "50.763923", "lon": "16.180389"},
         "name": "G\u00f3ry Wa\u0142brzyskie, masyw Che\u0142mca", "type": "SHED",
         "water_comment": None, "water_exists": None, "last_modified_timestamp": "1583403439"}
