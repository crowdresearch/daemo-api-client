import json


class Question(object):
    def __init__(self, value='Untitled Question', data_source=None):
        # type: (str, str) -> Question
        self.value = value
        self.data_source = data_source


class AuxAttributes(object):
    def __init__(self, src=None, data_source=None, question=None):
        # type: (str, str, Question) -> AuxAttributes
        self.src = src
        self.data_source = data_source
        self.question = question if question is not None else Question()
        # super(AuxAttributes, self).__init__()


class TemplateItem(object):
    def __init__(self, type, position, role, template=None, sub_type=None, required=True, aux_attributes=None):
        # type: (str, int, str, int, str, bool, AuxAttributes) -> TemplateItem
        self.type = type
        self.sub_type = sub_type
        self.position = position
        self.template = template
        self.role = role
        self.required = required
        self.aux_attributes = aux_attributes if aux_attributes is not None else AuxAttributes()


class RemoteContent(TemplateItem):
    def __init__(self, src, question_value=None, *args, **kwargs):
        question = Question(value=question_value) if question_value is not None else Question()
        aux_attributes = AuxAttributes(src=src, question=question)
        super(RemoteContent, self).__init__(type='iframe', role='display', aux_attributes=aux_attributes,
                                            *args, **kwargs)


class Template(object):
    def __init__(self, items):
        """

        :type items: list
        """
        self.items = items


class Project(object):
    def __init__(self, price, name='Untitled Project', repetition=1, template=None, post_mturk=False):
        """

        :type template: Template
        :type price: float
        :type name: str
        :type repetition: int
        :type post_mturk: bool
        """
        self.price = price
        self.repetition = repetition
        self.name = name
        self.template = template
        self.post_mturk = post_mturk


def to_dict(obj):
    _dict = obj.__dict__.copy()
    for k in _dict:
        if isinstance(_dict[k], list):
            _dict[k] = [to_dict(x) for x in _dict[k]]
        elif isinstance(_dict[k], Template) or isinstance(_dict[k], AuxAttributes) or isinstance(_dict[k], Question) \
                or isinstance(_dict[k], RemoteContent):
            _dict[k] = to_dict(_dict[k])
    return _dict


def to_json(obj):
    _dict = obj.__dict__.copy()
    for k in _dict:
        if isinstance(_dict[k], list):
            _dict[k] = [to_dict(x) for x in _dict[k]]
        elif isinstance(_dict[k], Template) or isinstance(_dict[k], AuxAttributes) or \
                isinstance(_dict[k], Question) or isinstance(_dict[k], RemoteContent):
            _dict[k] = to_dict(_dict[k])
    return json.dumps(_dict)


